import argparse
import io
import logging
import os
import re
import tarfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import psycopg2


# -------------------------
# ID normalization
# -------------------------

ID_VERSION_RE = re.compile(r"v\d+$", re.IGNORECASE)
OLD_WITH_SLASH_RE = re.compile(r"^([a-z\-]+)\/([0-9]{7})$", re.IGNORECASE)
OLD_NO_SLASH_RE = re.compile(r"^([a-z\-]+)([0-9]{7})$", re.IGNORECASE)
NEW_RE = re.compile(r"^[0-9]{4}\.[0-9]{4,6}$")


def strip_arxiv_prefix_and_version(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if s.lower().startswith("arxiv:"):
        s = s[6:].strip()
    s = ID_VERSION_RE.sub("", s)
    return s


def arxiv_id_to_lookup_key(raw_arxiv_id: str) -> str:
    """
    Приводим arxiv_paper.arxiv_id к виду, который лежит в pdf_tar_index.arxiv_id.

    new:  2406.08318 -> 2406.08318
    old:  cond-mat/0702234 -> cond-mat0702234
          astro-ph/0001477 -> astro-ph0001477
          astro-ph0001477  -> astro-ph0001477
    """
    s = strip_arxiv_prefix_and_version(raw_arxiv_id)
    if not s:
        return ""

    if NEW_RE.match(s):
        return s

    m = OLD_WITH_SLASH_RE.match(s)
    if m:
        return f"{m.group(1)}{m.group(2)}"

    m = OLD_NO_SLASH_RE.match(s)
    if m:
        return s

    return s.replace("/", "")


# -------------------------
# Logging
# -------------------------

def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# -------------------------
# DB
# -------------------------

SQL_RESET_STALE = """
update arxiv_paper
set status='NEW',
    updated_at=now(),
    last_error = coalesce(last_error,'') || ' | reset stale DOWNLOADING'
where status='DOWNLOADING'
  and updated_at < (now() - (%s || ' minutes')::interval);
"""

SQL_CLAIM_NEW = """
select arxiv_id
from arxiv_paper
where status='NEW'
  and coalesce(is_deleted,false) = false
  and coalesce(attempts,0) < %s
order by arxiv_id
limit %s
for update skip locked
"""

SQL_MARK_DOWNLOADING = """
update arxiv_paper
set status='DOWNLOADING',
    attempts = coalesce(attempts,0) + 1,
    updated_at=now()
where arxiv_id = any(%s)
"""

SQL_FIND_TAR_FOR_LOOKUP_IDS = """
select idx.tar_key, idx.arxiv_id
from pdf_tar_index idx
where idx.arxiv_id = any(%s)
"""

SQL_MARK_DONE = """
update arxiv_paper
set status='DONE',
    updated_at=now(),
    last_error=null
where arxiv_id = any(%s)
"""

SQL_MARK_NOT_FOUND = """
update arxiv_paper
set status='NOT_FOUND',
    updated_at=now(),
    last_error=%s
where arxiv_id = any(%s)
"""

SQL_MARK_ERROR = """
update arxiv_paper
set status='ERROR',
    updated_at=now(),
    last_error=%s
where arxiv_id = any(%s)
"""


# -------------------------
# S3 config
# -------------------------

@dataclass(frozen=True)
class S3SrcConfig:
    bucket: str
    region: str
    request_payer: Optional[str] = None
    profile: Optional[str] = None   # default


@dataclass(frozen=True)
class S3DstConfig:
    bucket: str
    endpoint_url: str               # timeweb endpoint (например https://s3.twcstorage.ru)
    prefix: str = "pdf/"
    region: Optional[str] = None    # чаще пусто/None для S3-compatible
    profile: Optional[str] = None   # timeweb
    addressing_style: str = "path"  # важно для многих S3-compatible


def make_s3_client_src(cfg: S3SrcConfig):
    # для arXiv у тебя работает подписанный доступ + RequestPayer=requester
    sess = boto3.Session(profile_name=cfg.profile) if cfg.profile else boto3.Session()
    return sess.client(
        "s3",
        region_name=cfg.region,
        config=Config(
            retries={"max_attempts": 10, "mode": "standard"},
            max_pool_connections=50,
        ),
    )


def make_s3_client_dst(cfg: S3DstConfig):
    # Timeweb S3 — S3-compatible: обычно нужен path-style
    sess = boto3.Session(profile_name=cfg.profile) if cfg.profile else boto3.Session()
    return sess.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        region_name=cfg.region,
        config=Config(
            s3={"addressing_style": cfg.addressing_style},
            retries={"max_attempts": 10, "mode": "standard"},
            max_pool_connections=50,
        ),
    )


# -------------------------
# Upload helpers (no seekable needed)
# -------------------------

def _dst_key_for_arxiv_id(dst_prefix: str, arxiv_id: str) -> str:
    safe = arxiv_id.replace("/", "_")
    return f"{dst_prefix}{safe}.pdf"


def put_stream_multipart(
    s3,
    bucket: str,
    key: str,
    stream,
    *,
    part_size: int,
    content_type: str = "application/pdf",
) -> None:
    """
    Multipart upload из не-seekable stream.
    """
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType=content_type)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1

    try:
        while True:
            chunk = stream.read(part_size)
            if not chunk:
                break

            resp = s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk,
            )
            parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
            part_number += 1

        if not parts:
            # теоретически не должно быть
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            s3.put_object(Bucket=bucket, Key=key, Body=b"", ContentType=content_type)
            return

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    except Exception:
        try:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise


# -------------------------
# TAR processing
# -------------------------

def stream_tar_and_upload(
    src_cfg: S3SrcConfig,
    dst_cfg: S3DstConfig,
    tar_key: str,
    wanted_lookup_keys: Set[str],
    wanted_original_ids_by_lookup: Dict[str, List[str]],
    *,
    part_size: int,
) -> Tuple[List[str], List[str], List[str], Optional[str]]:
    """
    ВАЖНО: создаём клиенты ВНУТРИ job (надёжнее в потоках).
    returns: uploaded_original_ids, not_found_original_ids, all_original_ids_in_job, error_string
    """
    s3_src = make_s3_client_src(src_cfg)
    s3_dst = make_s3_client_dst(dst_cfg)

    uploaded: List[str] = []
    found_lookup: Set[str] = set()

    all_original_ids: List[str] = []
    for lk, ids in wanted_original_ids_by_lookup.items():
        all_original_ids.extend(ids)

    body = None
    tf = None

    try:
        get_kwargs = {"Bucket": src_cfg.bucket, "Key": tar_key}
        if src_cfg.request_payer:
            get_kwargs["RequestPayer"] = src_cfg.request_payer

        resp = s3_src.get_object(**get_kwargs)
        body = resp["Body"]  # StreamingBody

        tf = tarfile.open(fileobj=body, mode="r|*")  # streaming mode

        for member in tf:
            if not member.isreg():
                continue
            if not member.name.lower().endswith(".pdf"):
                continue

            base = os.path.basename(member.name)
            lookup = base[:-4]  # remove .pdf

            if lookup not in wanted_lookup_keys:
                continue

            f = tf.extractfile(member)
            if f is None:
                continue

            originals = wanted_original_ids_by_lookup.get(lookup, [])
            if not originals:
                try:
                    f.close()
                except Exception:
                    pass
                continue

            if len(originals) == 1:
                # читаем поток ровно один раз
                original_id = originals[0]
                dst_key = _dst_key_for_arxiv_id(dst_cfg.prefix, original_id)
                put_stream_multipart(s3_dst, dst_cfg.bucket, dst_key, f, part_size=part_size)
                uploaded.append(original_id)
            else:
                # редкий кейс: буферизуем один раз и грузим всем
                data = f.read()
                for original_id in originals:
                    dst_key = _dst_key_for_arxiv_id(dst_cfg.prefix, original_id)
                    put_stream_multipart(s3_dst, dst_cfg.bucket, dst_key, io.BytesIO(data), part_size=part_size)
                    uploaded.append(original_id)

            try:
                f.close()
            except Exception:
                pass

            found_lookup.add(lookup)

        not_found_lookup = wanted_lookup_keys - found_lookup
        not_found_original: List[str] = []
        for lk in not_found_lookup:
            not_found_original.extend(wanted_original_ids_by_lookup.get(lk, []))

        return uploaded, not_found_original, all_original_ids, None

    except Exception as e:
        return [], [], all_original_ids, f"{type(e).__name__}: {e}"

    finally:
        # важно для Windows: закрыть корректно
        try:
            if tf is not None:
                tf.close()
        except Exception:
            pass
        try:
            if body is not None:
                body.close()
        except Exception:
            pass


# -------------------------
# Plan + run
# -------------------------

def build_plan_from_db(conn, limit_rows: int, max_attempts: int) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    1) claim NEW rows (skip locked)
    2) mark them DOWNLOADING (+attempt)
    3) map lookup_key -> tar_key via pdf_tar_index
    """
    tar_to_original_ids: Dict[str, List[str]] = defaultdict(list)
    no_tar: List[str] = []

    with conn.cursor() as cur:
        cur.execute(SQL_CLAIM_NEW, (max_attempts, limit_rows))
        claimed = [(r[0] or "").strip() for r in cur.fetchall()]
        claimed = [x for x in claimed if x]
        if not claimed:
            return {}, []

        cur.execute(SQL_MARK_DOWNLOADING, (claimed,))
        conn.commit()

        lookup_list = [arxiv_id_to_lookup_key(x) for x in claimed]
        lookup_list = [x for x in lookup_list if x]

        cur.execute(SQL_FIND_TAR_FOR_LOOKUP_IDS, (lookup_list,))
        mappings = cur.fetchall()  # (tar_key, lookup_key)

        lookup_to_tar: Dict[str, str] = {}
        for tar_key, lookup_key in mappings:
            if tar_key and lookup_key and lookup_key not in lookup_to_tar:
                lookup_to_tar[lookup_key] = tar_key

        for original in claimed:
            lk = arxiv_id_to_lookup_key(original)
            tar = lookup_to_tar.get(lk)
            if tar:
                tar_to_original_ids[tar].append(original)
            else:
                no_tar.append(original)

    return tar_to_original_ids, no_tar


def run_once(
    pg_dsn: str,
    src_cfg: S3SrcConfig,
    dst_cfg: S3DstConfig,
    *,
    concurrency: int,
    limit_rows: int,
    max_attempts: int,
    stale_minutes: int,
    sleep_on_empty_sec: float,
    part_size: int,
):
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = False
    try:
        # reset stale DOWNLOADING -> NEW
        if stale_minutes > 0:
            with conn.cursor() as cur:
                cur.execute(SQL_RESET_STALE, (stale_minutes,))
            conn.commit()

        tar_to_ids, no_tar = build_plan_from_db(conn, limit_rows=limit_rows, max_attempts=max_attempts)
        claimed_total = sum(len(v) for v in tar_to_ids.values()) + len(no_tar)

        logging.info("Plan built. claimed=%d tars=%d not_found_no_tar=%d", claimed_total, len(tar_to_ids), len(no_tar))

        if no_tar:
            with conn.cursor() as cur:
                cur.execute(SQL_MARK_NOT_FOUND, ("no tar_key in pdf_tar_index", no_tar))
            conn.commit()

        if not tar_to_ids:
            logging.info("No NEW rows claimed. Sleep %.1fs", sleep_on_empty_sec)
            time.sleep(sleep_on_empty_sec)
            return

        futures = []
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            for tar_key, original_ids in tar_to_ids.items():
                wanted_lookup = set(arxiv_id_to_lookup_key(x) for x in original_ids)
                lookup_to_originals: Dict[str, List[str]] = defaultdict(list)
                for oid in original_ids:
                    lookup_to_originals[arxiv_id_to_lookup_key(oid)].append(oid)

                futures.append(ex.submit(
                    stream_tar_and_upload,
                    src_cfg, dst_cfg,
                    tar_key,
                    wanted_lookup,
                    lookup_to_originals,
                    part_size=part_size,
                ))

            total_uploaded = 0
            total_not_found_in_tar = 0
            total_error_ids = 0

            for fut in as_completed(futures):
                uploaded_ids, not_found_ids, job_ids, error = fut.result()

                if uploaded_ids:
                    total_uploaded += len(uploaded_ids)
                    with conn.cursor() as cur:
                        cur.execute(SQL_MARK_DONE, (uploaded_ids,))
                    conn.commit()

                if not_found_ids:
                    total_not_found_in_tar += len(not_found_ids)
                    with conn.cursor() as cur:
                        cur.execute(SQL_MARK_NOT_FOUND, ("not found inside tar", not_found_ids))
                    conn.commit()

                if error:
                    total_error_ids += len(job_ids)
                    with conn.cursor() as cur:
                        cur.execute(SQL_MARK_ERROR, (error, job_ids))
                    conn.commit()
                    logging.error("TAR job error: %s (ids=%d)", error, len(job_ids))

        logging.info(
            "RUN DONE. tars=%d uploaded=%d not_found_in_tar=%d error_ids=%d",
            len(tar_to_ids), total_uploaded, total_not_found_in_tar, total_error_ids
        )

    finally:
        conn.close()


def run_loop(*args, interval_sec: int, **kwargs):
    while True:
        run_once(*args, **kwargs)
        time.sleep(interval_sec)


def main():
    ap = argparse.ArgumentParser(description="Stream arXiv tar -> upload PDFs to Timeweb S3, update arxiv_paper statuses")

    ap.add_argument("--pg", required=True, help="Postgres DSN (postgresql://user:pass@host:port/db)")

    # source (arxiv)
    ap.add_argument("--src-bucket", default="arxiv")
    ap.add_argument("--src-region", default="us-east-1")
    ap.add_argument("--src-request-payer", default="requester")
    ap.add_argument("--src-profile", default="default", help="AWS profile for SOURCE (arXiv)")

    # destination (timeweb)
    ap.add_argument("--dst-bucket", required=True)
    ap.add_argument("--dst-endpoint", required=True, help="e.g. https://s3.twcstorage.ru or https://s3.timeweb.cloud")
    ap.add_argument("--dst-prefix", default="pdf/")
    ap.add_argument("--dst-profile", default="timeweb", help="AWS profile for DEST (timeweb)")
    ap.add_argument("--dst-region", default="", help="Optional region for S3-compatible (often empty)")
    ap.add_argument("--dst-addressing-style", default="path", choices=["path", "virtual"])

    # runtime
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--limit-rows", type=int, default=20000)
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--stale-minutes", type=int, default=360)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval-sec", type=int, default=1800)
    ap.add_argument("--sleep-on-empty-sec", type=float, default=5.0)
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--part-size-mb", type=int, default=8, help="Multipart part size (MB), min 5. Use 8..64.")

    args = ap.parse_args()
    setup_logging(args.log_level)

    part_size = max(8, args.part_size_mb) * 1024 * 1024

    src_cfg = S3SrcConfig(
        bucket=args.src_bucket,
        region=args.src_region,
        request_payer=args.src_request_payer if args.src_request_payer else None,
        profile=args.src_profile or None,
    )
    dst_cfg = S3DstConfig(
        bucket=args.dst_bucket,
        endpoint_url=args.dst_endpoint,
        prefix=args.dst_prefix,
        region=args.dst_region or None,
        profile=args.dst_profile or None,
        addressing_style=args.dst_addressing_style,
    )

    if args.once:
        run_once(
            args.pg,
            src_cfg,
            dst_cfg,
            concurrency=args.concurrency,
            limit_rows=args.limit_rows,
            max_attempts=args.max_attempts,
            stale_minutes=args.stale_minutes,
            sleep_on_empty_sec=args.sleep_on_empty_sec,
            part_size=part_size,
        )
    else:
        run_loop(
            args.pg,
            src_cfg,
            dst_cfg,
            concurrency=args.concurrency,
            limit_rows=args.limit_rows,
            max_attempts=args.max_attempts,
            stale_minutes=args.stale_minutes,
            sleep_on_empty_sec=args.sleep_on_empty_sec,
            part_size=part_size,
            interval_sec=args.interval_sec,
        )


if __name__ == "__main__":
    main()
