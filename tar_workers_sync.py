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
from typing import Dict, List, Optional, Set, Tuple

import boto3
from botocore.config import Config
import psycopg2


# -------------------------
# ID normalization (нужно только для имени файла в Timeweb)
# -------------------------

ID_VERSION_RE = re.compile(r"v\d+$", re.IGNORECASE)

def strip_arxiv_prefix_and_version(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if s.lower().startswith("arxiv:"):
        s = s[6:].strip()
    s = ID_VERSION_RE.sub("", s)
    return s


# -------------------------
# Logging
# -------------------------

def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# -------------------------
# DB SQL
# -------------------------

SQL_RESET_STALE = """
update arxiv_paper
set status='NEW',
    updated_at=now(),
    locked_by=null,
    locked_at=null,
    heartbeat_at=null,
    last_error = 'reset stale DOWNLOADING'
where status='DOWNLOADING'
  and coalesce(heartbeat_at, locked_at, updated_at) < (now() - (%s || ' minutes')::interval);
"""

# Забираем задачи: возвращаем (arxiv_id, lookup_key)
SQL_CLAIM_AND_MARK = """
with cte as (
    select arxiv_id, lookup_key
    from arxiv_paper
    where status='NEW'
      and coalesce(is_deleted,false) = false
      and coalesce(attempts,0) < %s
      and lookup_key is not null
      and lookup_key <> ''
    order by arxiv_id
    limit %s
    for update skip locked
)
update arxiv_paper p
set status='DOWNLOADING',
    attempts = coalesce(attempts,0) + 1,
    updated_at=now(),
    locked_by=%s,
    locked_at=now(),
    heartbeat_at=now()
from cte
where p.arxiv_id = cte.arxiv_id
returning p.arxiv_id, p.lookup_key;
"""

SQL_HEARTBEAT = """
update arxiv_paper
set heartbeat_at=now(),
    updated_at=now()
where arxiv_id = any(%s::text[])
  and status='DOWNLOADING'
  and locked_by=%s;
"""

# Ищем tar по lookup_key (в pdf_tar_index оно хранится как arxiv_id)
SQL_FIND_TAR_FOR_LOOKUP_IDS = """
select idx.tar_key, idx.arxiv_id
from pdf_tar_index idx
where idx.arxiv_id = any(%s::text[]);
"""

SQL_MARK_ONE_DONE = """
update arxiv_paper
set status='DONE',
    locked_by=null,
    locked_at=null,
    heartbeat_at=null,
    updated_at=now(),
    last_error=null
where arxiv_id=%s;
"""

SQL_MARK_NOT_FOUND = """
update arxiv_paper
set status='NOT_FOUND',
    locked_by=null,
    locked_at=null,
    heartbeat_at=null,
    updated_at=now(),
    last_error=%s
where arxiv_id = any(%s::text[]);
"""

SQL_MARK_ERROR = """
update arxiv_paper
set status='ERROR',
    locked_by=null,
    locked_at=null,
    heartbeat_at=null,
    updated_at=now(),
    last_error=%s
where arxiv_id = any(%s::text[]);
"""


# -------------------------
# S3 config
# -------------------------

@dataclass(frozen=True)
class S3SrcConfig:
    bucket: str
    region: str
    request_payer: Optional[str] = None
    profile: Optional[str] = None   # aws profile


@dataclass(frozen=True)
class S3DstConfig:
    bucket: str
    endpoint_url: str               # timeweb endpoint
    prefix: str = "pdf/"
    region: Optional[str] = None
    profile: Optional[str] = None
    addressing_style: str = "path"


def make_s3_client_src(cfg: S3SrcConfig):
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
# Upload helpers
# -------------------------

def _dst_key_for_arxiv_id(dst_prefix: str, arxiv_id: str) -> str:
    # в файле заменим "/" чтобы не создавать pseudo-folders
    safe = strip_arxiv_prefix_and_version(arxiv_id).replace("/", "_")
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
# DB helpers
# -------------------------

def heartbeat(conn, arxiv_ids: List[str], worker_id: str):
    if not arxiv_ids:
        return
    with conn.cursor() as cur:
        cur.execute(SQL_HEARTBEAT, (arxiv_ids, worker_id))
    conn.commit()


def mark_one_done(pg_dsn: str, arxiv_id: str):
    # отдельный короткий транзакционный апдейт: DONE сразу после upload
    c = psycopg2.connect(pg_dsn)
    try:
        with c.cursor() as cur:
            cur.execute(SQL_MARK_ONE_DONE, (arxiv_id,))
        c.commit()
    finally:
        c.close()


# -------------------------
# TAR processing
# -------------------------

def stream_tar_and_upload(
    pg_dsn: str,
    worker_id: str,
    src_cfg: S3SrcConfig,
    dst_cfg: S3DstConfig,
    tar_key: str,
    wanted_lookup_keys: Set[str],
    lookup_to_originals: Dict[str, List[str]],
    *,
    part_size: int,
    heartbeat_sec: int,
) -> Tuple[List[str], List[str], List[str], Optional[str]]:
    """
    returns:
      uploaded_original_ids,
      not_found_original_ids,
      all_original_ids_in_job,
      error_string
    """
    s3_src = make_s3_client_src(src_cfg)
    s3_dst = make_s3_client_dst(dst_cfg)

    uploaded: List[str] = []
    found_lookup: Set[str] = set()

    all_original_ids: List[str] = []
    for ids in lookup_to_originals.values():
        all_original_ids.extend(ids)

    # initial heartbeat best-effort
    try:
        hb = psycopg2.connect(pg_dsn)
        heartbeat(hb, all_original_ids, worker_id)
        hb.close()
    except Exception:
        pass

    last_hb = time.time()
    body = None
    tf = None

    try:
        get_kwargs = {"Bucket": src_cfg.bucket, "Key": tar_key}
        if src_cfg.request_payer:
            get_kwargs["RequestPayer"] = src_cfg.request_payer

        resp = s3_src.get_object(**get_kwargs)
        body = resp["Body"]

        tf = tarfile.open(fileobj=body, mode="r|*")

        for member in tf:
            if heartbeat_sec > 0 and (time.time() - last_hb) >= heartbeat_sec:
                try:
                    hb = psycopg2.connect(pg_dsn)
                    heartbeat(hb, all_original_ids, worker_id)
                    hb.close()
                except Exception:
                    pass
                last_hb = time.time()

            if not member.isreg() or not member.name.lower().endswith(".pdf"):
                continue

            base = os.path.basename(member.name)
            lookup = base[:-4]  # remove .pdf

            if lookup not in wanted_lookup_keys:
                continue

            f = tf.extractfile(member)
            if f is None:
                continue

            originals = lookup_to_originals.get(lookup, [])
            if not originals:
                try:
                    f.close()
                except Exception:
                    pass
                continue

            # upload
            if len(originals) == 1:
                original_id = originals[0]
                dst_key = _dst_key_for_arxiv_id(dst_cfg.prefix, original_id)
                put_stream_multipart(s3_dst, dst_cfg.bucket, dst_key, f, part_size=part_size)

                # ✅ СРАЗУ ставим DONE
                mark_one_done(pg_dsn, original_id)
                uploaded.append(original_id)

            else:
                data = f.read()
                for original_id in originals:
                    dst_key = _dst_key_for_arxiv_id(dst_cfg.prefix, original_id)
                    put_stream_multipart(s3_dst, dst_cfg.bucket, dst_key, io.BytesIO(data), part_size=part_size)

                    # ✅ СРАЗУ DONE
                    mark_one_done(pg_dsn, original_id)
                    uploaded.append(original_id)

            try:
                f.close()
            except Exception:
                pass

            found_lookup.add(lookup)

        # что не нашли внутри tar
        not_found_lookup = wanted_lookup_keys - found_lookup
        not_found_original: List[str] = []
        for lk in not_found_lookup:
            not_found_original.extend(lookup_to_originals.get(lk, []))

        return uploaded, not_found_original, all_original_ids, None

    except Exception as e:
        # ✅ НЕ теряем uploaded (частичный прогресс)
        return uploaded, [], all_original_ids, f"{type(e).__name__}: {e}"

    finally:
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

def build_plan_from_db(
    conn,
    limit_rows: int,
    max_attempts: int,
    worker_id: str
) -> Tuple[Dict[str, Dict[str, List[str]]], List[str]]:
    """
    returns:
      tar_key -> { lookup_key -> [original_arxiv_id, ...] }
      no_tar_original_ids
    """
    tar_to_lookup_to_originals: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    no_tar: List[str] = []

    with conn.cursor() as cur:
        cur.execute(SQL_CLAIM_AND_MARK, (max_attempts, limit_rows, worker_id))
        claimed_rows = cur.fetchall()  # [(arxiv_id, lookup_key)]
        conn.commit()

    if not claimed_rows:
        return {}, []

    # lookup keys we need to resolve in pdf_tar_index
    lookup_list = [lk for _, lk in claimed_rows if lk]
    lookup_list = [x.strip() for x in lookup_list if x.strip()]

    with conn.cursor() as cur:
        cur.execute(SQL_FIND_TAR_FOR_LOOKUP_IDS, (lookup_list,))
        mappings = cur.fetchall()  # (tar_key, lookup_key)

    lookup_to_tar: Dict[str, str] = {}
    for tar_key, lookup_key in mappings:
        if tar_key and lookup_key and lookup_key not in lookup_to_tar:
            lookup_to_tar[lookup_key] = tar_key

    for original_id, lk in claimed_rows:
        lk = (lk or "").strip()
        tar = lookup_to_tar.get(lk)
        if tar:
            tar_to_lookup_to_originals[tar][lk].append(original_id)
        else:
            no_tar.append(original_id)

    return tar_to_lookup_to_originals, no_tar


def run_once(
    pg_dsn: str,
    src_cfg: S3SrcConfig,
    dst_cfg: S3DstConfig,
    *,
    worker_id: str,
    concurrency: int,
    limit_rows: int,
    max_attempts: int,
    stale_minutes: int,
    sleep_on_empty_sec: float,
    part_size: int,
    heartbeat_sec: int,
):
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = False
    try:
        if stale_minutes > 0:
            with conn.cursor() as cur:
                cur.execute(SQL_RESET_STALE, (stale_minutes,))
            conn.commit()

        tar_plan, no_tar = build_plan_from_db(conn, limit_rows=limit_rows, max_attempts=max_attempts, worker_id=worker_id)
        claimed_total = sum(len(ids) for t in tar_plan.values() for ids in t.values()) + len(no_tar)

        logging.info(
            "Plan built. claimed=%d tars=%d not_found_no_tar=%d",
            claimed_total, len(tar_plan), len(no_tar)
        )

        if no_tar:
            with conn.cursor() as cur:
                cur.execute(SQL_MARK_NOT_FOUND, ("no tar_key in pdf_tar_index", no_tar))
            conn.commit()

        if not tar_plan:
            logging.info("No NEW rows claimed. Sleep %.1fs", sleep_on_empty_sec)
            time.sleep(sleep_on_empty_sec)
            return

        futures = []
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            for tar_key, lookup_to_originals in tar_plan.items():
                wanted_lookup = set(lookup_to_originals.keys())

                futures.append(ex.submit(
                    stream_tar_and_upload,
                    pg_dsn,
                    worker_id,
                    src_cfg,
                    dst_cfg,
                    tar_key,
                    wanted_lookup,
                    lookup_to_originals,
                    part_size=part_size,
                    heartbeat_sec=heartbeat_sec,
                ))

            total_uploaded = 0
            total_not_found_in_tar = 0
            total_error_ids = 0

            for fut in as_completed(futures):
                uploaded_ids, not_found_ids, job_ids, error = fut.result()

                # DONE уже ставим внутри job, здесь просто считаем
                if uploaded_ids:
                    total_uploaded += len(uploaded_ids)

                if not_found_ids:
                    total_not_found_in_tar += len(not_found_ids)
                    with conn.cursor() as cur:
                        cur.execute(SQL_MARK_NOT_FOUND, ("not found inside tar", not_found_ids))
                    conn.commit()

                if error:
                    # ✅ ERROR только тем, кто НЕ попал в uploaded/not_found
                    uploaded_set = set(uploaded_ids)
                    nf_set = set(not_found_ids)
                    remaining_error = [x for x in job_ids if x not in uploaded_set and x not in nf_set]

                    if remaining_error:
                        total_error_ids += len(remaining_error)
                        with conn.cursor() as cur:
                            cur.execute(SQL_MARK_ERROR, (error, remaining_error))
                        conn.commit()

                    logging.error(
                        "TAR job error: %s (job=%d uploaded=%d not_found=%d error=%d)",
                        error, len(job_ids), len(uploaded_ids), len(not_found_ids), len(remaining_error)
                    )

        logging.info(
            "RUN DONE. tars=%d uploaded=%d not_found_in_tar=%d error_ids=%d",
            len(tar_plan), total_uploaded, total_not_found_in_tar, total_error_ids
        )

    finally:
        conn.close()


def run_loop(*args, interval_sec: int, **kwargs):
    while True:
        run_once(*args, **kwargs)
        time.sleep(interval_sec)


def main():
    ap = argparse.ArgumentParser(description="Stream arXiv tar -> upload PDFs to Timeweb S3, update arxiv_paper statuses")

    ap.add_argument("--pg", required=True, help="Postgres DSN")

    # source (arxiv)
    ap.add_argument("--src-bucket", default="arxiv")
    ap.add_argument("--src-region", default="us-east-1")
    ap.add_argument("--src-request-payer", default="requester")
    ap.add_argument("--src-profile", default="default", help="AWS profile for SOURCE (arXiv)")

    # destination (timeweb)
    ap.add_argument("--dst-bucket", required=True)
    ap.add_argument("--dst-endpoint", required=True)
    ap.add_argument("--dst-prefix", default="pdf/")
    ap.add_argument("--dst-profile", default="timeweb", help="AWS profile for DEST (timeweb)")
    ap.add_argument("--dst-region", default="")
    ap.add_argument("--dst-addressing-style", default="path", choices=["path", "virtual"])

    # runtime
    ap.add_argument("--concurrency", type=int, default=4, help="parallel tar jobs INSIDE one worker process")
    ap.add_argument("--limit-rows", type=int, default=5000, help="how many arxiv_paper rows to claim per iteration")
    ap.add_argument("--worker-id", default="", help="logical worker id")
    ap.add_argument("--heartbeat-sec", type=int, default=60, help="heartbeat while processing tar (0=disable)")
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--stale-minutes", type=int, default=360)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval-sec", type=int, default=60)
    ap.add_argument("--sleep-on-empty-sec", type=float, default=5.0)
    ap.add_argument("--log-level", default="INFO")
    ap.add_argument("--part-size-mb", type=int, default=8)

    args = ap.parse_args()

    host = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "worker"
    wid = args.worker_id or host
    worker_id = f"{wid}:{os.getpid()}"

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
            worker_id=worker_id,
            concurrency=args.concurrency,
            limit_rows=args.limit_rows,
            max_attempts=args.max_attempts,
            stale_minutes=args.stale_minutes,
            sleep_on_empty_sec=args.sleep_on_empty_sec,
            part_size=part_size,
            heartbeat_sec=args.heartbeat_sec,
        )
    else:
        run_loop(
            args.pg,
            src_cfg,
            dst_cfg,
            worker_id=worker_id,
            concurrency=args.concurrency,
            limit_rows=args.limit_rows,
            max_attempts=args.max_attempts,
            stale_minutes=args.stale_minutes,
            sleep_on_empty_sec=args.sleep_on_empty_sec,
            part_size=part_size,
            heartbeat_sec=args.heartbeat_sec,
            interval_sec=args.interval_sec,
        )


if __name__ == "__main__":
    main()
