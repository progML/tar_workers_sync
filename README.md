# tar_workers_sync

Воркеры для выгрузки нужных PDF из arXiv S3 tar-архивов и загрузки их в целевое S3-хранилище.

Проект берёт список статей из PostgreSQL, по `lookup_key` находит нужный `tar_key`, потоково читает tar из arXiv bucket, извлекает только нужные PDF и загружает их в целевой S3 bucket, например Timeweb S3.

---

## Что делает проект

`tar_workers_sync.py`:

- claim'ит пачку записей из `arxiv_paper`;
- по `lookup_key` ищет, в каком tar лежит нужный PDF;
- группирует статьи по `tar_key`;
- читает tar потоково напрямую из source S3;
- загружает только нужные PDF в destination S3;
- обновляет статусы обработки в БД;
- поддерживает heartbeat, батчевый `DONE`, recovery зависших задач и параллельную обработку tar.

---

## Где этот проект находится в пайплайне

Обычно полный pipeline выглядит так:

1. `oai_suprcon_sync` — загружает базовый список статей в `arxiv_paper`.
2. `manifest_parser_sync` — загружает список tar в `pdf_tar_manifest`.
3. `index_manifest_tars` — строит индекс `tar_key <-> arxiv_id` в `pdf_tar_index`.
4. `tar_workers_sync` — достаёт только нужные PDF и переносит их в целевое S3.

---

## Требования

- Python 3.10+
- PostgreSQL
- `boto3`
- `psycopg2-binary`
- доступ к source S3 bucket `arxiv`
- учётные данные destination S3

Установка:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install boto3 psycopg2-binary
```

---

## Переменные окружения

Для destination S3 обязательны:

```bash
export TIMEWEB_ACCESS_KEY_ID="..."
export TIMEWEB_SECRET_ACCESS_KEY="..."
```

Опционально:

```bash
export TIMEWEB_SESSION_TOKEN="..."
```

---

## Быстрый старт

```bash
python tar_workers_sync.py \
  --pg postgresql://postgres:postgres@localhost:5432/rag \
  --dst-bucket my-timeweb-bucket \
  --dst-endpoint https://s3.timeweb.cloud \
  --dst-region ru-1 \
  --src-region us-east-1 \
  --src-request-payer requester \
  --worker-id worker-1 \
  --concurrency 6 \
  --limit-rows 2000 \
  --part-size-mb 32 \
  --done-flush-every 200 \
  --log-level INFO
```

Один цикл обработки:

```bash
python tar_workers_sync.py \
  --pg postgresql://postgres:postgres@localhost:5432/rag \
  --dst-bucket my-timeweb-bucket \
  --dst-endpoint https://s3.timeweb.cloud \
  --once \
  --stop-when-empty
```

---

## Аргументы CLI

### Source S3

```text
--src-bucket              bucket источника, по умолчанию arxiv
--src-region              регион источника, по умолчанию us-east-1
--src-request-payer       requester для arXiv bucket
--src-profile             AWS profile для source
--src-ignore-aws-env      игнорировать AWS_* env vars для source
```

### Destination S3

```text
--dst-bucket              bucket назначения
--dst-endpoint            endpoint назначения
--dst-prefix              префикс объектов, по умолчанию pdf/
--dst-region              регион назначения
--dst-addressing-style    path | virtual
```

### Runtime

```text
--pg                      Postgres DSN
--concurrency             число параллельных tar jobs внутри процесса
--limit-rows              сколько строк arxiv_paper claim'ить за цикл
--worker-id               логический идентификатор воркера
--heartbeat-sec           период heartbeat во время длинной обработки tar
--max-attempts            максимум попыток для записей
--stale-minutes           сбрасывать зависшие задачи старше N минут
--once                    один проход и выход
--stop-when-empty         завершаться, если нет новых задач
--interval-sec            пауза между циклами run_loop
--sleep-on-empty-sec      пауза, если работы нет
--log-level               уровень логирования
--part-size-mb            размер части multipart upload
--done-flush-every        сколько DONE-ids копить перед batch update
```

---

## Предполагаемая схема БД

### Таблица статей

Проект использует `arxiv_paper` как очередь статей.

Минимально полезная форма:

```sql
CREATE TABLE IF NOT EXISTS arxiv_paper (
  arxiv_id       text PRIMARY KEY,
  lookup_key     text,
  status         text NOT NULL DEFAULT 'NEW',
  worker_id      text,
  locked_at      timestamptz,
  attempts       integer NOT NULL DEFAULT 0,
  last_error     text,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
```

### Индекс соответствия tar -> статья

```sql
CREATE TABLE IF NOT EXISTS pdf_tar_index (
  tar_key    text NOT NULL,
  arxiv_id   text NOT NULL,
  PRIMARY KEY (tar_key, arxiv_id)
);
```

---

## Как работает обработка

1. Из `arxiv_paper` claim'ится пачка статей со статусом `NEW`.
2. Для них берутся `lookup_key`.
3. Через `pdf_tar_index` определяется tar, в котором лежит нужный PDF.
4. Статьи группируются по `tar_key`, чтобы один tar читать один раз.
5. Для каждого tar source S3 отдаёт поток `GetObject`.
6. Внутри tar ищутся только нужные `*.pdf`.
7. Каждый найденный PDF грузится в destination S3.
8. Успешные id переводятся в `DONE` батчами.
9. Для долгих tar периодически отправляется heartbeat.
10. Если PDF в tar не найден, статья остаётся в списке `not_found_in_tar`.

---

## Формирование ключа в destination S3

По умолчанию используется префикс `pdf/`, а `arxiv_id` нормализуется в имя файла вида:

```text
pdf/<normalized-arxiv-id>.pdf
```

Примеры:

- `2501.01234v2` -> `pdf/2501.01234.pdf`
- `cond-mat/9801001` -> `pdf/cond-mat_9801001.pdf`

---

## Почему этот проект эффективен

- tar читается потоково, без полной распаковки на диск;
- обрабатываются только нужные PDF, а не весь архив целиком;
- статьи группируются по `tar_key`, что сокращает количество `GetObject`;
- `DONE` обновляется батчами, а не по одной записи;
- heartbeat и reset stale задач делают систему устойчивее к падениям.

---

## Проверка результата

### Сколько статей уже выгружено

```sql
SELECT status, count(*)
FROM arxiv_paper
GROUP BY status
ORDER BY status;
```

### Какие статьи сейчас в обработке

```sql
SELECT arxiv_id, worker_id, locked_at, attempts
FROM arxiv_paper
WHERE status = 'PROCESSING'
ORDER BY locked_at;
```

### Ошибки обработки

```sql
SELECT arxiv_id, attempts, last_error
FROM arxiv_paper
WHERE status = 'FAILED'
ORDER BY updated_at DESC
LIMIT 50;
```

---

## Типовые проблемы

### Нет доступа к Timeweb S3

Проверьте `TIMEWEB_ACCESS_KEY_ID` и `TIMEWEB_SECRET_ACCESS_KEY`.

### Source bucket не отдаёт tar

Проверьте `--src-request-payer requester`, регион и сетевой доступ.

### Много `FAILED`

Смотрите `last_error`, увеличивайте `stale-minutes`, проверяйте целостность tar и права доступа.

### Задачи зависают в `PROCESSING`

Используйте recovery stale задач и не отключайте heartbeat для длительных tar.

---

## Идеи для развития

- отдельная таблица аудита скачанных PDF;
- retry policy по типам ошибок;
- Prometheus-метрики;
- checksum/etag в целевом bucket;
- Dockerfile, systemd unit и k8s deployment манифесты;
- отдельный режим валидации целостности уже загруженных PDF.
