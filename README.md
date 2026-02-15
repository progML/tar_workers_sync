# arXiv pdf manifest loader

Скрипт для загрузки файлов из aws в timeweb


---

## Возможности

### Первичный запуск 

```bash

$env:TIMEWEB_ACCESS_KEY_ID="..."
$env:TIMEWEB_SECRET_ACCESS_KEY="..."

.\.venv\Scripts\python.exe .\tar_workers_sync.py `
  --pg "postgresql://USER:PASS@DBHOST:5432/ragProm" `
  --dst-bucket "YOUR_TIMEWEB_BUCKET" `
  --dst-endpoint "https://s3.timeweb.cloud" `
  --dst-region "ru-1" `
  --src-region "us-east-1" `
  --src-request-payer "requester" `
  --src-ignore-aws-env `
  --worker-id "w1" `
  --concurrency 10 `
  --limit-rows 20000 `
  --part-size-mb 64 `
  --done-flush-every 1500 `
  --log-level INFO
```




