# arXiv pdf manifest loader

Скрипт для загрузки файлов из aws в timeweb


---

## Возможности

### Первичный запуск 

```bash

$env:AWS_ACCESS_KEY_ID="..."
$env:AWS_SECRET_ACCESS_KEY="..."

.\.venv\Scripts\python.exe .\tar_workers_sync.py `
  --pg "postgresql://USER:PASS@DBHOST:5432/ragProm" `
  --dst-bucket "YOUR_TIMEWEB_BUCKET" `
  --dst-endpoint "https://s3.timeweb.cloud" `
  --concurrency 6 `
  --limit-rows 1500 `
  --max-attempts 5 `
  --stale-minutes 360 `
  --heartbeat-sec 60 `
  --interval-sec 30 `
  --sleep-on-empty-sec 5 `
  --log-level INFO
```




