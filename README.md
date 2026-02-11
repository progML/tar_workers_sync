# arXiv pdf manifest loader

Скрипт для загрузки файлов из aws в timeweb


---

## Возможности

### Первичный запуск 

```bash
python .\tar_workers_sync.py `
  --pg "postgresql://postgres:postgres@localhost:5432/ragProm" `
  --src-bucket "arxiv" `
  --src-region "us-east-1" `
  --src-request-payer "requester" `
  --src-profile "default" `
  --dst-bucket "9f5a4c69-7242cb33-be89-4bec-9aff-0a58eae53b3b" `
  --dst-endpoint "https://s3.twcstorage.ru" `
  --dst-profile "timeweb" `
  --dst-prefix "pdf/" `
  --worker-id "w1" `
  --concurrency 1 `
  --limit-rows 50000 `
  --interval-sec 60
  
python .\tar_workers_sync.py `
  --pg "postgresql://postgres:postgres@localhost:5432/ragProm" `
  --src-bucket "arxiv" `
  --src-region "us-east-1" `
  --src-request-payer "requester" `
  --src-profile "default" `
  --dst-bucket "9f5a4c69-7242cb33-be89-4bec-9aff-0a58eae53b3b" `
  --dst-endpoint "https://s3.twcstorage.ru" `
  --dst-profile "timeweb" `
  --dst-prefix "pdf/" `
  --worker-id "w2" `
  --concurrency 4 ` - 1 процесс качает 4 tar
  --limit-rows 50000 `
  --interval-sec 60
```

