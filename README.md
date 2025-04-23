## Task 1: Реализация ETL-пайплайна

### Что сделано:
- Полностью рабочий ETL-пайплайн для данных BTC каждые 3 часа + наполнение данными за последние 6 месяцев 
- Интеграция Airflow с ClickHouse через Connection
- Оптимизированная структура хранения данных

### Быстрый старт:
cd task_1/btc_etl_airflow
docker-compose up -d

### Конфигурация:
#### API-ключ:

Через Airflow UI:
Admin → Variables → Добавить:

Key: exchange_api_key

Val: your_api_key_here

### ClickHouse подключение:

Через Airflow UI:
Admin → Connections → Добавить:

ID: clickhouse_default

Type: HTTP

Host: http://clickhouse:8123

login: default

## Task 2: SQL

В данной папке выполненно второе задание. Были добавлены 3 файла для удобства: файлы .py делают запрос и сразу выгружают результат в .csv, в файле DDL.sql хранится чистый SQL код, который использовался в работе.
