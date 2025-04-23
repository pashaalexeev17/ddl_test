from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from clickhouse_driver import Client
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import json

# Локальная таймзона
LOCAL_TZ = ZoneInfo("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_store_raw(**kwargs):
    """
    Извлекаем текущий курс по API exchangerate.host,
    данны отправлем в сырую таблицу
    """
    API_KEY = Variable.get("exchange_api_key")
    url = f"https://api.exchangerate.host/live?access_key={API_KEY}&source=BTC&currencies=USD"
    response = requests.get(url, timeout=10)
    data = response.json()

    if response.status_code == 200 and data.get("success", False):
        
        ts_local = datetime.now(LOCAL_TZ).replace(microsecond=0)

        raw_str = json.dumps(data)
        client = Client(host='clickhouse', port=9000, user='default', password='', database='default')

        # Сырая таблица 
        client.execute("""
            CREATE TABLE IF NOT EXISTS btc_usd_raw (
                date_time  DateTime('Europe/Moscow'),
                raw_response  String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(date_time)
            ORDER BY date_time
        """)

        client.execute(
            "INSERT INTO btc_usd_raw (date_time, raw_response) VALUES",
            [(ts_local, raw_str)]
        )

        print(f"Raw data inserted at {ts_local}")
    else:
        raise Exception(f"API error or bad response: {data}")

def transform_raw_to_optimized(**kwargs):
    """
    Обраотка сырых данных и добавление в финальную таблицу
    """

    client = Client(host='clickhouse', port=9000, user='default', password='', database='default')

    # Оптимизированная таблица
    client.execute("""
        CREATE TABLE IF NOT EXISTS btc_usd_optimized (
            date_time     DateTime('Europe/Moscow'),
            currency_pair String,
            rate          Float64
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(date_time)
        ORDER BY (currency_pair, date_time)
    """)

    # Инкрементальная вставка
    insert_sql = """
    INSERT INTO btc_usd_optimized (date_time, currency_pair, rate)
    
    SELECT
        toStartOfHour(
            toDateTime(JSONExtractInt(raw_response, 'timestamp'), 'Europe/Moscow')
        ) AS date_time,

        JSONExtractString(raw_response, 'source') || 'USD' AS currency_pair,

        JSONExtractFloat(
            JSONExtractRaw(raw_response, 'quotes'),
            JSONExtractString(raw_response, 'source') || 'USD'
        ) AS rate
    FROM btc_usd_raw
    WHERE toStartOfHour(
            toDateTime(JSONExtractInt(raw_response, 'timestamp'), 'Europe/Moscow')
        ) NOT IN (SELECT date_time FROM btc_usd_optimized)
    """
    
    client.execute(insert_sql)
    print("transform_raw_to_optimized completed")

with DAG(
    dag_id='btc_usd_etl_pipeline',
    default_args=default_args,
    description='ETL: raw JSON -> optimized table каждые 3 часа (Local Time)',
    schedule_interval='2 0-23/3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['btc', 'clickhouse' 'etl'],
) as dag:

    fetch_raw = PythonOperator(
        task_id='fetch_and_store_raw',
        python_callable=fetch_and_store_raw,
    )

    transform = PythonOperator(
        task_id='transform_raw_to_optimized',
        python_callable=transform_raw_to_optimized,
    )

    fetch_raw >> transform