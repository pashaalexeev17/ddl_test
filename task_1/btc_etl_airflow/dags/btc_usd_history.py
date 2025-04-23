from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from clickhouse_driver import Client
from datetime import datetime, timedelta
import requests
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_store_historical_data(**kwargs):
    """
    Функция для получения исторических данных по BTC/USD (заполняется один раз)
    """

    client = Client(host='clickhouse', port=9000, user='default', password='', database='default')

    table_exists_query = "EXISTS TABLE btc_usd_history"
    table_exists = client.execute(table_exists_query)
    
    if table_exists[0][0] == 1: 
        data_check_query = "SELECT COUNT() FROM btc_usd_history"
        data_count = client.execute(data_check_query)

        if data_count[0][0] > 0:
            print("Historical data already exists in the table. Skipping historical data insertion.")
            return  # Пропускаем этап, если данные уже есть
        else:
            print("Table exists but no data. Inserting historical data.")
    else:
        print("Table does not exist. Creating table and inserting historical data.")
    
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

    API_KEY = Variable.get("exchange_api_key")
    url = f"https://api.exchangerate.host/timeframe?access_key={API_KEY}&source=BTC&currencies=USD&start_date={start_date}&end_date={end_date}"

    response = requests.get(url)
    data = response.json()

    if response.status_code == 200 and data.get("success", False):

        client.execute("""
            CREATE TABLE IF NOT EXISTS btc_usd_history (
                date DateTime,
                currency_pair String,
                rate Float64
            ) ENGINE = MergeTree()
            ORDER BY date
        """)

        historical_data = []
        for date, quotes in data["quotes"].items():
            rate = quotes.get("BTCUSD")
            if rate:
        
                date_time = datetime.strptime(date, "%Y-%m-%d")
                historical_data.append((date_time, 'BTCUSD', rate))

        client.execute(
            "INSERT INTO btc_usd_history (date, currency_pair, rate) VALUES",
            historical_data
        )

        print(f"Historical data inserted from {start_date} to {end_date}")
    else:
        raise Exception(f"API error or bad response: {data}")

def update_historical_data_incrementally(**kwargs):
    """
    Функция для вставки исторических данных из оптимизированных данных
    """

    client = Client(host='clickhouse', port=9000, user='default', password='', database='default')

    # Получаем список уникальных дат из таблицы btc_usd_optimized, которые еще нет в btc_usd_history
    query = """
    SELECT DISTINCT toDate(date_time) AS date
    FROM btc_usd_optimized
    WHERE toDate(date_time) > (SELECT MAX(date) FROM btc_usd_history)
    """
    dates_to_update = client.execute(query)

    
    for date_tuple in dates_to_update:
        date = date_tuple[0]
        print(f"Processing data for {date}")

        # Выполняем агрегацию данных за эту дату
        aggregation_query = f"""
        SELECT round(avg(rate), 2) AS avg_rate
        FROM btc_usd_optimized
        WHERE toDate(date_time) = '{date}'
        """

        aggregated_result = client.execute(aggregation_query)

        if aggregated_result:
            
            avg_rate = aggregated_result[0][0]
            
            insert_query = f"""
            INSERT INTO btc_usd_history (date, currency_pair, rate) VALUES ('{date}', 'BTCUSD', {avg_rate})
            """
        
            client.execute(insert_query)

            print(f"Inserted aggregated data for {date} with avg rate {avg_rate}")
        else:
            print(f"No data found for {date} to aggregate.")

with DAG(
    dag_id='btc_usd_history',
    default_args=default_args,
    description='Загрузка исторических данных по BTC/USD за последние 6 месяцев и ежедневное обновление средней стоимости',
    schedule_interval='0 19 * * *', 
    catchup=False,
    max_active_runs=1,
    tags=['btc', 'clickhouse', 'etl'],
) as dag:

    fetch_historical_data = PythonOperator(
        task_id='fetch_and_store_historical_data',
        python_callable=fetch_and_store_historical_data,
    )

    update_history_data = PythonOperator(
        task_id='update_historical_data_incrementally',
        python_callable=update_historical_data_incrementally,
    )

    fetch_historical_data >> update_history_data