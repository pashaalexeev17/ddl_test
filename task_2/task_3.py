import psycopg2
import pandas as pd


DB_PARAMS = {
    "dbname": "trade",
    "user": "postgres",
    "password": "k9413315K",
    "host": "localhost",
    "port": "5432"
}

SQL_QUERY = """
--- task 3
WITH 
-- Объединяем сделки MT4 и MT5
all_trades AS (
    -- MT4 сделки
    SELECT 
        login,
        ticket AS trade_id,
        open_time,
        symbol,
        cmd AS action,
        'MT4' AS platform
    FROM mt4.trades
    WHERE ticket NOT IN (
        SELECT ticket FROM mt4.marked_trades WHERE type LIKE '1%'
    ) AND cmd IN (0, 1)
    
    UNION ALL
    
    -- MT5 сделки (только открытия)
    SELECT 
        login,
        positionid AS trade_id,
        time AS open_time,
        symbol,
        action,
        'MT5' AS platform
    FROM mt5.deals
    WHERE 
        entry = 0 AND
        positionid NOT IN (
            SELECT positionid FROM mt5.marked_trades WHERE type LIKE '1%'
        )
),

-- Создаем 30-секундные интервалы
time_buckets AS (
    SELECT 
        trade_id,
        login,
        symbol,
        action,
		open_time,
        -- Округляем время до 30-секундных интервалов
        DATE_TRUNC('minute', open_time) + 
        (FLOOR(EXTRACT(SECOND FROM open_time)/30)*INTERVAL '30 seconds') AS time_bucket
    FROM all_trades
),

user_pairs AS ( 
SELECT 
        a.login AS login1,
        b.login AS login2,
        COUNT(*) AS pair_count
    FROM time_buckets a
    JOIN time_buckets b ON 
        a.time_bucket = b.time_bucket AND
        a.symbol = b.symbol AND
        a.login < b.login AND  -- Уникальные пары
        a.action != b.action   -- Разные направления
    GROUP BY a.login, b.login, a.time_bucket, a.symbol
    HAVING COUNT(*) > 10
	)


SELECT DISTINCT
    login1,
    login2
FROM user_pairs;
"""

def main():
    try:
  
        conn = psycopg2.connect(**DB_PARAMS)
        print("Успешное подключение к PostgreSQL!")
        
        df = pd.read_sql_query(SQL_QUERY, conn)
        
        df.to_csv('user_pairs.csv', index=False)
        print("Результаты сохранены в trading_metrics.csv")
        
        print("\nПервые 5 строк результатов:")
        print(df.head())
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Соединение с PostgreSQL закрыто.")

if __name__ == "__main__":
    main()