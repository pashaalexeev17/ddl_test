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
---task 1-2
--- mt4
WITH mt4_valid_trades AS (
    SELECT 
        *,
		close_time - open_time as time_between
    FROM mt4.trades
    WHERE ticket NOT IN (
            SELECT ticket 
            FROM mt4.marked_trades 
            WHERE type LIKE '1%'
        )
),

task_1_mt_4 AS (SELECT login, COUNT(ticket) as deals_1_minute 
FROM mt4_valid_trades
WHERE close_time >= open_time AND time_between < INTERVAL '1 minute' 
GROUP BY login),

task_2_mt_4 AS (SELECT  --- Оконная не подоходит (не учитывает все пары)
    a.login,
    COUNT(*) AS paired_deals_30_sec  -- Делим на 2 чтобы избежать дублирования
FROM mt4_valid_trades a
JOIN mt4_valid_trades b ON 
    a.login = b.login AND
    a.symbol = b.symbol AND
    a.ticket  < b.ticket AND 
    a.cmd != b.cmd
WHERE a.cmd IN (0, 1) AND b.cmd IN (0, 1) AND ABS(EXTRACT(EPOCH FROM (a.open_time - b.open_time))) <= 30
GROUP BY a.login
ORDER BY paired_deals_30_sec DESC),

--- mt5
mt5_valid_trades AS (
    SELECT *
    FROM mt5.deals
    WHERE positionid NOT IN (
            SELECT positionid 
            FROM mt5.marked_trades 
            WHERE type LIKE '1%'
        )
),

mt5_trade_periods AS (
    SELECT 
        positionid,
        login,
        MIN(CASE WHEN entry = 0 THEN time END) AS open_time,
        MAX(CASE WHEN entry = 1 THEN time END) AS close_time
    FROM mt5_valid_trades
    GROUP BY positionid, login
    HAVING 
        MIN(CASE WHEN entry = 0 THEN time END) IS NOT NULL  
        AND MAX(CASE WHEN entry = 1 THEN time END) IS NOT NULL  
),

task_1_mt_5 AS (SELECT 
    login,
    COUNT(positionid) AS deals_1_minute
FROM mt5_trade_periods
WHERE 
    close_time - open_time < INTERVAL '1 minute' AND close_time >= open_time
GROUP BY login),

mt5_clean_opens AS (
    SELECT 
        positionid,
        login,
        time AS open_time,
        symbol,
        action
    FROM mt5.deals
    WHERE entry = 0 ),

trade_pairs AS (
    SELECT 
        a.login,
        a.positionid AS positionid1,
        b.positionid AS positionid2,
        a.open_time AS open_time1,
        b.open_time AS open_time2,
        a.symbol,
        a.action AS action1,
        b.action AS action2,
        ABS(EXTRACT(EPOCH FROM (a.open_time - b.open_time))) AS time_diff_sec
    FROM mt5_clean_opens a
    JOIN mt5_clean_opens b ON 
        a.login = b.login AND  -- Один пользователь
        a.positionid < b.positionid AND  -- Уникальные пары
        a.symbol = b.symbol AND  -- Один инструмент
        a.action != b.action AND  -- Противоположные направления
        ABS(EXTRACT(EPOCH FROM (a.open_time - b.open_time))) <= 30  -- Разница <=30 сек
),

task_2_mt_5 AS (SELECT 
    login,
    COUNT(*) AS paired_deals_30_sec
FROM trade_pairs
GROUP BY login
ORDER BY paired_deals_30_sec DESC)


SELECT * FROM (
    SELECT COALESCE(a.login, b.login) AS login,
           COALESCE(a.deals_1_minute, 0) AS deals_1_minute,
           COALESCE(b.paired_deals_30_sec, 0) AS paired_deals_30_sec 
    FROM task_1_mt_4 a
    FULL JOIN task_2_mt_4 b ON a.login = b.login
    
    UNION ALL
    
    SELECT COALESCE(a.login, b.login) AS login,
           COALESCE(a.deals_1_minute, 0) AS deals_1_minute,
           COALESCE(b.paired_deals_30_sec, 0) AS paired_deals_30_sec 
    FROM task_1_mt_5 a
    FULL JOIN task_2_mt_5 b ON a.login = b.login
) combined_results
ORDER BY deals_1_minute DESC, paired_deals_30_sec DESC;
"""

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Успешное подключение к PostgreSQL!")
        
        df = pd.read_sql_query(SQL_QUERY, conn)
        
        df.to_csv('trading_metrics.csv', index=False)
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