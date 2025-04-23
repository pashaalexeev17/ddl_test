CREATE SCHEMA IF NOT EXISTS mt4;

CREATE TABLE mt4.trades (
    ticket BIGINT PRIMARY KEY,
    login INT,
    open_time TIMESTAMP,
    close_time TIMESTAMP,
    symbol TEXT,
    cmd INT
);

CREATE TABLE mt4.marked_trades (
    ticket BIGINT PRIMARY KEY,
    type TEXT
);


CREATE SCHEMA IF NOT EXISTS mt5;

CREATE TABLE mt5.deals (
    deal BIGINT PRIMARY KEY,
    positionid BIGINT,
    login INT,
    time TIMESTAMP,
    symbol TEXT,
    action INT,
    entry INT
);

CREATE TABLE mt5.marked_trades (
    positionid BIGINT PRIMARY KEY,
    type TEXT
);



COPY mt4.trades FROM '/private/tmp/trade_data/mt4_trades.csv' DELIMITER ',' CSV HEADER;
COPY mt4.marked_trades FROM '/private/tmp/trade_data/mt4_marked_trades.csv' DELIMITER ',' CSV HEADER;

COPY mt5.deals FROM '/private/tmp/trade_data/mt5_deals.csv' DELIMITER ',' CSV HEADER;
COPY mt5.marked_trades FROM '/private/tmp/trade_data/mt5_marked_trades.csv' DELIMITER ',' CSV HEADER;


---task 1-2
--- mt4
WITH mt4_valid_trades AS (
    SELECT *, close_time - open_time as time_between
    FROM mt4.trades
    WHERE ticket NOT IN (
            SELECT ticket 
            FROM mt4.marked_trades 
            WHERE type LIKE '1%'
        )
),

task_1_mt_4 AS (
	SELECT login, COUNT(ticket) as deals_1_minute 
	FROM mt4_valid_trades
	WHERE close_time >= open_time AND time_between < INTERVAL '1 minute' 
	GROUP BY login
),

task_2_mt_4 AS ( --- Оконная не подоходит (не учитывает все пары)
	SELECT a.login, COUNT(*) AS paired_deals_30_sec  
	FROM mt4_valid_trades a
	JOIN mt4_valid_trades b ON 
    a.login = b.login AND
    a.symbol = b.symbol AND
    a.ticket < b.ticket AND 
    a.cmd != b.cmd
	WHERE a.cmd IN (0, 1) AND b.cmd IN (0, 1) AND ABS(EXTRACT(EPOCH FROM (a.open_time - b.open_time))) <= 30
	GROUP BY a.login
	ORDER BY paired_deals_30_sec DESC
),

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
    SELECT positionid, login,
        MIN(CASE WHEN entry = 0 THEN time END) AS open_time,
        MAX(CASE WHEN entry = 1 THEN time END) AS close_time
    FROM mt5_valid_trades
    GROUP BY positionid, login
    HAVING 
        MIN(CASE WHEN entry = 0 THEN time END) IS NOT NULL  
        AND MAX(CASE WHEN entry = 1 THEN time END) IS NOT NULL  
),

task_1_mt_5 AS (
	SELECT login, COUNT(positionid) AS deals_1_minute
	FROM mt5_trade_periods
	WHERE close_time - open_time < INTERVAL '1 minute' AND close_time >= open_time
	GROUP BY login
),

mt5_clean_opens AS (
    SELECT 
        positionid,
        login,
        time AS open_time,
        symbol,
        action
    FROM mt5.deals
    WHERE entry = 0 
),

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
        a.login = b.login AND 
        a.positionid < b.positionid AND  
        a.symbol = b.symbol AND 
        a.action != b.action AND 
        ABS(EXTRACT(EPOCH FROM (a.open_time - b.open_time))) <= 30  
),

task_2_mt_5 AS (
	SELECT 
    login,
    COUNT(*) AS paired_deals_30_sec
	FROM trade_pairs
	GROUP BY login
	ORDER BY paired_deals_30_sec DESC)


SELECT * 
FROM (
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


--- task 3
WITH all_trades AS (
    SELECT 
        login,
        ticket AS trade_id,
        open_time,
        symbol,
        cmd AS action,
        'MT4' AS platform
    FROM mt4.trades
    WHERE ticket NOT IN (
        SELECT ticket 
		FROM mt4.marked_trades 
		WHERE type LIKE '1%'
    ) AND cmd IN (0, 1)
    
    UNION ALL
    
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
            SELECT positionid 
			FROM mt5.marked_trades 
			WHERE type LIKE '1%'
        )
),

time_buckets AS (
    SELECT 
        trade_id,
        login,
        symbol,
        action,
		open_time,
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
        a.login < b.login AND  
        a.action != b.action   
    GROUP BY a.login, b.login, a.time_bucket, a.symbol
    HAVING COUNT(*) > 10
)


SELECT DISTINCT
    login1,
    login2
FROM user_pairs;