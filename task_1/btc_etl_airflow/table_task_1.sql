
CREATE TABLE IF NOT EXISTS btc_usd_raw (
            date_time  DateTime('Europe/Moscow'),
            raw_response  String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(date_time)
            ORDER BY date_time;

CREATE TABLE IF NOT EXISTS btc_usd_optimized (
            date_time     DateTime('Europe/Moscow'),
            currency_pair String,
            rate          Float64
            ) 
            ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(date_time)
            ORDER BY (currency_pair, date_time);

CREATE TABLE IF NOT EXISTS btc_usd_history (
            date DateTime,
            currency_pair String,
            rate Float64
            ) 
            ENGINE = MergeTree()
            ORDER BY date;