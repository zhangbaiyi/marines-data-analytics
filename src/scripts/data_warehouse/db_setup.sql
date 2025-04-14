DROP TABLE IF EXISTS sites;
CREATE TABLE sites (
    site_id        INTEGER NOT NULL,
    site_name      VARCHAR(50),
    command_name   VARCHAR(30),
    store_format     VARCHAR(20) NOT NULL CHECK (store_format IN ('MAIN STORE', 'MARINE MART'))
);

DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics (
    id              INTEGER PRIMARY KEY,
    metric_name     VARCHAR(50) NOT NULL,
    metric_desc     VARCHAR(200) NULL,
    is_retail       BOOLEAN DEFAULT 0,
    is_marketing    BOOLEAN DEFAULT 0,
    is_survey       BOOLEAN DEFAULT 0,
    is_daily        BOOLEAN DEFAULT 0,
    is_monthly      BOOLEAN DEFAULT 0,
    is_quarterly    BOOLEAN DEFAULT 0,
    is_yearly       BOOLEAN DEFAULT 0,
    agg_method      VARCHAR(50) NOT NULL,
    etl_method      VARCHAR(200) NOT NULL
);

-- DROP TABLE IF EXISTS period_dim;
-- CREATE TABLE period_dim (
--     id     INTEGER NOT NULL,
--     period_name VARCHAR(15) NOT NULL
-- );

DROP TABLE IF EXISTS facts;
CREATE TABLE IF NOT EXISTS facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_id INTEGER NOT NULL,
    group_name TEXT NOT NULL,
    value REAL NOT NULL,
    date DATE NOT NULL,
    period_level INTEGER NOT NULL,
    record_inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metric_id, group_name, date, period_level)
);

DROP TABLE IF EXISTS camps;

CREATE TABLE camps (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  VARCHAR(100) NOT NULL UNIQUE,
    lat   REAL NOT NULL  CHECK (lat  BETWEEN -90  AND  90),
    long  REAL NOT NULL  CHECK (long BETWEEN -180 AND 180)
);
