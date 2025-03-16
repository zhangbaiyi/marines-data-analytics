DROP TABLE IF EXISTS sites;
CREATE TABLE sites (
    site_id        INTEGER NOT NULL,
    site_name      VARCHAR(50),
    command_name   VARCHAR(30),
    store_type     VARCHAR(20) NOT NULL CHECK (store_type IN ('Main Store', 'Marine Mart'))
);

DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name     VARCHAR(50) NOT NULL,
    metric_desc     VARCHAR(200) NULL,
    is_retail       BOOLEAN DEFAULT 0,
    is_marketing    BOOLEAN DEFAULT 0,
    is_survey       BOOLEAN DEFAULT 0,
    is_daily        BOOLEAN DEFAULT 0,
    is_monthly      BOOLEAN DEFAULT 0,
    is_quarterly    BOOLEAN DEFAULT 0,
    is_yearly       BOOLEAN DEFAULT 0
);

DROP TABLE IF EXISTS period_dim;
CREATE TABLE period_dim (
    id     INTEGER NOT NULL,
    period_name VARCHAR(15) NOT NULL
);

DROP TABLE IF EXISTS facts;
CREATE TABLE facts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_id           INTEGER,
    group_name          VARCHAR(50),
    value               REAL,
    date                DATE,
    period_level        INTEGER,
    record_inserted_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
