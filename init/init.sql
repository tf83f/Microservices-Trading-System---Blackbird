
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS bybit;

CREATE TABLE bybit.orderbook (
    topic           varchar(255) NOT NULL,
    "type"          varchar(255) NOT NULL,
    ts_message      bigint NOT NULL,
    channel         varchar(255) NOT NULL,
    symbol_name     varchar(255) NOT NULL,
    update_id       bigint NOT NULL,
    sequence_id     bigint NOT NULL,
    side            varchar(255) NOT NULL,
    price           double precision NOT NULL,
    size            double precision NOT NULL,
    ts_orderbook    bigint NOT NULL,
    matching_key    varchar(255) NOT NULL
);

SELECT create_hypertable(
    'bybit.orderbook',
    'ts_orderbook',
    chunk_time_interval => 86400000000::bigint,
    if_not_exists => TRUE
);


CREATE INDEX idx_orderbook_topic_time ON bybit.orderbook (topic, ts_orderbook DESC);
CREATE INDEX idx_orderbook_symbol_time ON bybit.orderbook (symbol_name, ts_orderbook DESC);

CREATE TABLE bybit."order" (
    channel         varchar(255) NOT NULL,
    symbol_name     varchar(255) NOT NULL,
    price           double precision NOT NULL,
    size            double precision NOT NULL,
    "type"          varchar(255) NOT NULL,
    flux            double precision NOT NULL,
    ts_orderbook    bigint NOT NULL
);