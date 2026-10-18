CREATE TABLE IF NOT EXISTS iceberg.bronze.stream_events_raw (
    _kafka_topic STRING,
    _kafka_partition INT,
    _kafka_offset BIGINT,
    _kafka_timestamp TIMESTAMP,
    raw_payload BINARY,
    _source STRING,
    _ingested_at TIMESTAMP,
    _batch_id BIGINT,
    _event_id STRING,
    _schema_version INT,
    event_time TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS iceberg.bronze.stream_events_quarantine (
    _kafka_topic STRING,
    _kafka_partition INT,
    _kafka_offset BIGINT,
    _kafka_timestamp TIMESTAMP,
    raw_payload BINARY,
    _source STRING,
    _ingested_at TIMESTAMP,
    _batch_id BIGINT,
    _event_id STRING,
    _schema_version INT,
    event_time TIMESTAMP,
    error_reason STRING
) USING iceberg;
