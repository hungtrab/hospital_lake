#!/bin/sh
set -eu
# Passwords pass as query parameters, never interpolated into SQL text.
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery < /contracts/schema.sql
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery < /contracts/views.sql
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --param_reader_password="$CLICKHOUSE_GRAFANA_PASSWORD" \
  --query 'CREATE USER IF NOT EXISTS grafana_reader IDENTIFIED WITH sha256_password BY {reader_password:String}'
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --query 'GRANT SELECT ON hospital.* TO grafana_reader'
