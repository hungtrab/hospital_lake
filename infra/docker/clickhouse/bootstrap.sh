#!/bin/sh
set -eu
# Hash secrets before constructing SQL; arbitrary password bytes are not SQL.
writer_hash=$(printf %s "$CLICKHOUSE_PASSWORD" | sha256sum | cut -d' ' -f1)
reader_hash=$(printf %s "$CLICKHOUSE_GRAFANA_PASSWORD" | sha256sum | cut -d' ' -f1)
clickhouse-client --host clickhouse --user hospital_admin --password "$CLICKHOUSE_ADMIN_PASSWORD" --multiquery < /schema.sql
clickhouse-client --host clickhouse --user hospital_admin --password "$CLICKHOUSE_ADMIN_PASSWORD" --multiquery <<SQL
CREATE USER IF NOT EXISTS hospital_writer IDENTIFIED WITH sha256_hash BY '$writer_hash';
ALTER USER hospital_writer IDENTIFIED WITH sha256_hash BY '$writer_hash';
GRANT SELECT, INSERT ON hospital.* TO hospital_writer;
CREATE USER IF NOT EXISTS grafana_reader IDENTIFIED WITH sha256_hash BY '$reader_hash';
ALTER USER grafana_reader IDENTIFIED WITH sha256_hash BY '$reader_hash';
GRANT SELECT ON hospital.* TO grafana_reader;
ALTER USER grafana_reader SETTINGS readonly=1, max_execution_time CHANGEABLE_IN_READONLY;
SQL
