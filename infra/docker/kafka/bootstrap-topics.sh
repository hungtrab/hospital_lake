#!/bin/sh
# No deletes/partition changes: keyed ordering must survive repeated bootstrap.
set -eu
partitions=${KAFKA_TOPIC_PARTITIONS:-3}
case "$partitions" in ''|*[!0-9]*) echo 'KAFKA_TOPIC_PARTITIONS must be a positive integer' >&2; exit 2;; esac
[ "$partitions" -gt 0 ] || { echo 'KAFKA_TOPIC_PARTITIONS must be positive' >&2; exit 2; }
broker=${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}
for topic in hospital.events hospital.dlq; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker" --create --if-not-exists \
        --topic "$topic" --partitions "$partitions" --replication-factor 1
    description=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker" --describe --topic "$topic")
    actual=$(printf '%s\n' "$description" | awk '{for(i=1;i<=NF;i++) if($i=="PartitionCount:") print $(i+1)}')
    replicas=$(printf '%s\n' "$description" | awk '{for(i=1;i<=NF;i++) if($i=="ReplicationFactor:") print $(i+1)}')
    [ "$actual" = "$partitions" ] && [ "$replicas" = '1' ] || {
        echo "FAIL: $topic has incompatible partitions/replicas; migrate explicitly instead of changing keyed routing" >&2
        exit 1
    }
    echo "PASS: $topic partitions=$actual replicas=$replicas"
done
