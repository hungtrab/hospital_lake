#!/bin/sh
# Create the warehouse bucket without deleting existing objects.
set -eu
: "${ICEBERG_WAREHOUSE:?Set ICEBERG_WAREHOUSE}"
case "$ICEBERG_WAREHOUSE" in
  s3://*) bucket=${ICEBERG_WAREHOUSE#s3://}; bucket=${bucket%%/*} ;;
  *) echo 'ICEBERG_WAREHOUSE must start with s3://' >&2; exit 1 ;;
esac
case "$bucket" in
  ''|*[!a-z0-9.-]*) echo 'Invalid warehouse bucket name' >&2; exit 1 ;;
esac
: "${MINIO_ENDPOINT:?Set MINIO_ENDPOINT}"
: "${MINIO_ACCESS_KEY:?Set MINIO_ACCESS_KEY}"
: "${MINIO_SECRET_KEY:?Set MINIO_SECRET_KEY}"
endpoint=${MINIO_ENDPOINT%/}/$bucket
request() {
  curl --silent --show-error --max-time 20 \
    --aws-sigv4 "aws:amz:${MINIO_REGION:-us-east-1}:s3" \
    --user "$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY" "$@" "$endpoint"
}
status=$(request --head --output /dev/null --write-out '%{http_code}')
case "$status" in
  200) echo "PASS: warehouse bucket $bucket already exists" ;;
  404)
    status=$(request --request PUT --output /dev/null --write-out '%{http_code}')
    case "$status" in
      200) echo "PASS: created warehouse bucket $bucket" ;;
      409) request --head --fail --output /dev/null
           echo "PASS: warehouse bucket $bucket is accessible" ;;
      *) echo "Bucket creation failed: HTTP $status" >&2; exit 1 ;;
    esac ;;
  *) echo "Bucket access failed: HTTP $status" >&2; exit 1 ;;
esac
