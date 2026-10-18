# Build the pinned upstream release without relying on MinIO's image registry.
FROM golang:1.24.2-alpine3.21 AS builder
ENV CGO_ENABLED=0 GOBIN=/out GOMAXPROCS=2
RUN go install -p 2 github.com/minio/minio@RELEASE.2025-04-22T22-12-26Z

FROM alpine:3.21.3
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /out/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
