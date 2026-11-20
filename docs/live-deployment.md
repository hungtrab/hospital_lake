# Live hospital simulation

The public website is a Cloudflare Worker with static assets. Its `HOSPITAL_API`
VPC service binding reaches `hospital-api:8088` through the named Cloudflare
Tunnel running beside the VPS containers. Database ports remain on loopback.

The simulator sends synthetic journeys to Kafka. Spark preserves Bronze,
validates the Silver event ledger and publishes ClickHouse snapshots. The web
API queries ClickHouse and Trino; it never computes hospital totals from the
producer's SQLite database. SQLite holds schedules, manual operator commands, current manual bed assignments and the durable producer outbox.
The hourly Spark job recomputes `gold.live_department_daily_metrics` from Silver;
the weekly report uses that historical table and excludes today's partial day.

## Manual admission demo

Open **Nhập tay**, enter a synthetic code such as `DEMO-22`, optionally enter age and condition group, select **Cấp cứu**, then choose **Nhập viện**. **Xem trên bản đồ** locates the assigned bed after Spark processes the event. Return to **Nhập tay**, select **Tim mạch** as the destination and choose **Chuyển khoa**. **Xuất viện** releases the second bed. **Xem hành trình** retrieves the registration, admission, assignment, transfer, release and discharge from the Silver ledger. Automatic arrivals can be paused throughout this demo.

The opt-in browser test creates and discharges a synthetic patient. With `HOSPITAL_WEB_URL` and `HOSPITAL_WEB_PASSWORD` set in the environment, run `node tests/e2e/hospital_manual.browser.cjs`.

## Operation

New installations start paused. The VPS pilot was enabled on **2026-11-19**.
The pilot switched to the Bạch Mai (central Hanoi) profile on the same day. The producer keeps
started journeys and uses a time-varying Poisson process for future registrations.
The live settings are `SIM_PROFILE=bach-mai-v1` and `SIM_DAILY_VISITS=5315`.
This value controls the long-run mean across weekdays and weekends. Individual
dates have random counts. See [calibration and resources](hospital-profile.md).
Unstarted visits missed by over three seconds expire. Patients already registered
keep their follow-up events in the durable Kafka outbox.
New arrivals cover 31 dates (21 Nov–21 Dec); already planned discharges continue
afterwards. Plan a new run explicitly if more arrival dates are needed.
This is synthetic operational data, not real patients or medical guidance.

Open the website over HTTPS, sign in as **admin** in the browser login dialog,
then choose **Bật mô phỏng**. The account protects both the dashboard and the API. Enabling before the start date does not send visits early.
Pause/resume and schedule state survive an API/container restart. If Kafka is
unavailable, events stay pending. An ambiguous acknowledgement can replay a
stable event ID; downstream deduplication handles that retry.

On the VPS, from `~/hospital-lake-live`, `make live-status`, `make live-up`,
and `make live-stop` preserve all state. `make live-check PYTHON=.runtime/bin/python`
runs a known synthetic journey with a Spark restart; run this on a quiet stack.
The equivalent inspection commands are:

```sh
docker compose -p hospital-live --env-file homecoming.env \
  -f docker-compose.yml -f infra/deploy/compose.live.yml ps
docker compose -p hospital-live --env-file homecoming.env \
  -f docker-compose.yml -f infra/deploy/compose.live.yml logs --tail 80 stream hospital-api
```

Keep `homecoming.env` private (mode 600). Do not use `down -v`: the volumes hold
Kafka, Iceberg/MinIO, ClickHouse and the simulator outbox. Stop/start or recreate
containers with volumes retained. The live stack runs only `realtime_aggregates.py`
as its streaming writer; the compatible clinical helpers are not a second writer.

## Rebuild on a fresh machine

Create private credentials from `.env.example` plus the required variables in
`infra/deploy/compose.live.yml`. Build the pinned MinIO and Iceberg images from
their existing Dockerfiles, and build `hospital-lake/spark:completion` with
`infra/docker/spark/Dockerfile`. Build `hospital-api` using the Compose overlay.
The preloaded image tag does not embed machine addresses; repository source is
mounted read-only and endpoints come from container environment variables.

Create a Cloudflare Tunnel and HTTP VPC service targeting `hospital-api:8088`;
put its service ID in `dashboard/hospital/wrangler.jsonc`, its tunnel token in the
private VPS environment. Prepare a mode-600 JSON file outside source control with
the matching VPS values for `HOSPITAL_SERVICE_TOKEN` and `HOSPITAL_ADMIN_TOKEN`,
and a `HOSPITAL_LOGIN_DIGEST` field. Compute the latter as lowercase hex SHA-256
of the exact UTF-8 Authorization header value: `Basic ` followed by standard
Base64 of UTF-8 `admin:<password>`. Use a password prompt or secret manager.
After the Kubernetes and browser checks pass, run `npm ci` and
`npx wrangler deploy --secrets-file /private/path/secrets.json` in `dashboard/hospital`.
Never put either API credential in static assets or Wrangler plaintext config.

## Validation boundaries

Unit tests cover deterministic schedules, outbox retries, overnight journeys,
input contracts and API authentication. Worker tests cover fixed backend routes,
credential replacement, mutation authorization, body limits and safe failures.
The live integration gate is `scripts/realtime_smoke.py`: it checks Kafka bytes,
quarantine, Silver deduplication, abnormal labs, ClickHouse and Grafana results.
Record actual results in `progress.md`; Compose deployment does not certify the
separate Kubernetes, scaling or failure-recovery integration gates.
