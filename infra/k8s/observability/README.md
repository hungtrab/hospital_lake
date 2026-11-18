# TV4 serving and observability on Kubernetes

These manifests deploy ClickHouse, Grafana and Prometheus into the existing
`hospital-lake` namespace. ClickHouse, Grafana and Prometheus each retain their
data on a PVC. Grafana dashboards and datasource provisioning are rebuilt from
the source files in `dashboard/grafana/` whenever the deployment target runs.

Set these values in the local shell before deploying; do not add them to Git.
In PowerShell:

```powershell
$env:CLICKHOUSE_GRAFANA_PASSWORD = '<local read-only ClickHouse password>'
$env:GRAFANA_ADMIN_PASSWORD = '<local Grafana admin password>'
```

In Bash:

```bash
export CLICKHOUSE_GRAFANA_PASSWORD='<local read-only ClickHouse password>'
export GRAFANA_ADMIN_PASSWORD='<local Grafana admin password>'
```

Then run (set `PYTHON=python` if `python3` is not on `PATH`):

```bash
make k8s-observability-up
kubectl get pods -n hospital-lake
```

The Make target waits for ClickHouse, Prometheus and Grafana rollouts to become
ready before returning successfully. The deploy script pipes Secret data to
`kubectl` on stdin, so credential values do not appear in process arguments.

The ClickHouse password is stored in a Kubernetes Secret and used for the
`grafana_reader` account, which is limited to read-only queries against the
`hospital` database. Prometheus scrapes ClickHouse's `/metrics` endpoint on
port 9363. Grafana receives Trino, ClickHouse and Prometheus datasources plus
the provisioned HospitalLake dashboards.

The services use ClusterIP. To open the UIs locally, run these in separate
terminals:

```bash
kubectl port-forward -n hospital-lake svc/grafana 3000:3000
kubectl port-forward -n hospital-lake svc/prometheus 9090:9090
```

Grafana is at `http://localhost:3000` with username `admin` and the local
`GRAFANA_ADMIN_PASSWORD`. Prometheus is at `http://localhost:9090`. The
ClickHouse HTTP endpoint can be reached with
`kubectl port-forward -n hospital-lake svc/clickhouse 8123:8123`.

The PVCs request 10 GiB for ClickHouse, 2 GiB for Grafana and 5 GiB for
Prometheus. The cluster must provide a default StorageClass capable of
`ReadWriteOnce` volumes. This is a single-replica development deployment; it
does not claim high availability or production security.
