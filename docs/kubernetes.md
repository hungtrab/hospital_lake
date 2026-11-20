# Local Kubernetes profile

This profile uses a separate kind cluster named `hospital-lake-lab` and stores
its kubeconfig in `results/k8s/kubeconfig`. Commands do not change your current
Kubernetes context. It is a single-node teaching/demo environment, with one
Kafka broker and one replica of the storage/catalog services. It is not HA.

The November 19 lab run completed batch processing for 100 patients and 3,315
source records. Its Kafka-to-Grafana check preserved raw bytes, detected
duplicate/invalid events and returned hospital occupancy to zero after discharge.
The run used one physical machine. Current experiment results are in `progress.md`.

## Deploy and run

Use Linux with Docker, the repository Python environment, and enough free
memory for the full stack. Stop the Compose workload first if the machine
cannot run both stacks at once. Image import checks for at least 10 GiB free
in Docker storage; it never automatically removes images, caches or volumes.

```bash
make setup PYTHON=.venv/bin/python
make k8s-up PYTHON=.venv/bin/python
make k8s-status PYTHON=.venv/bin/python
make k8s-batch PYTHON=.venv/bin/python K8S_PATIENTS=1000 K8S_EXECUTORS=2
make k8s-stream PYTHON=.venv/bin/python K8S_EXECUTORS=2
make k8s-smoke PYTHON=.venv/bin/python
```

The setup command preserves an existing `.env`. Review its synthetic local
credentials before deployment. `k8s-up` builds the application image, installs
checksum-verified kind/kubectl/Helm in `results/tools/bin`, loads images, installs
the pinned operators, provisions configuration/Secrets, bootstraps storage and
ClickHouse accounts, and waits for services/topics. Reapplying configuration
does not delete PVCs or pipeline checkpoints.

The batch application reuses the generator, Bronze, Silver and Gold modules.
The stream application uses the same two-query processing code as Compose.
The smoke Job sends known events and asserts hospital state, lab metrics,
invalid/duplicate/late behavior, Grafana panel results, and exact Bronze bytes
and Kafka coordinates. Its report is on the Spark PVC at
`/state/results/k8s-realtime.json`; Job logs also show the result. Run it with
the general producer stopped so its expected state deltas are unambiguous.

## View services and workload

```bash
.venv/bin/python -m scripts.k8s forward --service grafana --port 3000 --local-port 23001
# Separate terminal:
.venv/bin/python -m scripts.k8s forward --service prometheus --port 9090 --local-port 29090
# Workload:
make k8s-produce PYTHON=.venv/bin/python K8S_RATE=100
make k8s-stop-producer PYTHON=.venv/bin/python
```

Port forwarding binds to localhost. Grafana uses the credentials provisioned
from `.env`. Prometheus is configured for Spark progress/lag, Kafka JMX,
ClickHouse metrics and Kubernetes cAdvisor. Scrape configuration alone is not
proof those targets or all panels work; inspect target health during the live gate.

## Scaling and restart semantics

`make k8s-stream K8S_EXECUTORS=4` changes the SparkApplication specification.
The pinned operator resubmits the application; this is a restart with preserved
checkpoints, not live dynamic allocation. The command waits for a new submission
and the requested running executors. Run the recovery experiment on a quiet demo cluster:

```bash
make k8s-recovery PYTHON=.venv/bin/python K8S_RATE=30
```

This records observed throughput and Kafka lag at two load levels, resubmits
with four executors, removes one running executor while Kafka offsets advance,
then compares Kafka offsets with Bronze and the serving checkpoint. The final
report is written after the producer has stopped and both readers catch up.

Spark driver/executors share a `ReadWriteOnce` PVC on the single kind node.
Moving this profile to multiple worker nodes requires a suitable shared storage
design; the current PVC layout must not be presented as multi-node storage.
The local Iceberg REST fixture persists SQLite metadata on its own PVC and uses
one JDBC connection to avoid concurrent SQLite snapshot-lock errors.

```bash
make k8s-down PYTHON=.venv/bin/python
```

This stops the kind node and keeps its data. `make k8s-up` resumes it. No reset
or cluster deletion is performed by these commands. Set `HOSPITAL_KIND_CLUSTER=hospital-lake-final` before the commands to select
a second cluster with its own kubeconfig and storage. Choose an unused cluster
name when testing installation from scratch.
