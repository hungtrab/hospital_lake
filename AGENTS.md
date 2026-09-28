# AGENTS.md — HospitalLake Repository Instructions

> **This file is the mandatory operating contract for every coding agent working in this repository.**
>
> Before making any change, every agent **MUST read**:
>
> 1. [`plan.md`](./plan.md) — source of truth for architecture, scope, data model, pipeline, milestones, acceptance criteria, and technical priorities.
> 2. [`works.md`](./works.md) — source of truth for the 9-week work allocation across TV1–TV4, weekly integration gates, workload balance, and contribution requirements.
> 3. This file — source of truth for how agents must execute tasks inside the repository.
>
> If instructions conflict:
>
> ```text
> explicit human instruction
>     >
> AGENTS.md
>     >
> works.md
>     >
> plan.md
>     >
> existing implementation assumptions
> ```
>
> Do not silently invent a new architecture when the documents already specify one.

---

# 1. Project Identity

The project is:

> **HospitalLake — Cloud-Native Hospital Data Lakehouse for Batch and Real-Time Analytics**

The purpose of the project is to build a simplified but technically credible hospital Data Lakehouse that:

- integrates hospital-like historical and real-time data;
- implements **Bronze → Silver → Gold** data layers;
- supports **batch processing**;
- supports **stream processing**;
- demonstrates a clear **Lambda Architecture**;
- exposes historical and realtime analytics;
- runs on **Kubernetes**;
- demonstrates distributed processing, scalability, observability, and recovery.

This is primarily a **Big Data / Data Engineering project**.

It is **not** primarily:

- an AI diagnosis project;
- an ML research project;
- an EHR frontend;
- a medical recommendation system;
- an exercise in maximizing the number of technologies.

---

# 2. Mandatory Reading Before Coding

At the start of every agent session:

```text
READ plan.md
READ works.md
READ AGENTS.md
READ sitemap.md
```

[`sitemap.md`](./sitemap.md) maps every directory to its purpose, planned files, milestone, and the TV who touches it most. Put new code where `sitemap.md` says it belongs; if you add a new directory, update `sitemap.md` in the same change.

Then inspect:

```text
git status
git log --oneline -10
git branch --show-current
```

Then inspect only the relevant directories for the assigned task.

Do not begin implementation from memory or from a previous conversation summary if the repository contains newer instructions.

---

# 3. Sources of Truth

## `plan.md`

Use `plan.md` to answer questions such as:

- What is the architecture?
- What technologies should be used?
- What is Bronze/Silver/Gold?
- What are the schemas?
- Which metrics must exist?
- What belongs in batch versus stream?
- What should ClickHouse store?
- What should Trino query?
- How should Kubernetes be used?
- What is the minimum viable project?
- What features are optional?

Never replace a specified core technology without an explicit team decision.

---

## `works.md`

Use `works.md` to answer:

- What should TV1/TV2/TV3/TV4 work on this week?
- What counts as a real contribution?
- What is the integration gate for the current week?
- Which tasks must be completed before proceeding?
- How do we keep work balanced?
- What cross-module work is expected?

No agent should decide that another teammate can "just do docs" for a week.

Every teammate must make a meaningful executable contribution every week.

---

# 4. Core Architecture — Do Not Drift

The intended system is:

```text
                 HISTORICAL DATA
                       |
                       v
                 Apache Spark
                       |
                       v

     +--------------------------------------+
     |                                      |
     |          DATA LAKEHOUSE              |
     |       MinIO + Apache Iceberg         |
     |                                      |
     |  Bronze  --->  Silver  --->  Gold    |
     |                                      |
     +--------------------------------------+
                       |
                       +-------------> Trino
                       |                |
                       |                v
                       |        Historical Analytics
                       |
                       |
REALTIME EVENTS         |
      |                 |
      v                 |
   Kafka                |
      |                 |
      v                 |
Spark Structured        |
Streaming --------------+
      |
      +---------------------> ClickHouse
                                |
                                v
                        Realtime Analytics

Trino + ClickHouse
        |
        v
     Grafana

Infrastructure:
Kubernetes + Spark Operator + Strimzi + Prometheus
```

The agent must preserve this conceptual separation.

---

# 5. Technology Contract

## Core technologies

The baseline stack is:

```text
Storage            MinIO / S3-compatible object storage
Lakehouse          Apache Iceberg
Batch              Apache Spark
Event transport    Apache Kafka
Streaming          Spark Structured Streaming
Historical query   Trino
Realtime serving   ClickHouse
Visualization      Grafana
Orchestration      Kubernetes
Spark on K8s       Spark Operator
Kafka on K8s       Strimzi
Monitoring         Prometheus
```

Do not replace them casually.

---

## Technologies NOT required for the core project

Do not add these unless the core project is stable and the team explicitly wants the extension:

```text
Flink
Airflow
Debezium
MLflow
OpenMetadata
Feast
Neo4j
vector databases
LLM services
complex ML models
service mesh
multi-region Kubernetes
```

Agents have a strong tendency to over-engineer. Avoid that.

If a feature can be completed cleanly with the existing stack, do not introduce another distributed system.

---

# 6. Priority System

Use the priority order defined in `plan.md`.

## P0 — must work first

```text
data generation
MinIO + Iceberg
Spark batch
Bronze → Silver → Gold
Trino historical query
```

## P1 — core realtime product

```text
Kafka
Spark Structured Streaming
realtime aggregation
ClickHouse
Grafana
```

## P2 — distributed systems demonstration

```text
Kubernetes
Prometheus
scaling
fault recovery
benchmarking
```

## P3 — optional

```text
MIMIC-IV integration
Airflow
CDC
ML
advanced governance
```

Rule:

> Never work on P3 while a P0 or P1 dependency required by the current milestone is broken.

---

# 7. Assignment Discipline

Each teammate should know whether they are operating as:

```text
TV1
TV2
TV3
TV4
```

The actual assignment for the current week comes from `works.md`.

Before coding, write down internally:

```text
Current week:
Current TV:
Assigned task:
Expected output:
Acceptance criterion:
Integration dependency:
```

Do not take work assigned to another teammate unless:

1. the task is blocked;
2. coordination has occurred; or
3. you are fixing an integration bug.

Do not create a permanent "owner" hierarchy.

No teammate is the lead by default.

---

# 8. Every Week Must Produce Real Code

Every teammate must merge meaningful executable work every week.

Valid primary contributions include:

- Spark jobs;
- data generators;
- Kafka producers/consumers;
- SQL schemas and executable analytical queries;
- Iceberg setup;
- streaming logic;
- ClickHouse schemas/sinks;
- Grafana provisioning;
- Kubernetes manifests/Helm values;
- Prometheus configuration;
- benchmark scripts;
- load generators;
- integration tests;
- E2E tests;
- recovery/failure-injection scripts;
- reusable automation.

The following do **not** count as the primary weekly contribution:

- README-only changes;
- docs-only changes;
- formatting-only commits;
- renaming files;
- changing comments;
- TODO placeholders;
- screenshots;
- slides;
- reviewing somebody else's PR.

Documentation is necessary, but it must accompany technical work rather than replace it.

---

# 9. Task Execution Protocol

For every task:

## Step 1 — Understand the boundary

Identify:

```text
INPUT
PROCESSING
OUTPUT
BATCH OR STREAM
DEPENDENCIES
CONSUMERS
```

Example:

```text
Input:
bronze.encounters_raw

Processing:
normalize timestamps
normalize department
deduplicate encounter_id
validate patient_id

Output:
silver.encounters

Mode:
batch

Consumer:
Gold aggregation jobs
```

If you cannot state these clearly, do not start coding yet.

---

## Step 2 — Inspect existing implementation

Search for:

- existing schemas;
- helpers;
- config;
- naming conventions;
- tests;
- related jobs.

Avoid parallel implementations of the same concept.

Do not duplicate a utility because it is easier than understanding the existing one.

---

## Step 3 — Implement the smallest complete vertical slice

Prefer:

```text
input
→ processing
→ output
→ validation
```

over creating a large amount of disconnected scaffolding.

---

## Step 4 — Test locally

Minimum:

- syntax/import check;
- unit test where appropriate;
- smoke test;
- integration check for external systems where applicable.

---

## Step 5 — Verify downstream compatibility

If your task creates an output that another component consumes, verify the actual schema and behavior.

Do not assume that "the table exists" means the task is complete.

---

## Step 6 — Provide reproducible command

The feature must have a clear command such as:

```bash
make batch
python generators/stream_generator/producer.py --rate 1000
pytest tests/integration/test_kafka_to_spark.py
kubectl apply -f ...
```

Avoid manual UI configuration when it can be provisioned from source control.

---

# 10. Definition of Done

A code task is done only when all applicable conditions hold:

- implementation exists;
- code executes;
- expected output is produced;
- validation/test exists;
- error handling is reasonable;
- configuration is externalized;
- no secrets are hardcoded;
- existing integration is not broken;
- output schema matches downstream expectations;
- restart/retry behavior is considered when applicable;
- acceptance criterion in `works.md` is met.

"Implemented" does not mean "file created."

---

# 11. Data Contracts

The core domain entities are specified in `plan.md`.

Do not casually rename fields.

Core concepts include:

```text
patient
encounter
lab_result
medication
vital_measurement
bed_event
```

Every table/event should use stable IDs.

Prefer:

```text
patient_id
encounter_id
event_id
measurement_id
```

Avoid using mutable descriptive values as keys.

---

# 12. Streaming Event Contract

Streaming events must use a stable envelope similar to:

```json
{
  "event_id": "uuid",
  "event_type": "ADMISSION",
  "event_time": "2026-09-28T10:25:00Z",
  "source": "hospital-simulator",
  "version": 1,
  "payload": {}
}
```

Required concerns:

- `event_id` for deduplication;
- `event_time` for event-time processing;
- `version` for schema evolution;
- invalid events must not crash the entire stream;
- bad events must be visible through quarantine/DLQ.

Do not use processing time as a substitute for event time unless the exact metric requires it.

---

# 13. Bronze Rules

Bronze means **raw ingestion**, not "slightly cleaned."

Bronze should:

- preserve original information;
- add ingestion metadata;
- tolerate imperfect data;
- preserve source timestamps;
- provide replay/reprocessing ability.

Bronze should not:

- calculate business KPIs;
- heavily normalize the domain;
- silently delete bad records;
- contain dashboard-specific aggregates.

Typical metadata:

```text
_source
_ingested_at
_batch_id
_event_id
_schema_version
```

---

# 14. Silver Rules

Silver is the normalized, validated data model.

Silver is responsible for:

- type conversion;
- timestamp normalization;
- deduplication;
- canonical department names;
- required-field validation;
- reasonable domain validation;
- normalized entity relationships;
- reusable clean tables.

Bad rows should have a traceable reason for rejection/quarantine.

Avoid silently dropping data.

---

# 15. Gold Rules

Gold is business/analytics-ready.

Gold tables should answer real questions.

Examples:

```text
hospital_hourly_metrics
department_daily_metrics
icu_occupancy
lab_daily_metrics
patient_timeline
hospital_current_state
```

A Gold table must have an intended consumer.

Do not create Gold tables "because medallion architecture needs more tables."

---

# 16. Batch vs Streaming Contract

Keep responsibilities explicit.

## Batch

Batch is for:

- large historical ingestion;
- complete recomputation;
- long-range aggregation;
- historical trends;
- reprocessing.

Typical flow:

```text
files
→ Spark
→ Bronze
→ Silver
→ Gold
→ Trino
```

---

## Streaming

Streaming is for:

- current state;
- recent windows;
- low-latency metrics;
- operational dashboard updates.

Typical flow:

```text
event generator
→ Kafka
→ Structured Streaming
→ Iceberg / ClickHouse
→ Grafana
```

Do not duplicate all historical batch analytics inside the stream processor.

---

# 17. Lambda Architecture Requirement

The project must remain explainable as:

```text
              Hospital Data
              /           \
             /             \
            v               v
       Batch Layer      Speed Layer
          Spark       Kafka + Streaming
            |               |
            v               v
     Historical Gold   Realtime Gold
             \             /
              \           /
               v         v
                Serving
          Trino + ClickHouse
                  |
               Grafana
```

When adding a component, ask:

> Which Lambda layer does this component belong to, and why?

If the answer is unclear, reconsider whether the component belongs in the project.

---

# 18. Iceberg and MinIO Rules

Iceberg is the Lakehouse table format.

MinIO is the object storage.

Agents must not treat MinIO folders as the analytical data model.

Prefer table operations through Iceberg rather than manually managing arbitrary Parquet paths after Iceberg is established.

Pay attention to:

- partitioning;
- schema evolution;
- snapshots;
- idempotent writes;
- small files;
- table metadata.

Do not over-partition.

---

# 19. Trino Rules

Trino is the historical/ad-hoc SQL engine.

Use it to query Iceberg.

Do not duplicate the full Lakehouse into another database just to query it.

Queries should be reproducible from source-controlled SQL where practical.

---

# 20. ClickHouse Rules

ClickHouse is a serving layer for fast realtime/dashboard queries.

It is **not the source of truth**.

Store only what is required for low-latency serving.

Do not mirror every Bronze/Silver table into ClickHouse.

If a query is historical and naturally belongs in Iceberg/Trino, keep it there.

---

# 21. Grafana Rules

Dashboards must be provisioned from code where practical.

Avoid manual dashboards that cannot be recreated on another machine.

Required dashboard categories from `plan.md`:

```text
Hospital Overview
Realtime Operations
Historical Analytics
```

Dashboard panels must be backed by actual pipeline outputs.

Never fake a dashboard using static values.

---

# 22. Kubernetes Rules

Kubernetes exists to demonstrate:

- deployment;
- service discovery;
- scaling;
- restart/recovery;
- persistent infrastructure;
- observability.

Do not use Kubernetes merely as a packaging layer.

Expected components include:

```text
Kafka / Strimzi
Spark Operator
MinIO
Iceberg Catalog
Trino
ClickHouse
Grafana
Prometheus
stream generator
```

Use:

- Deployments for stateless services;
- StatefulSets/operators where appropriate;
- PVCs for persistent state;
- ConfigMaps for configuration;
- Secrets for credentials;
- Services for communication.

Do not bake environment-specific addresses into application source code.

---

# 23. Kubernetes Is Not the First Development Environment

The required development order is:

```text
local logic
    ↓
Docker/local services
    ↓
integration works
    ↓
Kubernetes
```

Do not debug application logic, Kafka schemas, Iceberg configuration, and Kubernetes networking simultaneously if the same feature has never worked locally.

---

# 24. Configuration Rules

All environment-specific values belong in configuration or environment variables.

Example:

```env
MINIO_ENDPOINT=
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=

KAFKA_BOOTSTRAP_SERVERS=

CLICKHOUSE_HOST=
CLICKHOUSE_PORT=

TRINO_HOST=
TRINO_PORT=

ICEBERG_CATALOG_URI=
```

Provide sensible local defaults only when safe.

Never commit real credentials.

---

# 25. Logging Rules

Applications must log useful state.

Batch jobs should log at least:

```text
job name
batch ID
input rows
output rows
invalid rows
duplicate rows
duration
```

Streaming applications should log/measure:

```text
events received
events processed
invalid events
late events
duplicates
processing latency
checkpoint/restart state where practical
```

Do not flood logs with one line per event at high volume.

---

# 26. Testing Contract

Testing is not optional.

## Unit tests

Use for:

- normalization functions;
- schema validation;
- event validation;
- aggregation logic;
- deterministic generator behavior.

## Integration tests

Use for:

```text
Spark → Iceberg
Trino → Iceberg
Kafka → Spark
Spark → ClickHouse
```

## E2E tests

At least one path must validate:

```text
generate event
→ Kafka
→ Structured Streaming
→ serving table
→ query expected result
```

Tests must fail loudly and specifically.

---

# 27. Data Generator Rules

Synthetic data is the default development dataset.

Generator requirements:

- configurable scale;
- deterministic seed;
- realistic foreign keys;
- plausible hospital event order;
- Parquet preferred for batch;
- configurable event rate for stream.

The streaming simulator should intentionally support some:

```text
late events
duplicate events
invalid events
```

because those are useful Big Data processing cases.

Do not generate only perfectly clean data.

---

# 28. Performance Rules

Do not optimize blindly.

Measure before and after.

When changing:

- partitions;
- executor count;
- file size;
- caching;
- repartitioning;
- ClickHouse layout;
- Kafka partition count;

record measurable effects.

No performance claim should be made without a reproducible test.

---

# 29. Benchmark Rules

Benchmarks must be scriptable.

Batch dimensions:

```text
dataset size
executor count
runtime
rows/sec
resource usage
```

Streaming dimensions:

```text
producer rate
consumer throughput
Kafka lag
end-to-end latency
CPU
memory
```

Do not publish an arbitrary "supports X events/sec" number unless it was measured.

---

# 30. Fault-Recovery Rules

Where relevant, stateful components must be tested for restart behavior.

Examples:

```text
kill Spark executor
restart streaming job
restart serving pod
restart producer
```

A fault test is valid only if:

1. workload is active;
2. a failure is introduced;
3. recovery is observed;
4. the resulting data behavior is checked.

"Kubernetes restarted the pod" alone is not enough.

---

# 31. Git Rules

## Keep commits meaningful

Good:

```text
feat(stream): add event-time deduplication for hospital events
feat(batch): implement encounter bronze-to-silver transform
infra(k8s): deploy Kafka topics with Strimzi
test(e2e): verify admission updates realtime hospital state
```

Bad:

```text
update
fix stuff
final
aaa
test
```

---

## Do not bundle unrelated work

Avoid one PR containing:

```text
Spark refactor
+
Grafana dashboard
+
Kubernetes rewrite
+
README rewrite
```

unless all are necessary for one narrowly defined integration change.

---

## Authorship — never commit or push under an AI identity

**Mandatory rule.** Every commit, push, tag, and PR must be authored **only** by the human teammate's own Git identity — the `user.name` / `user.email` already configured in that teammate's `git config`.

Agents **MUST NOT**:

- commit or push as Claude, Codex, Copilot, Gemini, Cursor, or any other AI/bot identity;
- change `git config user.name` / `user.email` to an AI name or bot address;
- pass `--author` with an AI identity;
- add `Co-Authored-By:` (or similar) trailers naming an AI tool;
- add "Generated with <AI tool>" footers to commit messages or PR descriptions.

Before committing, verify:

```bash
git config user.name
git config user.email
```

If no human identity is configured, **stop and ask the teammate** — do not invent one.

Reason: `works.md` evaluates each teammate's contribution from Git history. Commits must be attributable to a real team member.

---

# 32. PR Contract

Every substantial PR should state:

```text
What changed?
Why is it needed?
How do I run it?
How is it tested?
What output should I see?
Which milestone/week does it satisfy?
```

If there is a schema change, state downstream impact.

If there is a migration, provide the migration path.

---

# 33. Review Rules

Reviewers should verify behavior rather than only style.

Check:

- architecture compatibility;
- input/output correctness;
- schema compatibility;
- idempotency where needed;
- failure handling;
- tests;
- configuration;
- resource use;
- unnecessary technology additions.

Reject technically impressive changes that violate the agreed project scope.

---

# 34. No Silent Breaking Changes

Before changing:

```text
table names
column names
event types
Kafka topics
config names
service names
ports
Gold metric definitions
```

search the entire repository for consumers.

Update all affected code in the same integration change where possible.

Do not casually rename shared contracts.

---

# 35. Error Handling

Expected bad data should not kill the platform.

Examples:

- malformed event → DLQ/quarantine;
- invalid vital measurement → quarantine/invalid metric;
- temporary ClickHouse write failure → retry or explicit failure path;
- missing optional field → defined behavior.

Unexpected programming/configuration errors should fail visibly.

Avoid broad:

```python
except Exception:
    pass
```

---

# 36. Idempotency

Batch jobs must be safely rerunnable where specified.

Streaming events must use stable IDs for deduplication.

Never "solve" duplicate data by manually deleting output directories during normal operation.

Reset scripts may exist for development but are not a substitute for correct processing semantics.

---

# 37. Security and Privacy

This is an academic project using synthetic/de-identified hospital-like data.

Rules:

- no real patient PII;
- no credentials in Git;
- no production medical claims;
- no diagnosis recommendations;
- clearly identify synthetic/de-identified data when presenting.

Do not create realistic personal identities beyond what is necessary for synthetic testing.

---

# 38. Documentation Is Generated From Reality

Documentation must describe the implementation that actually exists.

Do not write diagrams or README claims for unimplemented features.

Example:

Bad:

```text
"The system automatically scales based on Kafka lag."
```

when only manual scaling exists.

Good:

```text
"The current demo scales Spark executors manually from 2 to 4 while
observing Kafka lag. Automatic lag-based scaling is an optional extension."
```

---

# 39. No Fake Big Data Claims

Do not claim:

- petabyte-scale operation;
- production-grade hospital readiness;
- exactly-once guarantees;
- high availability;
- arbitrary event throughput;

unless the system actually implements/tests those properties.

It is acceptable to say:

```text
architecture is designed to scale horizontally
```

and then provide measured experiment results.

---

# 40. Avoid Premature AI/ML

Do not add a model just because this is hospital data.

If an optional ML feature is later added, the correct architecture is:

```text
Lakehouse Silver/Gold
       ↓
ML consumer
```

not:

```text
ML model at the center of the architecture
```

The Big Data pipeline must remain the main product.

---

# 41. Integration Gate Discipline

At the end of each week, follow the integration gate in `works.md`.

Examples:

```text
Week 1:
Spark → Iceberg → Trino

Week 3:
Bronze → Silver → Gold → SQL

Week 5:
Event → Kafka → Spark → ClickHouse → Grafana

Week 6:
Full core flow on Kubernetes

Week 7:
Scale + failure recovery

Week 8:
Benchmark + E2E

Week 9:
Fresh deploy → complete demo
```

Do not call a week "done" if individual PRs work but the weekly integration gate fails.

---

# 42. Dependency Awareness

Before changing a component, identify who depends on it.

Typical dependencies:

```text
generator
→ batch ingestion
→ Bronze

Bronze
→ Silver jobs

Silver
→ Gold jobs

Gold
→ Trino/Grafana

event generator
→ Kafka

Kafka
→ Structured Streaming

Structured Streaming
→ Iceberg / ClickHouse

ClickHouse
→ Grafana

K8s
→ all deployed services
```

Your output is another teammate's input.

Treat contracts accordingly.

---

# 43. Cross-Team Compatibility

Agents working for different teammates must not create incompatible local conventions.

Shared code/config belongs in common modules when appropriate.

Examples:

```text
spark/common/config.py
spark/common/schemas.py
common event schema
central environment names
shared Makefile targets
```

Avoid:

```text
TV1_CONFIG
TV2_CONFIG
my_local_topic
temporary-final-table-v2
```

---

# 44. Naming Conventions

Prefer clear names.

Examples:

```text
bronze.encounters_raw
silver.encounters
gold.department_daily_metrics

hospital.encounter
hospital.lab
hospital.vitals

realtime_hospital_state
```

Avoid overly clever abbreviations.

Use one canonical spelling per concept.

---

# 45. Time Semantics

Healthcare event processing is time-sensitive.

Distinguish:

```text
event_time
ingestion_time
processing_time
```

Use `event_time` for streaming windows where appropriate.

Use UTC internally where feasible.

Do not mix naive timestamps with timezone-aware timestamps without an explicit normalization rule.

---

# 46. Partitioning Rules

Partition only based on actual access patterns.

Starting recommendations are in `plan.md`.

Never create extremely high-cardinality partitions like:

```text
patient_id
event_id
```

for large analytical tables.

After implementation, inspect:

```text
number of files
average file size
partition count
query behavior
```

---

# 47. Small-File Awareness

Streaming into a Lakehouse can create small files.

Do not ignore this completely.

By Week 8, the system should at least:

- measure small-file behavior;
- include a compaction/maintenance experiment or job if needed;
- document measured effect.

Do not implement a huge optimization subsystem in Week 2.

---

# 48. Resource Awareness

Local teammate machines may differ.

Make scale configurable.

Do not require:

```text
32 cores
64 GB RAM
large GPU
```

for normal development.

Prefer:

```text
small local profile
medium demo profile
benchmark profile
```

Example:

```text
DEV_PATIENTS=10000
DEMO_PATIENTS=100000
BENCH_PATIENTS=1000000
```

---

# 49. Makefile / Command Interface

Prefer stable top-level commands.

Expected examples from `plan.md`:

```bash
make setup
make up
make down
make reset
make generate-data
make batch
make stream
make query
make test
make benchmark
make k8s-up
make k8s-down
make demo
```

Agents should improve these rather than creating one-off undocumented command sequences.

---

# 50. Reproducibility

A fresh teammate should be able to:

```text
clone repo
copy .env.example
start required services
generate data
run pipeline
query result
```

without inheriting hidden local state.

Avoid relying on:

- manually created Kafka topics;
- manually created Grafana dashboards;
- local files outside repository;
- manually edited containers;
- shell history;
- machine-specific absolute paths.

---

# 51. Agent Behavior When Blocked

If blocked:

1. identify the failing dependency;
2. reproduce the issue;
3. inspect logs;
4. reduce to the smallest failing case;
5. determine whether it is your module or an integration issue;
6. fix the root cause where feasible.

Do not hide broken dependencies with mocks in the final integration path.

Mocks are acceptable in unit tests.

---

# 52. Agent Behavior When Requirements Are Ambiguous

First consult:

```text
AGENTS.md
works.md
plan.md
existing schemas/code/tests
```

Then choose the simplest implementation consistent with all four.

Do not broaden scope without reason.

If two interpretations materially affect architecture, stop and ask the human/team rather than committing a large speculative implementation.

---

# 53. Agent Behavior When Existing Code Is Bad

Do not rewrite the entire subsystem automatically.

Prefer:

```text
small refactor
+ tests
+ compatibility
```

A rewrite is justified only when:

- the current implementation blocks the milestone;
- the interface is fundamentally incorrect;
- the rewrite can be bounded and validated.

Avoid "clean architecture" rewrites during Weeks 7–9.

---

# 54. Week 9 Freeze Rule

During Week 9:

- no new large feature;
- no new database;
- no new processing engine;
- no broad framework migration;
- no speculative refactor.

Focus on:

```text
bugs
recovery
reproducibility
performance issues
integration
demo stability
```

The goal is a stable final system.

---

# 55. Final Demo Contract

The completed system should support this story:

### 1. Show infrastructure

```bash
kubectl get pods -n hospital-lake
```

### 2. Show Lakehouse

Query Bronze/Silver/Gold using Trino.

### 3. Show batch analytics

Run historical query.

### 4. Start live workload

```bash
python producer.py --rate 1000
```

or the repository equivalent.

### 5. Show realtime dashboard update

Admission/ICU/lab metrics change.

### 6. Increase load

Observe Kafka lag and processing metrics.

### 7. Scale Spark

Show processing improvement.

### 8. Inject failure

Kill a processing pod.

### 9. Show recovery

Pipeline resumes.

This demonstration should be possible without editing code live.

---

# 56. Final Technical Quality Checklist

Before declaring the repository ready:

## Data

- [ ] Synthetic generator works.
- [ ] Referential relationships are reasonable.
- [ ] Scale is configurable.
- [ ] Bad/late/duplicate event cases exist.

## Lakehouse

- [ ] Bronze exists.
- [ ] Silver exists.
- [ ] Gold exists.
- [ ] Iceberg is the table layer.
- [ ] MinIO persistence works.

## Batch

- [ ] Spark batch ingestion works.
- [ ] Transformations are testable.
- [ ] Re-run semantics are safe enough for demo.

## Stream

- [ ] Kafka ingestion works.
- [ ] Structured Streaming works.
- [ ] Event-time logic exists.
- [ ] Dedup exists.
- [ ] DLQ/quarantine exists.
- [ ] Realtime metrics exist.

## Serving

- [ ] Trino historical query works.
- [ ] ClickHouse realtime query works.
- [ ] Grafana dashboard works.

## Kubernetes

- [ ] Core services deploy reproducibly.
- [ ] Persistent services retain data appropriately.
- [ ] Spark job can run on K8s.
- [ ] Stream can run on K8s.

## Observability

- [ ] Prometheus runs.
- [ ] Kafka lag visible.
- [ ] Spark/processing metric visible.
- [ ] At least one resource metric visible.

## Distributed-system demonstration

- [ ] Scaling experiment works.
- [ ] Failure recovery experiment works.
- [ ] Benchmark scripts exist.
- [ ] Results are reproducible.

---

# 57. Mandatory Reminder for Every Agent

Before starting:

> **Read `plan.md`, `works.md`, and `AGENTS.md`.**

Before submitting:

> **Check the current week's acceptance criterion and integration gate in `works.md`.**

Before adding technology:

> **Check whether it is part of the agreed architecture in `plan.md`.**

Before marking work done:

> **Run the code and verify the actual output.**

The project is judged as one integrated Big Data system, not as four unrelated collections of commits.
