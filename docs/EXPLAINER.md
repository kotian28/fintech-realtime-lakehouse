# Explainer: Real-Time FinTech Lakehouse

## 1. The problem
A payments company processes thousands of card transactions per second. Three things hurt:
1. **Fraud is time-sensitive.** A nightly batch job finds fraud after the money is gone.
2. **Upstream data is messy.** Malformed JSON, negative amounts, unknown currencies, a producer team silently shipping a new schema version. One bad record can corrupt dashboards or break pipelines.
3. **Analytics, ML and compliance need the same trustworthy data** but batch and streaming stacks usually duplicate logic and drift apart.

**This project:** a streaming pipeline that lands every event safely, validates it against a data contract, quarantines bad records instead of losing or propagating them, scores fraud in near-real-time, and publishes trusted aggregate tables and a live dashboard, all reproducible via code.

## 2. Architecture
```
 Producer ──► Kafka (topic: transactions, 3 partitions)
                │
                ▼
     Spark Structured Streaming  (micro-batch every 5s, foreachBatch)
        │            │               │
        ▼            ▼               ▼
     BRONZE        SILVER        QUARANTINE
   raw payload   valid, typed,   invalid rows +
   + kafka meta  deduped,        list of failed rules
   (Delta)       fraud-scored    (Delta)
                 (Delta)
                    │
        Airflow DAG (every 2 min):  wait_for_lake → Great Expectations gate
                    │                              → dbt run → dbt test
                    ▼
                 GOLD (Parquet): kpis_per_minute, merchant_metrics, fraud_alerts, dq_summary
                    │
                    ▼
        Streamlit dashboard  (reads Silver/Quarantine live + Gold)

 Terraform: same footprint in AWS (S3 layers, IAM, VPC, Glue, Athena, optional MSK)
```

### Local ↔ cloud mapping (say this to the recruiter)
| Concern | Local (this repo) | Cloud (Terraform) |
|---|---|---|
| Object storage | folder `./data` | S3 bucket per layer |
| Message bus | Kafka in Docker (KRaft) | MSK / MSK Serverless |
| SQL serving | DuckDB (dbt) + pandas | Athena over S3 + Glue catalog (or Snowflake) |
| Security | n/a | IAM least-privilege role, private encrypted buckets |

## 3. Workflow, step by step
1. **Producer** emits ~15 events/s. ~4% look like fraud; ~5% are intentionally broken (garbage JSON, missing ID, negative amount, bad currency, future timestamp, schema version 3).
2. **Kafka** buffers events durably, partitioned by `user_id`, so one user's events stay ordered.
3. **Spark** reads micro-batches. In `foreachBatch`:
   - **Bronze:** append the raw payload plus Kafka partition/offset. Nothing is dropped, so you can always replay.
   - **Contract check:** parse JSON to typed columns, evaluate 7 rules (ID present, amount range, currency set, country format, timestamp sane, supported schema version…). Null results count as failures.
   - **Silver:** valid rows get fraud scoring and are **MERGE-upserted on `transaction_id`**, so replays never duplicate.
   - **Quarantine:** invalid rows are stored with the names of the rules they broke.
4. **Airflow** every 2 min: waits for the Delta tables → runs a **Great Expectations** suite on Silver (stops the DAG if it fails, so bad data never reaches Gold) → **dbt run** builds Gold → **dbt test** asserts uniqueness, accepted values and score ranges.
5. **Streamlit** refreshes every 5 s: counts at each layer, per-minute throughput, flagged transactions, quarantine reasons, and the Gold tables.

## 4. Tech stack in detail

**Apache Kafka.** A distributed, append-only commit log. Producers write events to *topics*; topics are split into *partitions* for parallelism; consumers track *offsets* so they can replay. Decouples the payment system from analytics and absorbs bursts. Here: single-node KRaft (no ZooKeeper), 3 partitions. *Interview angle:* why partition by user_id, at-least-once delivery, consumer groups.

**PySpark Structured Streaming.** Spark's streaming engine treats a stream as an unbounded table processed in micro-batches. Gives the same DataFrame API for batch and streaming, plus checkpointing (offsets stored in `data/checkpoints`) for fault tolerance. We use `foreachBatch` to write one batch to three different sinks. *Angle:* checkpoints + idempotent MERGE give effectively-once results; watermarking and stateful aggregations would be the next step.

**Delta Lake (open table format).** Parquet files plus a transaction log (`_delta_log`) that adds ACID transactions, schema enforcement, time travel and MERGE. This is what lets one table be written by streaming and read by batch consistently (the "unify batch and streaming" claim). *Honest note:* this build uses Delta; Iceberg is an equivalent alternative (the swap is mostly config).

**Medallion architecture.** Bronze = raw and immutable (replay/audit), Silver = cleaned and conformed (trusted record-level data), Gold = business aggregates (what analysts consume). Each layer adds quality and reduces flexibility.

**Data contract (`contracts.py`).** The rules every record must satisfy, written as pure DataFrame functions so they are unit-testable. Violations are quarantined with the reason, and an unsupported `schema_version` is how schema drift gets blocked.

**Great Expectations.** Declarative data-quality framework. Expectations like "not null", "unique", "between" run against a batch and produce pass/fail. Here it is a **gate in Airflow** before Gold is built. *Honest note:* record-level contract checks in the stream are native Spark expressions (GE is batch-oriented); GE validates the resulting Silver table.

**dbt (via dbt-duckdb).** SQL transformations as version-controlled models with `ref()` dependencies, plus tests. DuckDB is an embedded analytical engine that reads Delta directly (`delta_scan`) and writes Gold as Parquet, which is a laptop stand-in for Athena/Snowflake. dbt tests = the second quality gate.

**Apache Airflow.** Workflow orchestrator; pipelines are Python DAGs. We show a sensor, task dependencies, retries with delay, SLAs with a callback, failure callbacks, `max_active_runs=1`, and a `run_date` parameter for backfills. (Dagster is a modern alternative; not implemented here.)

**Terraform.** Declarative IaC: you describe desired infrastructure, Terraform plans and applies the diff. Provides S3 per layer (versioned, encrypted, public access blocked), VPC/subnets/security group, IAM role with least privilege, Glue database, Athena workgroup, optional MSK Serverless. *Honest note:* it was written to be deployable but not executed in a live AWS account for this package; run `terraform init && terraform validate` before claiming "100% provisioned".

**Streamlit.** Python framework turning scripts into web apps. `st.fragment(run_every="5s")` gives the live-refresh feel.

**Docker Compose.** Defines all 7 services so the whole platform is one command and reproducible.

## 5. Two-minute recruiter pitch
> "I built a real-time fraud-monitoring lakehouse for card transactions. Events stream through Kafka into Spark Structured Streaming, which writes a Bronze raw layer, a validated Silver layer in Delta Lake, and a Quarantine table for records that break my data contract, so bad data is isolated instead of silently corrupting analytics. Airflow runs a Great Expectations gate and dbt models with tests to build Gold tables, and Streamlit shows live KPIs and fraud alerts. Writes are idempotent via MERGE, so replays don't create duplicates. The same architecture is defined as Terraform for AWS: S3 layers, IAM, a VPC, and Athena for serving. Everything runs locally with `docker compose up`."

## 6. Likely interview questions
* *Why Bronze if Silver exists?* Replay, audit, and recovery from contract bugs without re-pulling from source.
* *How do you avoid duplicates?* Delta MERGE on `transaction_id` + checkpointing; at-least-once from Kafka becomes effectively-once.
* *What if a producer changes the schema?* `schema_version` rule quarantines unknown versions; Delta schema enforcement rejects mismatched writes.
* *Why not just batch?* Fraud detection latency; streaming gives seconds, not hours.
* *Scaling?* Raise Kafka partitions, run Spark on a cluster (EMR/Databricks) instead of `local[2]`, compact small Delta files (`OPTIMIZE`), partition Silver by `event_date`.
* *Limitations?* Fraud scoring is rule-based; no stateful velocity features (needs watermarks/state); single-node Kafka; Airflow on SQLite.

## 7. Extending it
Add velocity features with windowed aggregations; swap rules for an ML model; add Prometheus/Grafana; replace DuckDB with Athena DDL such as
`CREATE EXTERNAL TABLE gold.merchant_metrics (...) STORED AS PARQUET LOCATION 's3://<gold-bucket>/'`.
