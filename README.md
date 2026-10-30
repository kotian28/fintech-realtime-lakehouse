# Real-Time FinTech Lakehouse (Kafka · Spark · Delta · dbt · Great Expectations · Airflow · Terraform · Streamlit)

Runs 100% on your laptop with Docker. See `docs/EXPLAINER.md` for architecture, workflow, tech-stack deep dive and the recruiter pitch.

## Run it
Prereqs: Docker Desktop (give it **6+ GB RAM**), internet on first start (images + Spark/Delta jars + DuckDB delta extension).

```bash
cd fintech-lakehouse
docker compose up --build -d          # first build: ~5-8 min
docker compose logs -f spark-streaming # watch micro-batches (Ctrl+C to leave logs)
```

| What | Where |
|---|---|
| Live dashboard (Streamlit) | http://localhost:8501 |
| Airflow UI (admin / admin) | http://localhost:8080 |

Give it ~2 minutes: dashboard KPIs appear first (streaming), Gold tables appear after the first successful Airflow run (DAG `fintech_gold_refresh`, every 2 min).

## Useful commands
```bash
docker compose ps                                   # all services healthy/running?
docker compose logs producer | tail                 # event generator
docker compose exec airflow airflow dags trigger fintech_gold_refresh --conf '{"run_date":"2026-10-29"}'   # parametrised rebuild/backfill
docker compose exec airflow airflow dags backfill fintech_gold_refresh -s 2026-10-28 -e 2026-10-28
docker compose down                                 # stop
docker compose down -v && rm -rf data/*             # full reset (wipes lake + checkpoints)
```

## Layout
```
docker-compose.yml        orchestrates all services
src/producer              Kafka event generator (with deliberately bad records)
src/streaming             Spark job: Bronze / Silver / Quarantine + data contract
src/quality               Great Expectations gate (runs inside Airflow)
airflow/dags              DAG: sensor -> GE gate -> dbt run -> dbt test
dbt/                      Gold models + tests (DuckDB reading Delta)
src/dashboard             Streamlit app
terraform/                AWS version of the infra (S3, IAM, VPC, Glue, Athena, optional MSK)
data/                     the lakehouse on your disk (bronze/silver/quarantine/gold/checkpoints)
```

## Troubleshooting
* Spark container restarting: check `docker compose logs spark-streaming`; first start downloads jars, give it 2 min. If memory errors, raise Docker RAM.
* `dbt_run` fails with extension error: DuckDB downloads its `delta` extension on first run; needs internet.
* Port busy: change `8501` / `8080` in `docker-compose.yml`.
* Apple Silicon: images are multi-arch; the Spark Dockerfile handles the arm64 Java path.

## Verification status
- Local stack (Kafka, Spark, Delta Lake, Airflow, dbt, Great Expectations, Streamlit): verified end to end with `src/validate.py` (13/13 checks) and a crash-recovery test.
- Terraform (AWS): `terraform validate` passes. Not applied to a live AWS account.
- Demo credentials only: Airflow admin / admin.
