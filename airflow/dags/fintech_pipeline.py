"""Gold refresh: wait for lake -> Great Expectations gate -> dbt run -> dbt test.
Features shown: retries, SLAs + SLA-miss callback, failure callback, backfill-able run_date param."""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

LAKE = os.environ.get("LAKE_ROOT", "/opt/airflow/data")
log = logging.getLogger(__name__)


def lake_ready():
    return all(os.path.isdir(f"{LAKE}/{p}/_delta_log") for p in ("silver/transactions", "quarantine/transactions"))


def on_failure(context):
    log.error("ALERT: task %s failed in run %s", context["task_instance"].task_id, context["run_id"])


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    log.error("SLA MISSED for %s: %s", dag.dag_id, task_list)


ENV = {"LAKE_ROOT": LAKE, "DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"}
DBT = "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt {cmd} --profiles-dir /opt/airflow/dbt"
VARS = """ --vars '{"run_date": "{{ params.run_date }}"}'"""

with DAG(
    dag_id="fintech_gold_refresh",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    params={"run_date": ""},  # e.g. trigger with {"run_date": "2026-10-28"} to rebuild a single day
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(seconds=30),
                  "sla": timedelta(minutes=10), "on_failure_callback": on_failure},
    sla_miss_callback=on_sla_miss,
    tags=["fintech", "gold"],
) as dag:
    wait = PythonSensor(task_id="wait_for_lake", python_callable=lake_ready, poke_interval=15,
                        timeout=900, mode="reschedule")
    ge = BashOperator(task_id="great_expectations_gate", env=ENV, append_env=True,
                      bash_command="/home/airflow/ge_venv/bin/python /opt/airflow/quality/ge_validate.py")
    run = BashOperator(task_id="dbt_run", env=ENV, append_env=True, bash_command=DBT.format(cmd="run") + VARS)
    test = BashOperator(task_id="dbt_test", env=ENV, append_env=True, bash_command=DBT.format(cmd="test"))
    wait >> ge >> run >> test
