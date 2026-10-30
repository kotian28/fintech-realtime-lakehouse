FROM apache/airflow:2.9.3-python3.11
# dbt and Great Expectations live in isolated venvs so they never clash with Airflow's pinned deps
RUN python -m venv /home/airflow/dbt_venv && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core==1.9.1 dbt-duckdb==1.9.1 duckdb==1.1.3
RUN python -m venv /home/airflow/ge_venv && /home/airflow/ge_venv/bin/pip install --no-cache-dir \
    great-expectations==1.3.0 deltalake==0.20.1 pyarrow==17.0.0
