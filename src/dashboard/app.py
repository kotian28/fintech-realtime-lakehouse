"""Live Streamlit dashboard: reads Silver/Quarantine (Delta) in near-real-time and Gold (Parquet, built by dbt)."""
import json, os
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

LAKE = os.getenv("LAKE_ROOT", "/data")
st.set_page_config(page_title="FinTech Lakehouse", layout="wide")
st.title("💳 Real-Time FinTech Transactions & Fraud Monitor")
st.caption("Kafka → Spark Structured Streaming → Delta (Bronze/Silver) → dbt/Airflow (Gold)")


def load(rel):
    path = f"{LAKE}/{rel}"
    if not os.path.isdir(f"{path}/_delta_log"):
        return None
    try:
        return DeltaTable(path).to_pandas()
    except Exception:
        return None


def bronze_count():
    path = f"{LAKE}/bronze/transactions"
    try:
        return DeltaTable(path).to_pyarrow_dataset().count_rows()
    except Exception:
        return 0


@st.fragment(run_every="5s")
def live():
    silver, quar = load("silver/transactions"), load("quarantine/transactions")
    if silver is None or silver.empty:
        st.info("Waiting for the first micro-batches (first start downloads Spark jars, ~1-2 min)...")
        return
    silver["event_ts"] = pd.to_datetime(silver["event_ts"], utc=True)
    n_quar = 0 if quar is None else len(quar)
    flagged = silver[silver["is_flagged"]]
    c = st.columns(5)
    c[0].metric("Bronze (raw events)", f"{bronze_count():,}")
    c[1].metric("Silver (valid)", f"{len(silver):,}")
    c[2].metric("Quarantined", f"{n_quar:,}")
    c[3].metric("Volume (all ccy units)", f"{silver['amount'].sum():,.0f}")
    c[4].metric("Fraud flag rate", f"{len(flagged) / len(silver):.2%}")

    left, right = st.columns(2)
    with left:
        st.subheader("Transactions per minute")
        per_min = (silver.assign(minute=silver["event_ts"].dt.floor("min"))
                   .groupby("minute").agg(txns=("transaction_id", "count"), flagged=("is_flagged", "sum")))
        st.line_chart(per_min.tail(30))
    with right:
        st.subheader("Why records were quarantined")
        if quar is not None and not quar.empty:
            st.bar_chart(quar["failed_rules"].explode().value_counts())
        else:
            st.write("No bad records yet.")

    st.subheader("🚨 Latest fraud alerts")
    st.dataframe(flagged.sort_values("event_ts", ascending=False)
                 [["event_ts", "transaction_id", "user_id", "merchant_id", "amount", "currency",
                   "merchant_country", "card_country", "fraud_score"]].head(15), use_container_width=True)


live()

st.divider()
st.subheader("🥇 Gold layer (built by Airflow + dbt)")
gold = f"{LAKE}/gold"
cols = st.columns(2)
mm = f"{gold}/merchant_metrics.parquet"
if os.path.exists(mm):
    cols[0].write("Top merchants by flagged transactions")
    cols[0].dataframe(pd.read_parquet(mm).sort_values("flagged_count", ascending=False).head(10),
                      use_container_width=True)
else:
    cols[0].info("Gold not built yet — the Airflow DAG `fintech_gold_refresh` runs every 2 minutes.")
rep = f"{gold}/ge_report.json"
if os.path.exists(rep):
    cols[1].write("Last Great Expectations validation of Silver")
    cols[1].json(json.load(open(rep)))
