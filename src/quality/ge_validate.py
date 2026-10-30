"""Great Expectations gate on the Silver table. Exit code != 0 stops the Airflow DAG before Gold is built."""
import json, os, sys, time
import great_expectations as gx
from deltalake import DeltaTable

LAKE = os.getenv("LAKE_ROOT", "/opt/airflow/data")
df = DeltaTable(f"{LAKE}/silver/transactions").to_pandas()

ctx = gx.get_context(mode="ephemeral")
asset = ctx.data_sources.add_pandas("silver").add_dataframe_asset("transactions")
batch = asset.add_batch_definition_whole_dataframe("whole").get_batch(batch_parameters={"dataframe": df})

E = gx.expectations
suite = {
    "transaction_id not null": E.ExpectColumnValuesToNotBeNull(column="transaction_id"),
    "transaction_id unique": E.ExpectColumnValuesToBeUnique(column="transaction_id"),
    "amount in (0, 1e6)": E.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01, max_value=999999),
    "currency in set": E.ExpectColumnValuesToBeInSet(column="currency", value_set=["USD", "EUR", "GBP", "INR", "JPY"]),
    "fraud_score 0-100": E.ExpectColumnValuesToBeBetween(column="fraud_score", min_value=0, max_value=100),
}
results = {name: bool(batch.validate(exp).success) for name, exp in suite.items()}
report = {"checked_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), "rows": int(len(df)),
          "results": results, "passed": all(results.values())}
with open(f"{LAKE}/gold/ge_report.json", "w") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))
sys.exit(0 if report["passed"] else 1)
