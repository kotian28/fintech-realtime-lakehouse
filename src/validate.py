"""End-to-end validation. Run:  docker compose exec dashboard python /app/src/validate.py"""
import json, os, sys, time
import pandas as pd
from deltalake import DeltaTable

LAKE = os.getenv("LAKE_ROOT", "/data")
results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  ->  {detail}" if detail else ""))


def count(layer):
    return DeltaTable(f"{LAKE}/{layer}/transactions").to_pyarrow_dataset().count_rows()


try:
    b1 = count("bronze")
    silver = DeltaTable(f"{LAKE}/silver/transactions").to_pandas()
    quar = DeltaTable(f"{LAKE}/quarantine/transactions").to_pandas()
except Exception as e:
    print(f"[FAIL] Delta tables readable  ->  {e}")
    sys.exit(1)

s, q = len(silver), len(quar)
check("Bronze, Silver, Quarantine tables exist and have data", min(b1, s, q) > 0, f"bronze={b1} silver={s} quarantine={q}")
check("Bronze ~= Silver + Quarantine (within 2%, allows in-flight batch)",
      abs(b1 - (s + q)) <= max(300, 0.02 * b1), f"diff={b1 - (s + q)}")
check("Quarantine share is 2-10% (expected ~5%)", 0.02 <= q / b1 <= 0.10, f"{q / b1:.2%}")
check("Silver transaction_id is unique (no duplicates)", silver["transaction_id"].is_unique)
check("Fraud flag rate is 1-10% (expected ~3-5%)", 0.01 <= silver["is_flagged"].mean() <= 0.10,
      f"{silver['is_flagged'].mean():.2%}")
check("Silver amounts > 0 and currencies valid",
      (silver["amount"] > 0).all() and silver["currency"].isin(["USD", "EUR", "GBP", "INR", "JPY"]).all())
check("Fraud score within 0-100", silver["fraud_score"].between(0, 100).all())
check("All flagged rows have score >= 50", (silver.loc[silver["is_flagged"], "fraud_score"] >= 50).all())
rules = quar["failed_rules"].explode().value_counts()
check("Quarantine shows 4+ distinct failure reasons", len(rules) >= 4, ", ".join(f"{k}:{v}" for k, v in rules.items()))

age = (pd.Timestamp.now(tz="UTC") - pd.to_datetime(silver["processed_at"], utc=True).max()).total_seconds()
check("Silver is fresh (latest record processed < 2 min ago)", age < 120, f"{age:.0f}s ago")
print("... waiting 15s to confirm data is still flowing")
time.sleep(15)
b2 = count("bronze")
check("Bronze is still growing (stream is live)", b2 > b1, f"+{b2 - b1} rows in ~15s")

gold = [f"{LAKE}/gold/{n}.parquet" for n in ("kpis_per_minute", "merchant_metrics", "fraud_alerts", "dq_summary")]
missing = [os.path.basename(p) for p in gold if not os.path.exists(p)]
check("All 4 Gold parquet files exist (Airflow + dbt ran)", not missing, f"missing: {missing}" if missing else "")
rep = f"{LAKE}/gold/ge_report.json"
if os.path.exists(rep):
    r = json.load(open(rep))
    check("Great Expectations report passed", r.get("passed") is True, f"checked_at {r.get('checked_at')}")
else:
    check("Great Expectations report exists", False, "ge_report.json not found")

print(f"\n{sum(results)}/{len(results)} checks passed")
print("RESULT: PROJECT FULLY FUNCTIONAL" if all(results) else "RESULT: SOME CHECKS FAILED, send me the FAIL lines")
sys.exit(0 if all(results) else 1)