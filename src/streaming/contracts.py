"""Data contract + transformations for the Silver layer (pure DataFrame functions, easy to unit test)."""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType)

TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("card_country", StringType()),
    StructField("merchant_country", StringType()),
    StructField("channel", StringType()),
    StructField("event_time", StringType()),
    StructField("schema_version", IntegerType()),
])

SUPPORTED_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY"]
SUPPORTED_SCHEMA_VERSIONS = [1, 2]


def parse_payload(df: DataFrame) -> DataFrame:
    """Raw string payload -> typed columns. Bad JSON yields all-null columns (caught by the contract)."""
    return (df.withColumn("j", F.from_json("payload", TXN_SCHEMA))
              .select("*", "j.*").drop("j")
              .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")))


def _rules():
    return {
        "transaction_id_present": F.col("transaction_id").isNotNull() & (F.length("transaction_id") > 0),
        "user_id_present": F.col("user_id").isNotNull(),
        "amount_in_range": (F.col("amount") > 0) & (F.col("amount") < 1_000_000),
        "currency_supported": F.col("currency").isin(SUPPORTED_CURRENCIES),
        "country_code_format": F.col("card_country").rlike("^[A-Z]{2}$"),
        "event_time_valid": F.col("event_ts").isNotNull()
                            & (F.col("event_ts") <= F.current_timestamp() + F.expr("INTERVAL 1 MINUTE")),
        "schema_version_supported": F.col("schema_version").isin(SUPPORTED_SCHEMA_VERSIONS),
    }


def apply_contract(df: DataFrame) -> DataFrame:
    """Adds failed_rules (array<string>) and is_valid. NULL rule results count as failures."""
    checks = [F.when(~F.coalesce(expr, F.lit(False)), F.lit(name)) for name, expr in _rules().items()]
    return (df.withColumn("failed_rules", F.array_compact(F.array(*checks)))
              .withColumn("is_valid", F.size("failed_rules") == 0))


def enrich_silver(df: DataFrame) -> DataFrame:
    """Rule-based fraud scoring (0-100) on valid records."""
    score = (F.when(F.col("amount") >= 5000, 50).otherwise(0)
             + F.when(F.col("card_country") != F.col("merchant_country"), 30).otherwise(0)
             + F.when((F.col("channel") == "online") & (F.col("amount") > 2000), 20).otherwise(0)
             + F.when(F.col("merchant_category").isin("crypto", "gambling"), 20).otherwise(0))
    return (df.withColumn("fraud_score", F.least(score, F.lit(100)))
              .withColumn("is_flagged", F.col("fraud_score") >= 50)
              .withColumn("event_date", F.to_date("event_ts"))
              .withColumn("processed_at", F.current_timestamp()))


SILVER_COLS = ["transaction_id", "user_id", "card_id", "merchant_id", "merchant_category", "amount",
               "currency", "card_country", "merchant_country", "channel", "event_ts", "event_date",
               "schema_version", "fraud_score", "is_flagged", "processed_at"]
