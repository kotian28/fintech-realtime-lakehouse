"""Kafka -> Bronze / Silver / Quarantine (Delta Lake) with Spark Structured Streaming."""
import os
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from contracts import SILVER_COLS, apply_contract, enrich_silver, parse_payload

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "transactions")
LAKE = os.getenv("LAKE_ROOT", "/data")
BRONZE, SILVER, QUAR = (f"{LAKE}/bronze/transactions", f"{LAKE}/silver/transactions",
                        f"{LAKE}/quarantine/transactions")
CKPT = f"{LAKE}/checkpoints/stream_main"

builder = (SparkSession.builder.appName("fintech-medallion").master("local[2]")
           .config("spark.driver.memory", "1g")
           .config("spark.sql.shuffle.partitions", "4")
           .config("spark.sql.session.timeZone", "UTC")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def upsert(df, path, keys):
    """Idempotent write: a replayed micro-batch cannot create duplicates (used for ALL three sinks)."""
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    if DeltaTable.isDeltaTable(spark, path):
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(df.alias("s"), cond).whenNotMatchedInsertAll().execute())
    else:
        df.write.format("delta").mode("append").save(path)


def process_batch(batch_df, batch_id):
    batch_df.persist()
    try:
        if batch_df.isEmpty():
            return
        # ---- BRONZE: immutable raw copy, nothing is ever dropped -------------------------------
        bronze = batch_df.select(
            F.col("topic"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"), F.col("value").cast("string").alias("payload"),
            F.lit(batch_id).alias("batch_id"), F.current_timestamp().alias("ingested_at"))
        upsert(bronze, BRONZE, ["kafka_partition", "kafka_offset"])

        checked = apply_contract(parse_payload(bronze)).persist()
        # ---- SILVER: valid, typed, deduplicated, enriched --------------------------------------
        good = (enrich_silver(checked.filter("is_valid")).select(*SILVER_COLS)
                .dropDuplicates(["transaction_id"]))
        upsert(good, SILVER, ["transaction_id"])
        # ---- QUARANTINE: invalid rows + the reasons they failed ---------------------------------
        bad = (checked.filter(~F.col("is_valid"))
               .select("payload", "failed_rules", "kafka_partition", "kafka_offset", "batch_id",
                       F.current_timestamp().alias("quarantined_at")))
        if not bad.isEmpty():
            upsert(bad, QUAR, ["kafka_partition", "kafka_offset"])
        checked.unpersist()
    finally:
        batch_df.unpersist()


if __name__ == "__main__":
    (spark.readStream.format("kafka")
     .option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC)
     .option("startingOffsets", "earliest").option("failOnDataLoss", "false")
     .option("maxOffsetsPerTrigger", 5000).load()
     .writeStream.foreachBatch(process_batch)
     .option("checkpointLocation", CKPT).trigger(processingTime="5 seconds")
     .start().awaitTermination())