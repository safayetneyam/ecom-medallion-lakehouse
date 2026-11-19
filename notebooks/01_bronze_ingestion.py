# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: lossless ingestion
# MAGIC
# MAGIC * Each CSV is loaded into its own Delta table: `bronze_orders` and `bronze_transactions`.
# MAGIC * **Every source column is stored as STRING**, using an explicit schema (no `inferSchema`), so malformed values are preserved exactly as received.
# MAGIC * Audit columns:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `_ingested_at` | Timestamp when this load wrote the row. |
# MAGIC | `_source_file` | Full volume path of the file the row came from (`_metadata.file_path`). |
# MAGIC | `_batch_id` | Deterministic id of the load = `<file name>:<file size>:<file modification time>`. Same file means the same batch id. |
# MAGIC | `_row_hash` | SHA-256 of all source column values. Identical rows have identical hashes (used to detect exact duplicates and changes). |
# MAGIC | `_bronze_row_id` | Unique, deterministic id of each raw row = hash(`_batch_id`, `_row_hash`, occurrence number). It is carried into every Silver outcome. |
# MAGIC | `_rescued_data` | Anything the CSV reader could not map to the schema (should be NULL). |
# MAGIC
# MAGIC **Idempotency:** each load overwrites only its own batch (`replaceWhere _batch_id = ...`). Re-running the notebook on
# MAGIC the same file replaces that batch instead of appending it again, so the row count never doubles.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "ecom_poc", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
volume_path = f"/Volumes/{catalog}/{schema}/raw_files"
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

SOURCES = {
    "orders": {
        "file": "orders.csv",
        "table": "bronze_orders",
        "columns": ["order_id", "customer_id", "order_date", "category", "quantity",
                    "unit_price", "order_status", "country"],
    },
    "transactions": {
        "file": "transactions.csv",
        "table": "bronze_transactions",
        "columns": ["txn_id", "order_id", "payment_method", "amount", "currency",
                    "txn_status", "txn_ts"],
    },
}


def ingest(name: str, cfg: dict) -> dict:
    path = f"{volume_path}/{cfg['file']}"
    cols = cfg["columns"]
    info = [f for f in dbutils.fs.ls(volume_path) if f.name == cfg["file"]]
    if not info:
        raise FileNotFoundError(path)
    info = info[0]
    batch_id = f"{cfg['file']}:{info.size}:{info.modificationTime}"

    # 1) Header check: the file's header must match the contract exactly
    header = spark.read.text(path).limit(1).collect()[0][0].strip().split(",")
    if header != cols:
        raise ValueError(f"Unexpected header in {cfg['file']}: {header}")

    # 2) Read with an explicit all-STRING schema (no type inference, nothing rejected)
    string_schema = StructType([StructField(c, StringType(), True) for c in cols])
    raw = (spark.read.format("csv")
           .option("header", "true")
           .option("inferSchema", "false")
           .option("mode", "PERMISSIVE")
           .option("rescuedDataColumn", "_rescued_data")
           .schema(string_schema)
           .load(path))

    # 3) Audit metadata
    row_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in cols]), 256)
    occurrence = F.row_number().over(Window.partitionBy("_row_hash").orderBy("_row_hash"))
    df = (raw.select(*cols, "_rescued_data",
                     F.current_timestamp().alias("_ingested_at"),
                     F.col("_metadata.file_path").alias("_source_file"),
                     F.lit(batch_id).alias("_batch_id"))
          .withColumn("_row_hash", row_hash)
          .withColumn("_occurrence", occurrence)
          .withColumn("_bronze_row_id",
                      F.sha2(F.concat_ws("|", F.col("_batch_id"), F.col("_row_hash"),
                                         F.col("_occurrence").cast("string")), 256))
          .drop("_occurrence"))

    # 4) Idempotent write: create the table once, then replace only this batch
    df.limit(0).write.format("delta").mode("ignore").saveAsTable(cfg["table"])
    (df.write.format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"_batch_id = '{batch_id}'")
       .saveAsTable(cfg["table"]))

    # 5) Lossless check: source data rows (lines - header) == Bronze rows for this batch
    source_rows = spark.read.text(path).count() - 1
    bronze_batch_rows = spark.table(cfg["table"]).where(F.col("_batch_id") == batch_id).count()
    bronze_total_rows = spark.table(cfg["table"]).count()
    return {"dataset": name, "table": cfg["table"], "batch_id": batch_id, "source_file": path,
            "file_bytes": int(info.size), "source_data_rows": int(source_rows),
            "bronze_batch_rows": int(bronze_batch_rows), "bronze_total_rows": int(bronze_total_rows)}


results = [ingest(n, c) for n, c in SOURCES.items()]

# COMMAND ----------

# MAGIC %md ## Load log (one row appended per run: shows that re-runs do not double the data)

# COMMAND ----------

log = (spark.createDataFrame(results)
       .withColumn("lossless", F.col("source_data_rows") == F.col("bronze_batch_rows"))
       .withColumn("loaded_at", F.current_timestamp()))
log.write.mode("append").option("mergeSchema", "true").saveAsTable("ops_bronze_load_log")
display(spark.table("ops_bronze_load_log").orderBy(F.desc("loaded_at")))

# COMMAND ----------

for r in results:
    assert r["source_data_rows"] == r["bronze_batch_rows"], f"Row count mismatch: {r}"
    assert r["bronze_total_rows"] == r["bronze_batch_rows"], (
        f"{r['table']} holds more than one batch ({r['bronze_total_rows']} rows). "
        "A different file version was loaded earlier. Review ops_bronze_load_log.")
    print(f"OK {r['table']}: source={r['source_data_rows']:,} bronze={r['bronze_batch_rows']:,}")

# Every Bronze row id must be unique (needed for exact Silver reconciliation)
for t in ["bronze_orders", "bronze_transactions"]:
    n, d = spark.table(t).select(F.count("*"), F.countDistinct("_bronze_row_id")).first()
    assert n == d, f"{t}: _bronze_row_id not unique"
    assert spark.table(t).where("_rescued_data IS NOT NULL").count() == 0, f"{t}: rescued data present"

# COMMAND ----------

# MAGIC %md ## Table comments (documentation in Unity Catalog)

# COMMAND ----------

spark.sql("COMMENT ON TABLE bronze_orders IS 'Bronze: raw orders.csv rows, all source columns as STRING, plus audit columns. One row per source data row. Never cleaned.'")
spark.sql("COMMENT ON TABLE bronze_transactions IS 'Bronze: raw transactions.csv rows, all source columns as STRING, plus audit columns. One row per source data row. Never cleaned.'")
for t in ["bronze_orders", "bronze_transactions"]:
    for c, txt in {"_ingested_at": "Load timestamp of this batch",
                   "_source_file": "Volume path of the source file",
                   "_batch_id": "Deterministic batch key: file name:size:modification time",
                   "_row_hash": "SHA-256 of all source values (exact-duplicate detection)",
                   "_bronze_row_id": "Unique deterministic raw-row id, preserved in every Silver outcome",
                   "_rescued_data": "Values the CSV reader could not map to the schema (expected NULL)"}.items():
        spark.sql(f"ALTER TABLE {t} ALTER COLUMN {c} COMMENT '{txt}'")

# COMMAND ----------

# MAGIC %md ## Evidence: schema and sample rows

# COMMAND ----------

display(spark.sql("DESCRIBE TABLE bronze_orders"))

# COMMAND ----------

display(spark.table("bronze_orders").limit(10))

# COMMAND ----------

display(spark.table("bronze_transactions").limit(10))
