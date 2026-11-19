# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: schema, volume, reference data, source inventory
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Creates the `ecom_poc` schema and the managed `raw_files` volume in your Unity Catalog catalog.
# MAGIC 2. Creates a small reference table of currency rates (`ref_fx_rates`), used by Silver/Gold.
# MAGIC 3. After you upload the CSVs, records each source file's **exact data-row count (excluding header), byte size and volume path** in `ops_source_inventory`.
# MAGIC
# MAGIC Run it on **Serverless** compute. Set the `catalog` widget at the top to your catalog name (Catalog Explorer shows it, for example `workspace`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "ecom_poc", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
volume_path = f"/Volumes/{catalog}/{schema}/raw_files"
print(f"catalog={catalog}  schema={schema}  volume={volume_path}")

# COMMAND ----------

# MAGIC %md ## 1. Schema and managed volume (same as `sql/01_setup.sql`)

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}` COMMENT 'E-commerce medallion POC (Bronze/Silver/Gold) - synthetic data only'")
spark.sql(f"USE SCHEMA `{schema}`")
spark.sql("CREATE VOLUME IF NOT EXISTS raw_files COMMENT 'Landing zone: raw, unchanged source CSV extracts'")
display(spark.sql(f"DESCRIBE VOLUME `{catalog}`.`{schema}`.raw_files"))

# COMMAND ----------

# MAGIC %md ## 2. Reference data: fixed FX rates (USD per 1 unit of currency)
# MAGIC These are the same static rates the generator used (`fx_rates.csv`). Fixed rates make revenue reproducible.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE ref_fx_rates (
  currency     STRING        COMMENT 'ISO 4217 currency code (upper case)',
  usd_per_unit DECIMAL(12,6) COMMENT 'Static conversion rate: 1 unit of currency = usd_per_unit USD'
) COMMENT 'Static FX reference used to convert transaction amounts to USD. Supported currencies = rows in this table.'
""")
spark.sql("""
INSERT INTO ref_fx_rates VALUES
 ('USD',1.000000),('EUR',1.080000),('GBP',1.270000),('BDT',0.008400),('INR',0.012000),
 ('CAD',0.730000),('AUD',0.660000),('SGD',0.740000),('AED',0.272300)
""")
display(spark.table("ref_fx_rates"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upload the source files (manual UI step)
# MAGIC In the left sidebar: **Catalog → your catalog → ecom_poc → Volumes → raw_files → Upload to this volume**
# MAGIC (or **New → Add or upload data → Upload files to a volume**). Upload `orders.csv` and `transactions.csv`.
# MAGIC Then run the cells below.

# COMMAND ----------

display(dbutils.fs.ls(volume_path))

# COMMAND ----------

# MAGIC %md ## 4. Source inventory: exact data-row count, bytes and path (reproducible)
# MAGIC Row count = number of physical lines minus 1 header line. The files contain no embedded line breaks inside fields,
# MAGIC so lines = records. This is checked independently of the CSV parser used in Bronze.

# COMMAND ----------

from pyspark.sql import functions as F

rows = []
for fname in ["orders.csv", "transactions.csv"]:
    path = f"{volume_path}/{fname}"
    info = [f for f in dbutils.fs.ls(volume_path) if f.name == fname]
    if not info:
        raise FileNotFoundError(f"{fname} not found in {volume_path}. Upload it first.")
    lines = spark.read.text(path).count()
    rows.append((fname, path, int(info[0].size), int(lines), int(lines) - 1))

inventory = (spark.createDataFrame(rows, "file_name STRING, volume_path STRING, file_bytes BIGINT, physical_lines BIGINT, data_rows BIGINT")
             .withColumn("checked_at", F.current_timestamp()))
inventory.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ops_source_inventory")
spark.sql("COMMENT ON TABLE ops_source_inventory IS 'Evidence: exact source data-row counts (excluding header), bytes and paths of the raw CSVs in the volume'")
display(spark.table("ops_source_inventory"))

# COMMAND ----------

# Contract check: exactly 1,000,000 data rows per file
bad = [r for r in rows if r[4] != 1_000_000]
assert not bad, f"Dataset contract violated: {bad}"
print("Dataset contract OK: both files have exactly 1,000,000 data rows (+ header).")
