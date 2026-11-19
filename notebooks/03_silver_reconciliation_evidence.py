# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver reconciliation and quality evidence (read-only)
# MAGIC Run after `02_silver_transform`. Every cell produces a screenshot for the `evidence/` folder.
# MAGIC Nothing here writes data.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "ecom_poc", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

# MAGIC %md ## 1. Source → Bronze parity (lossless load)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT i.file_name, i.data_rows AS source_data_rows, b.bronze_rows,
# MAGIC        CASE WHEN i.data_rows = b.bronze_rows THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM ops_source_inventory i
# MAGIC JOIN (SELECT 'orders.csv' AS file_name, COUNT(*) AS bronze_rows FROM bronze_orders
# MAGIC       UNION ALL
# MAGIC       SELECT 'transactions.csv', COUNT(*) FROM bronze_transactions) b USING (file_name)

# COMMAND ----------

# MAGIC %md ## 2. Bronze → Silver outcome reconciliation (two summaries)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dataset, bronze_rows, accepted_rows, quarantined_rows, duplicate_loser_rows,
# MAGIC        accepted_rows + quarantined_rows + duplicate_loser_rows AS outcome_total,
# MAGIC        difference, overlapping_rows, bronze_rows_without_outcome, outcome_rows_not_in_bronze, status
# MAGIC FROM silver_reconciliation ORDER BY dataset

# COMMAND ----------

# MAGIC %md ## 3. Row counts by rule

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dataset, rejection_reason, rows FROM silver_rule_counts ORDER BY dataset, rows DESC

# COMMAND ----------

# MAGIC %md ## 4. Sample quarantined rows (with reasons)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT primary_rejection_reason, rejection_reason, order_id, customer_id, order_date, category, quantity, unit_price, order_status, country, _bronze_row_id
# MAGIC FROM silver_orders_quarantine
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_rejection_reason ORDER BY _bronze_row_id) <= 2
# MAGIC ORDER BY primary_rejection_reason

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT primary_rejection_reason, rejection_reason, txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts, _bronze_row_id
# MAGIC FROM silver_transactions_quarantine
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_rejection_reason ORDER BY _bronze_row_id) <= 2
# MAGIC ORDER BY primary_rejection_reason

# COMMAND ----------

# MAGIC %md ## 5. Duplicates: winners vs losers (exact duplicate vs duplicated key)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT duplicate_type, COUNT(*) AS loser_rows FROM silver_orders_duplicates GROUP BY duplicate_type
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('txn_', duplicate_type), COUNT(*) FROM silver_transactions_duplicates GROUP BY duplicate_type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Key duplicates: the kept row (winner) next to the dropped row (loser)
# MAGIC SELECT d.business_key AS order_id,
# MAGIC        w.order_status AS winner_status, w.quantity AS winner_qty,
# MAGIC        d.order_status AS loser_status,  d.quantity AS loser_qty,
# MAGIC        d.duplicate_type, w._bronze_row_id AS winner_row_id, d._bronze_row_id AS loser_row_id
# MAGIC FROM silver_orders_duplicates d
# MAGIC JOIN silver_orders w ON w._bronze_row_id = d.winner_bronze_row_id
# MAGIC WHERE d.duplicate_type = 'key_duplicate'
# MAGIC ORDER BY d.business_key LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.business_key AS txn_id,
# MAGIC        w.txn_status AS winner_status, w.txn_ts AS winner_ts,
# MAGIC        d.txn_status AS loser_status, d.txn_ts AS loser_ts,
# MAGIC        d.duplicate_type
# MAGIC FROM silver_transactions_duplicates d
# MAGIC JOIN silver_transactions w ON w._bronze_row_id = d.winner_bronze_row_id
# MAGIC ORDER BY d.duplicate_type DESC, d.business_key LIMIT 10

# COMMAND ----------

# MAGIC %md ## 6. Before / after type conversion examples (Bronze string → Silver typed value)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT b.order_date AS bronze_order_date, s.order_date AS silver_order_date,
# MAGIC        b.quantity   AS bronze_quantity,   s.quantity   AS silver_quantity,
# MAGIC        b.unit_price AS bronze_unit_price, s.unit_price_usd AS silver_unit_price,
# MAGIC        CONCAT('[', b.order_status, ']') AS bronze_status_with_spaces, s.order_status AS silver_status,
# MAGIC        CONCAT('[', b.country, ']')      AS bronze_country, s.country AS silver_country
# MAGIC FROM bronze_orders b JOIN silver_orders s USING (_bronze_row_id)
# MAGIC WHERE b.order_status <> s.order_status OR b.country <> s.country OR b.category <> s.category
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What happens to strings that cannot be parsed: try_cast returns NULL (no job failure), then a rule quarantines the row
# MAGIC SELECT quantity AS raw_value, try_cast(quantity AS INT) AS try_cast_result, rejection_reason
# MAGIC FROM silver_orders_quarantine WHERE array_contains(rejection_reason, 'non_numeric_quantity') LIMIT 5

# COMMAND ----------

# MAGIC %md ## 7. Trace one record end-to-end (lineage by `_bronze_row_id`)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ids AS (SELECT _bronze_row_id FROM silver_orders_duplicates LIMIT 1)
# MAGIC SELECT 'bronze' AS layer, b.order_id, b.order_status, b._source_file, b._batch_id, b._bronze_row_id FROM bronze_orders b JOIN ids USING (_bronze_row_id)
# MAGIC UNION ALL
# MAGIC SELECT 'silver_duplicate_loser', d.order_id, d.order_status, d._source_file, d._batch_id, d._bronze_row_id FROM silver_orders_duplicates d JOIN ids USING (_bronze_row_id)
