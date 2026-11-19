# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: business-ready models
# MAGIC
# MAGIC ### Revenue definition (single source of truth = `silver_transactions`)
# MAGIC **Net revenue (USD)** = Σ `amount_usd` of accepted transactions with `txn_status = 'SUCCESS'`
# MAGIC − Σ `amount_usd` of accepted transactions with `txn_status = 'REFUNDED'`.
# MAGIC * `FAILED` and `PENDING` transactions are never revenue. A failed attempt followed by a successful retry counts **once** (only the SUCCESS row).
# MAGIC * **Partial / split payments:** every SUCCESS row counts, so two half-payments add up to the order total.
# MAGIC * **Repeat payment rows** for the same `txn_id` are removed in Silver (deduplication), so they can't be counted twice.
# MAGIC * **Refunds / cancellations:** REFUNDED rows are subtracted. A cancelled order with no SUCCESS payment contributes 0.
# MAGIC * **Currency:** every amount is converted to USD with the static `ref_fx_rates` table (`amount_usd` in Silver). All Gold money columns are USD.
# MAGIC * **Date attribution:** revenue is reported on the **order date** (`silver_orders.order_date`), including refunds.
# MAGIC * Only transactions whose order was accepted in `silver_orders` are included (enforced in Silver).
# MAGIC
# MAGIC **Fan-out protection:** transactions are first aggregated to **one row per order** (`gold_order_revenue`) and only then
# MAGIC joined 1:1 to orders. Joining raw transactions to orders and summing would multiply values.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "ecom_poc", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

# MAGIC %md ## 0. Order-level revenue (helper Gold table; grain = 1 row per accepted order)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_order_revenue
# MAGIC COMMENT 'Gold helper. Grain: one row per accepted order (key: order_id). Transactions are aggregated per order BEFORE joining, so there is no fan-out. Money in USD. Net revenue = SUCCESS - REFUNDED.'
# MAGIC AS
# MAGIC WITH pay AS (
# MAGIC   SELECT order_id,
# MAGIC          SUM(CASE WHEN txn_status = 'SUCCESS'  THEN amount_usd ELSE 0 END) AS gross_revenue_usd,
# MAGIC          SUM(CASE WHEN txn_status = 'REFUNDED' THEN amount_usd ELSE 0 END) AS refunds_usd,
# MAGIC          COUNT_IF(txn_status = 'SUCCESS') AS successful_payments,
# MAGIC          COUNT_IF(txn_status = 'FAILED')  AS failed_payments
# MAGIC   FROM silver_transactions
# MAGIC   GROUP BY order_id
# MAGIC )
# MAGIC SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, o.order_status, o.quantity,
# MAGIC        o.order_amount_usd,
# MAGIC        CAST(COALESCE(p.gross_revenue_usd, 0) AS DECIMAL(18,2)) AS gross_revenue_usd,
# MAGIC        CAST(COALESCE(p.refunds_usd, 0) AS DECIMAL(18,2))       AS refunds_usd,
# MAGIC        CAST(COALESCE(p.gross_revenue_usd, 0) - COALESCE(p.refunds_usd, 0) AS DECIMAL(18,2)) AS net_revenue_usd,
# MAGIC        COALESCE(p.successful_payments, 0) AS successful_payments,
# MAGIC        COALESCE(p.failed_payments, 0)     AS failed_payments,
# MAGIC        o.order_status = 'cancelled' AS is_cancelled,
# MAGIC        o.order_status = 'returned'  AS is_returned
# MAGIC FROM silver_orders o
# MAGIC LEFT JOIN pay p ON p.order_id = o.order_id

# COMMAND ----------

# MAGIC %md ## 1. `gold_daily_sales`: revenue and cancellation rate per day

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_sales
# MAGIC COMMENT 'Gold. Grain: one row per order_date (key: order_date). Daily orders, cancellations and net revenue in USD. Cancellation rate = cancelled_orders / orders. Revenue definition: see README.'
# MAGIC AS
# MAGIC SELECT order_date,
# MAGIC        COUNT(*)                                   AS orders,
# MAGIC        COUNT_IF(is_cancelled)                     AS cancelled_orders,
# MAGIC        COUNT_IF(is_returned)                      AS returned_orders,
# MAGIC        CAST(COUNT_IF(is_cancelled) / COUNT(*) AS DECIMAL(9,6)) AS cancellation_rate,
# MAGIC        COUNT_IF(gross_revenue_usd > 0)            AS paid_orders,
# MAGIC        SUM(quantity)                              AS units_ordered,
# MAGIC        CAST(SUM(gross_revenue_usd) AS DECIMAL(18,2)) AS gross_revenue_usd,
# MAGIC        CAST(SUM(refunds_usd) AS DECIMAL(18,2))       AS refunds_usd,
# MAGIC        CAST(SUM(net_revenue_usd) AS DECIMAL(18,2))   AS net_revenue_usd
# MAGIC FROM gold_order_revenue
# MAGIC GROUP BY order_date

# COMMAND ----------

# MAGIC %md ## 2. `gold_revenue_breakdown`: revenue by category and country

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_revenue_breakdown
# MAGIC COMMENT 'Gold. Grain: one row per (order_date, category, country) (composite key). Net revenue in USD. Keep order_date so BI tools can filter by the same date range as gold_daily_sales.'
# MAGIC AS
# MAGIC SELECT order_date, category, country,
# MAGIC        COUNT(*)                                      AS orders,
# MAGIC        COUNT_IF(is_cancelled)                        AS cancelled_orders,
# MAGIC        COUNT_IF(gross_revenue_usd > 0)               AS paid_orders,
# MAGIC        SUM(quantity)                                 AS units_ordered,
# MAGIC        CAST(SUM(gross_revenue_usd) AS DECIMAL(18,2)) AS gross_revenue_usd,
# MAGIC        CAST(SUM(refunds_usd) AS DECIMAL(18,2))       AS refunds_usd,
# MAGIC        CAST(SUM(net_revenue_usd) AS DECIMAL(18,2))   AS net_revenue_usd
# MAGIC FROM gold_order_revenue
# MAGIC GROUP BY order_date, category, country

# COMMAND ----------

# MAGIC %md ## 3. `gold_payment_success`: success rate by payment method

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_payment_success
# MAGIC COMMENT 'Gold. Grain: one row per (txn_date, payment_method) (composite key). Payment attempts = SUCCESS + FAILED transactions (PENDING and REFUNDED are not attempts). success_rate = successful_attempts / total_attempts. Re-aggregate with SUM(successful)/SUM(total), never by averaging rates.'
# MAGIC AS
# MAGIC SELECT txn_date, payment_method,
# MAGIC        COUNT_IF(txn_status IN ('SUCCESS','FAILED')) AS total_attempts,
# MAGIC        COUNT_IF(txn_status = 'SUCCESS')             AS successful_attempts,
# MAGIC        COUNT_IF(txn_status = 'FAILED')              AS failed_attempts,
# MAGIC        COUNT_IF(txn_status = 'PENDING')             AS pending_transactions,
# MAGIC        CAST(COUNT_IF(txn_status = 'SUCCESS') / NULLIF(COUNT_IF(txn_status IN ('SUCCESS','FAILED')), 0) AS DECIMAL(9,6)) AS success_rate,
# MAGIC        CAST(SUM(CASE WHEN txn_status = 'SUCCESS' THEN amount_usd ELSE 0 END) AS DECIMAL(18,2)) AS successful_amount_usd
# MAGIC FROM silver_transactions
# MAGIC GROUP BY txn_date, payment_method

# COMMAND ----------

# MAGIC %md ## 4. `gold_top_customers`: customers ranked by spend

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_top_customers
# MAGIC COMMENT 'Gold. Grain: one row per customer_id (key: customer_id), lifetime totals over all accepted orders. spend_rank 1 = highest net revenue (ties broken by customer_id). Net revenue in USD.'
# MAGIC AS
# MAGIC SELECT customer_id,
# MAGIC        COUNT(*)                                    AS orders,
# MAGIC        COUNT_IF(gross_revenue_usd > 0)             AS paid_orders,
# MAGIC        CAST(SUM(net_revenue_usd) AS DECIMAL(18,2)) AS net_revenue_usd,
# MAGIC        CAST(SUM(net_revenue_usd) / NULLIF(COUNT_IF(gross_revenue_usd > 0), 0) AS DECIMAL(18,2)) AS avg_net_revenue_per_paid_order_usd,
# MAGIC        MIN(order_date) AS first_order_date,
# MAGIC        MAX(order_date) AS last_order_date,
# MAGIC        ROW_NUMBER() OVER (ORDER BY SUM(net_revenue_usd) DESC, customer_id) AS spend_rank
# MAGIC FROM gold_order_revenue
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %md ## Column descriptions (Unity Catalog documentation)

# COMMAND ----------

COLUMN_DOCS = {
    "gold_order_revenue": {
        "order_id": "Business key; unique",
        "order_amount_usd": "Booked value = quantity x unit price (USD). NOT revenue",
        "gross_revenue_usd": "Sum of SUCCESS transaction amounts in USD",
        "refunds_usd": "Sum of REFUNDED transaction amounts in USD",
        "net_revenue_usd": "Revenue = gross_revenue_usd - refunds_usd (USD)",
    },
    "gold_daily_sales": {
        "order_date": "Grain key: calendar date the order was placed",
        "orders": "Accepted orders placed on this date",
        "cancelled_orders": "Orders with status cancelled",
        "cancellation_rate": "cancelled_orders / orders (0-1). Re-aggregate as SUM(cancelled)/SUM(orders)",
        "paid_orders": "Orders with at least one SUCCESS payment",
        "gross_revenue_usd": "SUCCESS payments in USD",
        "refunds_usd": "REFUNDED amounts in USD",
        "net_revenue_usd": "Revenue (USD) = gross - refunds; additive across days",
    },
    "gold_revenue_breakdown": {
        "order_date": "Grain key part 1: order date",
        "category": "Grain key part 2: product category",
        "country": "Grain key part 3: customer country",
        "net_revenue_usd": "Revenue (USD) = gross - refunds; additive",
    },
    "gold_payment_success": {
        "txn_date": "Grain key part 1: transaction date (UTC)",
        "payment_method": "Grain key part 2: payment method",
        "total_attempts": "SUCCESS + FAILED transactions",
        "successful_attempts": "SUCCESS transactions",
        "success_rate": "successful_attempts / total_attempts. Do not average; re-aggregate from counts",
    },
    "gold_top_customers": {
        "customer_id": "Grain key: customer",
        "net_revenue_usd": "Lifetime revenue (USD) from accepted orders",
        "spend_rank": "1 = highest net revenue",
    },
}
for tbl, cols in COLUMN_DOCS.items():
    for col, txt in cols.items():
        spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} COMMENT '{txt}'")
    grain = {"gold_order_revenue": "order_id", "gold_daily_sales": "order_date",
             "gold_revenue_breakdown": "order_date,category,country",
             "gold_payment_success": "txn_date,payment_method", "gold_top_customers": "customer_id"}[tbl]
    spark.sql(f"ALTER TABLE {tbl} SET TBLPROPERTIES ('grain' = '{grain}', 'revenue_definition' = 'SUCCESS minus REFUNDED, USD, by order_date')")

# COMMAND ----------

# MAGIC %md ## Quality assertions (fail the task if Gold is inconsistent)

# COMMAND ----------

checks = spark.sql("""
SELECT 'gold_order_revenue key unique' AS check_name, COUNT(*) = COUNT(DISTINCT order_id) AS passed FROM gold_order_revenue
UNION ALL SELECT 'gold_daily_sales key unique', COUNT(*) = COUNT(DISTINCT order_date) FROM gold_daily_sales
UNION ALL SELECT 'gold_revenue_breakdown key unique', COUNT(*) = COUNT(DISTINCT order_date, category, country) FROM gold_revenue_breakdown
UNION ALL SELECT 'gold_payment_success key unique', COUNT(*) = COUNT(DISTINCT txn_date, payment_method) FROM gold_payment_success
UNION ALL SELECT 'gold_top_customers key unique', COUNT(*) = COUNT(DISTINCT customer_id) FROM gold_top_customers
UNION ALL SELECT 'orders preserved (no fan-out)', (SELECT COUNT(*) FROM gold_order_revenue) = (SELECT COUNT(*) FROM silver_orders)
UNION ALL SELECT 'revenue: daily = breakdown', (SELECT SUM(net_revenue_usd) FROM gold_daily_sales) = (SELECT SUM(net_revenue_usd) FROM gold_revenue_breakdown)
UNION ALL SELECT 'revenue: daily = customers', (SELECT SUM(net_revenue_usd) FROM gold_daily_sales) = (SELECT SUM(net_revenue_usd) FROM gold_top_customers)
UNION ALL SELECT 'revenue: gold = silver', (SELECT SUM(net_revenue_usd) FROM gold_daily_sales) =
      (SELECT SUM(CASE WHEN txn_status='SUCCESS' THEN amount_usd WHEN txn_status='REFUNDED' THEN -amount_usd ELSE 0 END) FROM silver_transactions)
""")
display(checks)
failed = [r["check_name"] for r in checks.collect() if not r["passed"]]
if failed:
    raise Exception(f"Gold quality checks failed: {failed}")
print("All Gold checks passed.")

# COMMAND ----------

# MAGIC %md ## Evidence: Gold table list, row counts and samples

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'gold_order_revenue' AS table_name, COUNT(*) AS row_count FROM gold_order_revenue
# MAGIC UNION ALL SELECT 'gold_daily_sales', COUNT(*) FROM gold_daily_sales
# MAGIC UNION ALL SELECT 'gold_revenue_breakdown', COUNT(*) FROM gold_revenue_breakdown
# MAGIC UNION ALL SELECT 'gold_payment_success', COUNT(*) FROM gold_payment_success
# MAGIC UNION ALL SELECT 'gold_top_customers', COUNT(*) FROM gold_top_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_daily_sales ORDER BY order_date DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category, country, SUM(orders) AS orders, SUM(net_revenue_usd) AS net_revenue_usd
# MAGIC FROM gold_revenue_breakdown GROUP BY category, country ORDER BY net_revenue_usd DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT payment_method, SUM(total_attempts) AS attempts, SUM(successful_attempts) AS successes,
# MAGIC        ROUND(SUM(successful_attempts) / SUM(total_attempts), 4) AS success_rate
# MAGIC FROM gold_payment_success GROUP BY payment_method ORDER BY success_rate DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_top_customers WHERE spend_rank <= 10 ORDER BY spend_rank
