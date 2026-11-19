# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: cleaning, validation, deduplication and reconciliation
# MAGIC
# MAGIC **Processing precedence (each Bronze row ends in exactly one bucket):**
# MAGIC 1. **Standardise:** trim whitespace, turn null-like tokens (`''`, `NULL`, `None`, `N/A`) into real NULLs, normalise case to the canonical value (`  DELIVERED ` → `delivered`).
# MAGIC 2. **Type safely:** `try_cast` / `try_to_timestamp`. A string that cannot be parsed becomes NULL instead of failing the job, and the rule matrix then flags it.
# MAGIC 3. **Validate:** apply the business-rule matrix. A row failing **one or more** rules → **quarantine** table, with every failed rule listed in `rejection_reason`.
# MAGIC 4. **Deduplicate (valid rows only):** one winner per business key (`order_id` / `txn_id`), chosen by a stable keep-one rule. Other rows → **duplicate losers** table, marked `exact_duplicate` or `key_duplicate`.
# MAGIC 5. **Reconcile:** prove `Bronze = accepted + quarantined + duplicate losers` with no overlap. If it fails, the notebook raises an error, the job stops and Gold is not built.
# MAGIC
# MAGIC The Silver tables are fully rebuilt from Bronze on every run (`CREATE OR REPLACE`), so re-runs are idempotent.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "ecom_poc", "Schema")
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Deterministic parsing: strict modern date parser (impossible dates such as 2026-02-30 -> NULL, never an
# "upgrade" exception) and UTC session time zone. Both are supported settings on serverless compute.
for k, v in {"spark.sql.legacy.timeParserPolicy": "CORRECTED", "spark.sql.session.timeZone": "UTC"}.items():
    try:
        spark.conf.set(k, v)
    except Exception as e:  # if a setting is not allowed, the regex pre-checks below still protect parsing
        print(f"Could not set {k}: {e}")

# ---------------- Reference values (the business-rule matrix lives in sql/02_business_rules_matrix.md) ----------------
NULL_TOKENS = ["", "null", "none", "n/a"]
ALLOWED_CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Beauty", "Sports", "Books", "Toys", "Grocery"]
ALLOWED_COUNTRIES = ["Bangladesh", "India", "United States", "United Kingdom", "Germany", "France", "Canada",
                     "Australia", "Singapore", "United Arab Emirates"]
ALLOWED_ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
ALLOWED_PAYMENT_METHODS = ["credit_card", "debit_card", "mobile_wallet", "paypal", "bank_transfer", "cash_on_delivery"]
ALLOWED_TXN_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
SUPPORTED_CURRENCIES = [r["currency"] for r in spark.table("ref_fx_rates").select("currency").collect()]
QTY_MIN, QTY_MAX = 1, 50
PRICE_MIN, PRICE_MAX = 0.50, 10000.00
MAX_TXN_AMOUNT_USD = 500000.00
# Keep-one rule for duplicate order_id: most advanced lifecycle status wins
ORDER_STATUS_RANK = {"pending": 1, "processing": 2, "shipped": 3, "delivered": 4, "cancelled": 5, "returned": 6}

AUDIT_COLS = ["_bronze_row_id", "_batch_id", "_row_hash", "_source_file", "_ingested_at"]


def clean_str(col_name):
    """trim + null-like tokens -> NULL"""
    t = F.trim(F.col(col_name))
    return F.when(t.isNull() | F.lower(t).isin(NULL_TOKENS), F.lit(None)).otherwise(t)


def canonical(expr, allowed):
    """Case-insensitive match to an allowed value; returns the canonical spelling, or NULL if not allowed."""
    low = F.lower(expr)
    out = None
    for a in allowed:
        cond = low == F.lit(a.lower())
        out = F.when(cond, F.lit(a)) if out is None else out.when(cond, F.lit(a))
    return out


def reasons(checks):
    """checks = [(failure_condition, 'reason_code'), ...] -> ARRAY<STRING> of the failed reasons only"""
    arr = F.array(*[F.when(cond, F.lit(code)) for cond, code in checks])
    return F.filter(arr, lambda x: x.isNotNull())

# COMMAND ----------

# MAGIC %md ## Orders

# COMMAND ----------

ORDER_COLS = ["order_id", "customer_id", "order_date", "category", "quantity", "unit_price", "order_status", "country"]
bo = spark.table("bronze_orders")

o = (bo.select(*ORDER_COLS, *AUDIT_COLS,
               *[clean_str(c).alias(f"c_{c}") for c in ORDER_COLS])
     # safe typing
     .withColumn("t_quantity", F.expr("try_cast(c_quantity AS INT)"))
     .withColumn("t_unit_price", F.expr("try_cast(c_unit_price AS DECIMAL(12,2))"))
     .withColumn("t_order_date", F.expr(
         "CASE WHEN c_order_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
         "THEN to_date(try_to_timestamp(c_order_date, 'yyyy-MM-dd')) END"))
     .withColumn("t_category", canonical(F.col("c_category"), ALLOWED_CATEGORIES))
     .withColumn("t_order_status", canonical(F.col("c_order_status"), ALLOWED_ORDER_STATUSES))
     .withColumn("t_country", canonical(F.col("c_country"), ALLOWED_COUNTRIES)))

o = o.withColumn("rejection_reason", reasons([
    (F.col("c_order_id").isNull(), "missing_order_id"),
    (F.col("c_order_id").isNotNull() & ~F.col("c_order_id").rlike(r"^ORD-[0-9]{7}$"), "invalid_order_id_format"),
    (F.col("c_customer_id").isNull(), "missing_customer_id"),
    (F.col("c_order_date").isNull(), "missing_order_date"),
    (F.col("c_order_date").isNotNull() & F.col("t_order_date").isNull(), "invalid_order_date"),
    (F.col("t_order_date") > F.current_date(), "future_order_date"),
    (F.col("c_category").isNull(), "missing_category"),
    (F.col("c_category").isNotNull() & F.col("t_category").isNull(), "invalid_category"),
    (F.col("c_quantity").isNull(), "missing_quantity"),
    (F.col("c_quantity").isNotNull() & F.col("t_quantity").isNull(), "non_numeric_quantity"),
    (F.col("t_quantity") < 0, "negative_quantity"),
    ((F.col("t_quantity") == 0) | (F.col("t_quantity") > QTY_MAX), "quantity_out_of_range"),
    (F.col("c_unit_price").isNull(), "missing_unit_price"),
    (F.col("c_unit_price").isNotNull() & F.col("t_unit_price").isNull(), "non_numeric_unit_price"),
    (F.col("t_unit_price") < 0, "negative_unit_price"),
    (((F.col("t_unit_price") >= 0) & (F.col("t_unit_price") < PRICE_MIN)) | (F.col("t_unit_price") > PRICE_MAX),
     "unit_price_out_of_range"),
    (F.col("c_order_status").isNull(), "missing_order_status"),
    (F.col("c_order_status").isNotNull() & F.col("t_order_status").isNull(), "invalid_order_status"),
    (F.col("c_country").isNull(), "missing_country"),
    (F.col("c_country").isNotNull() & F.col("t_country").isNull(), "invalid_country"),
])).withColumn("is_valid", F.size("rejection_reason") == 0)

# --- Quarantine (validation first) ---
orders_quarantine = (o.where(~F.col("is_valid"))
                     .select(*ORDER_COLS, "rejection_reason",
                             F.col("rejection_reason")[0].alias("primary_rejection_reason"),
                             *AUDIT_COLS, F.current_timestamp().alias("_silver_processed_at")))

# --- Deduplication among valid rows (keep-one rule) ---
status_rank = F.create_map(*[x for k, v in ORDER_STATUS_RANK.items() for x in (F.lit(k), F.lit(v))])
w = Window.partitionBy("c_order_id").orderBy(F.desc("_status_rank"), F.asc("_row_hash"), F.asc("_bronze_row_id"))
valid_o = (o.where("is_valid")
           .withColumn("_status_rank", status_rank[F.col("t_order_status")])
           .withColumn("_rn", F.row_number().over(w))
           .withColumn("_winner_row_hash", F.first("_row_hash").over(w))
           .withColumn("_winner_bronze_row_id", F.first("_bronze_row_id").over(w)))

silver_orders = (valid_o.where("_rn = 1")
                 .select(F.col("c_order_id").alias("order_id"),
                         F.col("c_customer_id").alias("customer_id"),
                         F.col("t_order_date").alias("order_date"),
                         F.col("t_category").alias("category"),
                         F.col("t_quantity").alias("quantity"),
                         F.col("t_unit_price").alias("unit_price_usd"),
                         F.col("t_order_status").alias("order_status"),
                         F.col("t_country").alias("country"),
                         (F.col("t_quantity") * F.col("t_unit_price")).cast("decimal(18,2)").alias("order_amount_usd"),
                         *AUDIT_COLS, F.current_timestamp().alias("_silver_processed_at")))

orders_duplicates = (valid_o.where("_rn > 1")
                     .select(F.col("c_order_id").alias("business_key"), *ORDER_COLS,
                             F.when(F.col("_row_hash") == F.col("_winner_row_hash"), F.lit("exact_duplicate"))
                              .otherwise(F.lit("key_duplicate")).alias("duplicate_type"),
                             F.col("_rn").alias("duplicate_rank"),
                             F.col("_winner_bronze_row_id").alias("winner_bronze_row_id"),
                             *AUDIT_COLS, F.current_timestamp().alias("_silver_processed_at")))

for df, name in [(silver_orders, "silver_orders"), (orders_quarantine, "silver_orders_quarantine"),
                 (orders_duplicates, "silver_orders_duplicates")]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
print("orders written")

# COMMAND ----------

# MAGIC %md ## Transactions
# MAGIC Relationship checks: `orphan_order_id` = the order id does not exist in the source orders at all (Bronze).
# MAGIC `parent_order_not_accepted` = the order exists but was quarantined, so it is not in `silver_orders`. Both are rejected,
# MAGIC so every accepted transaction joins to exactly one accepted order.

# COMMAND ----------

TXN_COLS = ["txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts"]
bt = spark.table("bronze_transactions")
bronze_order_ids = (spark.table("bronze_orders").select(F.trim("order_id").alias("_ref_oid")).distinct()
                    .withColumn("_in_bronze_orders", F.lit(True)))
silver_order_ids = spark.table("silver_orders").select(F.col("order_id").alias("_ref_oid2"),
                                                        F.lit(True).alias("_in_silver_orders"))
fx = spark.table("ref_fx_rates").select(F.col("currency").alias("_fx_ccy"), "usd_per_unit")

t = (bt.select(*TXN_COLS, *AUDIT_COLS, *[clean_str(c).alias(f"c_{c}") for c in TXN_COLS])
     .withColumn("t_amount", F.expr("try_cast(c_amount AS DECIMAL(18,2))"))
     .withColumn("t_txn_ts", F.expr(
         "CASE WHEN c_txn_ts RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' "
         "THEN try_to_timestamp(c_txn_ts, 'yyyy-MM-dd HH:mm:ss') END"))
     .withColumn("t_payment_method", canonical(F.col("c_payment_method"), ALLOWED_PAYMENT_METHODS))
     .withColumn("t_currency", canonical(F.col("c_currency"), SUPPORTED_CURRENCIES))
     .withColumn("t_txn_status", canonical(F.col("c_txn_status"), ALLOWED_TXN_STATUSES))
     .join(bronze_order_ids, F.col("c_order_id") == F.col("_ref_oid"), "left")
     .join(silver_order_ids, F.col("c_order_id") == F.col("_ref_oid2"), "left")
     .join(F.broadcast(fx), F.col("t_currency") == F.col("_fx_ccy"), "left")
     .withColumn("t_amount_usd", F.round(F.col("t_amount") * F.col("usd_per_unit"), 2).cast("decimal(18,2)")))

t = t.withColumn("rejection_reason", reasons([
    (F.col("c_txn_id").isNull(), "missing_txn_id"),
    (F.col("c_txn_id").isNotNull() & ~F.col("c_txn_id").rlike(r"^TXN-[0-9]{8}$"), "invalid_txn_id_format"),
    (F.col("c_order_id").isNull(), "missing_order_id"),
    (F.col("c_order_id").isNotNull() & F.col("_in_bronze_orders").isNull(), "orphan_order_id"),
    (F.col("_in_bronze_orders") & F.col("_in_silver_orders").isNull(), "parent_order_not_accepted"),
    (F.col("c_payment_method").isNull(), "missing_payment_method"),
    (F.col("c_payment_method").isNotNull() & F.col("t_payment_method").isNull(), "invalid_payment_method"),
    (F.col("c_amount").isNull(), "missing_amount"),
    (F.col("c_amount").isNotNull() & F.col("t_amount").isNull(), "non_numeric_amount"),
    (F.col("t_amount") <= 0, "non_positive_amount"),
    (F.col("t_amount_usd") > MAX_TXN_AMOUNT_USD, "amount_out_of_range"),
    (F.col("c_currency").isNull(), "missing_currency"),
    (F.col("c_currency").isNotNull() & F.col("t_currency").isNull(), "unsupported_currency"),
    (F.col("c_txn_status").isNull(), "missing_txn_status"),
    (F.col("c_txn_status").isNotNull() & F.col("t_txn_status").isNull(), "invalid_txn_status"),
    (F.col("c_txn_ts").isNull(), "missing_txn_ts"),
    (F.col("c_txn_ts").isNotNull() & F.col("t_txn_ts").isNull(), "invalid_txn_ts"),
    (F.col("t_txn_ts") > F.current_timestamp(), "future_txn_ts"),
])).withColumn("is_valid", F.size("rejection_reason") == 0)

txn_quarantine = (t.where(~F.col("is_valid"))
                  .select(*TXN_COLS, "rejection_reason",
                          F.col("rejection_reason")[0].alias("primary_rejection_reason"),
                          *AUDIT_COLS, F.current_timestamp().alias("_silver_processed_at")))

# Keep-one rule for duplicate txn_id: latest txn_ts wins (late-arriving update), then lowest _row_hash, then _bronze_row_id
w = Window.partitionBy("c_txn_id").orderBy(F.desc("t_txn_ts"), F.asc("_row_hash"), F.asc("_bronze_row_id"))
valid_t = (t.where("is_valid")
           .withColumn("_rn", F.row_number().over(w))
           .withColumn("_winner_row_hash", F.first("_row_hash").over(w))
           .withColumn("_winner_bronze_row_id", F.first("_bronze_row_id").over(w)))

silver_transactions = (valid_t.where("_rn = 1")
                       .select(F.col("c_txn_id").alias("txn_id"),
                               F.col("c_order_id").alias("order_id"),
                               F.col("t_payment_method").alias("payment_method"),
                               F.col("t_amount").alias("amount"),
                               F.col("t_currency").alias("currency"),
                               F.col("t_amount_usd").alias("amount_usd"),
                               F.col("t_txn_status").alias("txn_status"),
                               F.col("t_txn_ts").alias("txn_ts"),
                               F.to_date("t_txn_ts").alias("txn_date"),
                               *AUDIT_COLS, F.current_timestamp().alias("_silver_processed_at")))

txn_duplicates = (valid_t.where("_rn > 1")
                  .select(F.col("c_txn_id").alias("business_key"), *TXN_COLS,
                          F.when(F.col("_row_hash") == F.col("_winner_row_hash"), F.lit("exact_duplicate"))
                           .otherwise(F.lit("key_duplicate")).alias("duplicate_type"),
                          F.col("_rn").alias("duplicate_rank"),
                          F.col("_winner_bronze_row_id").alias("winner_bronze_row_id"),
                          *AUDIT_COLS, F.current_timestamp().alias("_silver_processed_at")))

for df, name in [(silver_transactions, "silver_transactions"), (txn_quarantine, "silver_transactions_quarantine"),
                 (txn_duplicates, "silver_transactions_duplicates")]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
print("transactions written")

# COMMAND ----------

# MAGIC %md ## Reconciliation: Bronze = accepted + quarantined + duplicate losers (no overlap)

# COMMAND ----------

def reconcile(dataset, bronze, accepted, quarantine, duplicates):
    b = spark.table(bronze).select("_bronze_row_id")
    parts = [spark.table(x).select("_bronze_row_id") for x in (accepted, quarantine, duplicates)]
    n_b = b.count()
    n_a, n_q, n_d = [p.count() for p in parts]
    union = reduce(lambda x, y: x.unionAll(y), parts)
    n_union_distinct = union.distinct().count()
    missing = b.join(union, "_bronze_row_id", "left_anti").count()      # Bronze rows with no outcome
    extra = union.join(b, "_bronze_row_id", "left_anti").count()        # outcome rows not in Bronze
    overlap = (n_a + n_q + n_d) - n_union_distinct                      # rows in more than one bucket
    ok = (n_b == n_a + n_q + n_d) and missing == 0 and extra == 0 and overlap == 0
    return (dataset, n_b, n_a, n_q, n_d, n_a + n_q + n_d, n_b - (n_a + n_q + n_d), overlap, missing, extra,
            "PASS" if ok else "FAIL")


recon = spark.createDataFrame(
    [reconcile("orders", "bronze_orders", "silver_orders", "silver_orders_quarantine", "silver_orders_duplicates"),
     reconcile("transactions", "bronze_transactions", "silver_transactions", "silver_transactions_quarantine",
               "silver_transactions_duplicates")],
    "dataset STRING, bronze_rows BIGINT, accepted_rows BIGINT, quarantined_rows BIGINT, duplicate_loser_rows BIGINT, "
    "outcome_total BIGINT, difference BIGINT, overlapping_rows BIGINT, bronze_rows_without_outcome BIGINT, "
    "outcome_rows_not_in_bronze BIGINT, status STRING").withColumn("checked_at", F.current_timestamp())
recon.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_reconciliation")
display(spark.table("silver_reconciliation"))

# COMMAND ----------

# Row counts by rule (a row can fail several rules, so these can add up to more than the quarantine count)
rule_counts = (spark.table("silver_orders_quarantine").select(F.lit("orders").alias("dataset"),
                                                              F.explode("rejection_reason").alias("rejection_reason"))
               .unionByName(spark.table("silver_transactions_quarantine")
                            .select(F.lit("transactions").alias("dataset"),
                                    F.explode("rejection_reason").alias("rejection_reason")))
               .groupBy("dataset", "rejection_reason").count().withColumnRenamed("count", "rows"))
rule_counts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_rule_counts")
display(spark.table("silver_rule_counts").orderBy("dataset", F.desc("rows")))

# COMMAND ----------

# Documentation
docs = {
    "silver_orders": "Silver: typed, validated orders. Grain: one row per order_id (unique). Prices in USD.",
    "silver_orders_quarantine": "Silver: Bronze order rows that failed one or more rules; rejection_reason lists every failed rule.",
    "silver_orders_duplicates": "Silver: valid order rows that lost deduplication (keep rule: highest lifecycle status, then lowest _row_hash).",
    "silver_transactions": "Silver: typed, validated transactions whose order is in silver_orders. Grain: one row per txn_id (unique). amount_usd uses ref_fx_rates.",
    "silver_transactions_quarantine": "Silver: Bronze transaction rows that failed one or more rules (incl. orphan / parent-not-accepted).",
    "silver_transactions_duplicates": "Silver: valid transaction rows that lost deduplication (keep rule: latest txn_ts, then lowest _row_hash).",
    "silver_reconciliation": "Evidence: Bronze = accepted + quarantined + duplicate losers, with overlap checks, per dataset.",
    "silver_rule_counts": "Evidence: number of quarantined rows failing each rule.",
}
for tbl, txt in docs.items():
    spark.sql(f"COMMENT ON TABLE {tbl} IS '{txt}'")

# COMMAND ----------

# Quality gate: fail the task (and therefore the job) if reconciliation does not hold
failed = spark.table("silver_reconciliation").where("status <> 'PASS'").collect()
if failed:
    raise Exception(f"Silver reconciliation FAILED: {failed}")
for tbl, key in [("silver_orders", "order_id"), ("silver_transactions", "txn_id")]:
    n, d = spark.table(tbl).select(F.count("*"), F.countDistinct(key)).first()
    if n != d:
        raise Exception(f"{tbl}: {key} is not unique")
print("Silver reconciliation PASSED for both datasets.")
