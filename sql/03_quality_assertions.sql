-- 03_quality_assertions.sql : every query must return passed = true (or 0 rows where stated)
USE CATALOG workspace; USE SCHEMA ecom_poc;

-- A. Source -> Bronze parity (lossless)
SELECT i.file_name, i.data_rows, b.bronze_rows, i.data_rows = b.bronze_rows AS passed
FROM ops_source_inventory i
JOIN (SELECT 'orders.csv' file_name, COUNT(*) bronze_rows FROM bronze_orders
      UNION ALL SELECT 'transactions.csv', COUNT(*) FROM bronze_transactions) b USING (file_name);

-- B. Bronze row ids are unique, and all source columns are STRING
SELECT 'bronze_orders' t, COUNT(*) = COUNT(DISTINCT _bronze_row_id) AS passed FROM bronze_orders
UNION ALL SELECT 'bronze_transactions', COUNT(*) = COUNT(DISTINCT _bronze_row_id) FROM bronze_transactions;

SELECT table_name, column_name, data_type FROM information_schema.columns
WHERE table_schema = 'ecom_poc' AND table_name LIKE 'bronze_%' AND column_name NOT LIKE '\_%' AND data_type <> 'STRING';
-- expect 0 rows

-- C. Silver reconciliation: Bronze = accepted + quarantined + duplicate losers, no overlap
SELECT dataset, bronze_rows, accepted_rows, quarantined_rows, duplicate_loser_rows, difference, overlapping_rows,
       status = 'PASS' AS passed
FROM silver_reconciliation;

-- D. Silver business keys are unique
SELECT 'silver_orders' t, COUNT(*) = COUNT(DISTINCT order_id) AS passed FROM silver_orders
UNION ALL SELECT 'silver_transactions', COUNT(*) = COUNT(DISTINCT txn_id) FROM silver_transactions;

-- E. Silver content rules hold on accepted rows
SELECT COUNT(*) AS violations FROM silver_orders
WHERE customer_id IS NULL OR order_date IS NULL OR order_date > current_date()
   OR quantity NOT BETWEEN 1 AND 50 OR unit_price_usd NOT BETWEEN 0.50 AND 10000
   OR order_status NOT IN ('pending','processing','shipped','delivered','cancelled','returned');
-- expect 0
SELECT COUNT(*) AS violations FROM silver_transactions t
LEFT JOIN silver_orders o USING (order_id)
WHERE o.order_id IS NULL OR t.amount <= 0 OR t.amount_usd > 500000 OR t.txn_ts > current_timestamp()
   OR t.currency NOT IN (SELECT currency FROM ref_fx_rates)
   OR t.txn_status NOT IN ('SUCCESS','FAILED','PENDING','REFUNDED');
-- expect 0

-- F. Every quarantined row has at least one reason
SELECT 'orders' d, COUNT(*) AS rows_without_reason FROM silver_orders_quarantine WHERE size(rejection_reason) = 0
UNION ALL SELECT 'transactions', COUNT(*) FROM silver_transactions_quarantine WHERE size(rejection_reason) = 0;
-- expect 0, 0

-- G. Gold grain uniqueness and revenue consistency
SELECT COUNT(*) = COUNT(DISTINCT order_date) AS passed FROM gold_daily_sales;
SELECT COUNT(*) = COUNT(DISTINCT order_date, category, country) AS passed FROM gold_revenue_breakdown;
SELECT COUNT(*) = COUNT(DISTINCT txn_date, payment_method) AS passed FROM gold_payment_success;
SELECT COUNT(*) = COUNT(DISTINCT customer_id) AS passed FROM gold_top_customers;
SELECT (SELECT SUM(net_revenue_usd) FROM gold_daily_sales) = (SELECT SUM(net_revenue_usd) FROM gold_revenue_breakdown) AS passed;
