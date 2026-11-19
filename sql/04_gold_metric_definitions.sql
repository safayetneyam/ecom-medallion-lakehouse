-- 04_gold_metric_definitions.sql : the ONE definition of each metric, used by SQL, AI/BI, Genie and Power BI
USE CATALOG workspace; USE SCHEMA ecom_poc;

-- NET REVENUE (USD)
--   = SUM(amount_usd WHERE txn_status = 'SUCCESS') - SUM(amount_usd WHERE txn_status = 'REFUNDED')
--   source of truth : silver_transactions (accepted, de-duplicated, FX-converted with ref_fx_rates)
--   attributed to   : silver_orders.order_date
--   excluded        : FAILED, PENDING, quarantined and duplicate-loser rows
--   in Gold         : SUM(net_revenue_usd) over gold_daily_sales / gold_revenue_breakdown / gold_top_customers
SELECT SUM(net_revenue_usd) AS net_revenue_usd FROM gold_daily_sales;

-- Same number computed straight from Silver (proves Gold did not change it)
SELECT SUM(CASE WHEN t.txn_status = 'SUCCESS' THEN t.amount_usd
                WHEN t.txn_status = 'REFUNDED' THEN -t.amount_usd ELSE 0 END) AS net_revenue_usd
FROM silver_transactions t JOIN silver_orders o USING (order_id);

-- ORDERS                 = SUM(orders)                          FROM gold_daily_sales
-- CANCELLATION RATE      = SUM(cancelled_orders) / SUM(orders)  FROM gold_daily_sales      (never AVG of daily rates)
-- AVERAGE ORDER VALUE    = SUM(net_revenue_usd) / SUM(paid_orders) FROM gold_daily_sales
-- PAYMENT SUCCESS RATE   = SUM(successful_attempts) / SUM(total_attempts) FROM gold_payment_success
SELECT SUM(orders) AS orders,
       ROUND(SUM(cancelled_orders) / SUM(orders), 4) AS cancellation_rate,
       ROUND(SUM(net_revenue_usd) / SUM(paid_orders), 2) AS avg_order_value_usd
FROM gold_daily_sales;
SELECT ROUND(SUM(successful_attempts) / SUM(total_attempts), 4) AS payment_success_rate FROM gold_payment_success;

-- Small worked example of the revenue definition (one order of each pattern)
WITH t AS (
  SELECT order_id, txn_status, amount, currency, amount_usd,
         COUNT(*) OVER (PARTITION BY order_id) AS n_txn,
         COLLECT_SET(txn_status) OVER (PARTITION BY order_id) AS statuses
  FROM silver_transactions)
SELECT t.*, r.net_revenue_usd AS order_net_revenue_usd
FROM t JOIN gold_order_revenue r USING (order_id)
WHERE (n_txn = 2 AND array_contains(statuses, 'FAILED'))
   OR (n_txn = 2 AND array_contains(statuses, 'REFUNDED'))
   OR (n_txn = 2 AND size(statuses) = 1)
QUALIFY DENSE_RANK() OVER (PARTITION BY array_sort(statuses) ORDER BY order_id) = 1
ORDER BY order_id, txn_status;
