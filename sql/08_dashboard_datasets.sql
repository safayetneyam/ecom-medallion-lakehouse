-- 08_dashboard_datasets.sql : datasets for the Databricks AI/BI dashboard (paste each into Data -> Create from SQL)
-- Replace `workspace` with your catalog.

-- Dataset: daily_sales   (KPI counters + time trend). Filter field: order_date
SELECT order_date, orders, cancelled_orders, paid_orders, gross_revenue_usd, refunds_usd, net_revenue_usd
FROM workspace.ecom_poc.gold_daily_sales;

-- Dataset: revenue_breakdown   (category and country breakdowns). Filter field: order_date
SELECT order_date, category, country, orders, net_revenue_usd
FROM workspace.ecom_poc.gold_revenue_breakdown;

-- Dataset: payment_success   (success rate by method). Filter field: txn_date
SELECT txn_date, payment_method, total_attempts, successful_attempts, failed_attempts
FROM workspace.ecom_poc.gold_payment_success;

-- Dataset: top_customers   (table of top 10)
SELECT spend_rank, customer_id, orders, net_revenue_usd, last_order_date
FROM workspace.ecom_poc.gold_top_customers WHERE spend_rank <= 10;

-- Custom calculations to add in the dashboard (same definitions as sql/04):
--   Net revenue        : SUM(net_revenue_usd)
--   Orders             : SUM(orders)
--   Cancellation rate  : SUM(cancelled_orders) / SUM(orders)
--   Avg order value    : SUM(net_revenue_usd) / SUM(paid_orders)
--   Payment success    : SUM(successful_attempts) / SUM(total_attempts)
