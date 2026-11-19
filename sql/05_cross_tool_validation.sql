-- 05_cross_tool_validation.sql : the reference number that AI/BI and Power BI must reproduce
-- Fixed test slice:  order_date 2026-01-01 .. 2026-06-30 (inclusive), currency = USD (all Gold money is USD),
--                    all categories, all countries, all statuses.
USE CATALOG workspace; USE SCHEMA ecom_poc;

-- 1) From gold_daily_sales (what the KPI counters use)
SELECT SUM(net_revenue_usd) AS net_revenue_usd, SUM(orders) AS orders
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';

-- 2) From gold_revenue_breakdown (what the category/country charts use) - must be identical
SELECT SUM(net_revenue_usd) AS net_revenue_usd, SUM(orders) AS orders
FROM gold_revenue_breakdown
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';

-- 3) Full-period total (no date filter)
SELECT SUM(net_revenue_usd) AS net_revenue_usd_all_time FROM gold_daily_sales;

-- Typical mismatch causes to check if a tool shows a different number:
--   * date filter applied to txn_date instead of order_date, or end date exclusive vs inclusive
--   * a tool summing order_amount_usd (booked value) instead of net_revenue_usd
--   * averaging cancellation_rate / success_rate instead of SUM/SUM
--   * Power BI relationship missing, so the slicer does not filter a table
--   * stale Import data (refresh Power BI after the job re-runs)
