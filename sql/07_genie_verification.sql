-- 07_genie_verification.sql : trusted SQL that checks each Genie answer
USE CATALOG workspace; USE SCHEMA ecom_poc;

-- Q1  "What was total net revenue in USD between 1 January and 30 June 2026?"
SELECT ROUND(SUM(net_revenue_usd), 2) AS net_revenue_usd
FROM gold_daily_sales WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-06-30';

-- Q2  "Which 3 product categories generated the most net revenue in 2026, and how much?"
SELECT category, ROUND(SUM(net_revenue_usd), 2) AS net_revenue_usd
FROM gold_revenue_breakdown WHERE YEAR(order_date) = 2026
GROUP BY category ORDER BY net_revenue_usd DESC LIMIT 3;

-- Q3  "Which payment method has the lowest payment success rate overall?"
SELECT payment_method, ROUND(SUM(successful_attempts) / SUM(total_attempts), 4) AS success_rate
FROM gold_payment_success GROUP BY payment_method ORDER BY success_rate ASC LIMIT 1;
