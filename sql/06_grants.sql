-- 06_grants.sql : least-privilege reporting access to GOLD ONLY
-- Principal: account group `ecom_reporting` (create it in Settings -> Identity and access -> Groups).
-- If you cannot create groups/users in the trial, this file is the executable plan; document the restriction.
USE CATALOG workspace; USE SCHEMA ecom_poc;

-- 1. Navigation privileges only (no data access by themselves)
GRANT USE CATALOG ON CATALOG workspace          TO `ecom_reporting`;
GRANT USE SCHEMA  ON SCHEMA  workspace.ecom_poc TO `ecom_reporting`;

-- 2. Read access to the Gold tables only
GRANT SELECT ON TABLE workspace.ecom_poc.gold_daily_sales       TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_payment_success   TO `ecom_reporting`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_top_customers     TO `ecom_reporting`;
-- Deliberately NOT granted: SELECT on bronze_* / silver_* / ops_* / gold_order_revenue (order-level), READ VOLUME on raw_files,
-- MODIFY, CREATE TABLE, ALL PRIVILEGES. The SQL warehouse needs CAN USE (set in the warehouse Permissions dialog).

-- 3. Verify
SHOW GRANTS `ecom_reporting` ON SCHEMA workspace.ecom_poc;
SHOW GRANTS ON TABLE workspace.ecom_poc.gold_daily_sales;
SHOW GRANTS ON TABLE workspace.ecom_poc.silver_orders;   -- ecom_reporting must NOT appear

-- 4. How to test (as a member of ecom_reporting, on the SQL warehouse):
--   SELECT COUNT(*) FROM workspace.ecom_poc.gold_daily_sales;   -- succeeds
--   SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders;      -- fails: PERMISSION_DENIED / INSUFFICIENT_PERMISSIONS
--   LIST '/Volumes/workspace/ecom_poc/raw_files/';               -- fails

-- 5. Rollback
-- REVOKE SELECT ON TABLE workspace.ecom_poc.gold_daily_sales FROM `ecom_reporting`;  (repeat per table)
