-- 01_setup.sql : Unity Catalog setup (run in the SQL editor or a notebook)
-- Replace `workspace` with a catalog that you can see in Catalog Explorer and where you have CREATE SCHEMA.
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS ecom_poc COMMENT 'E-commerce medallion POC (Bronze/Silver/Gold) - synthetic data only';
USE SCHEMA ecom_poc;
CREATE VOLUME IF NOT EXISTS raw_files COMMENT 'Landing zone: raw, unchanged source CSV extracts';

-- Upload both files into:
--   /Volumes/workspace/ecom_poc/raw_files/orders.csv
--   /Volumes/workspace/ecom_poc/raw_files/transactions.csv
-- UI: New -> Add or upload data -> Upload files to a volume

-- Reproducible source row counts (data rows exclude the header line)
SELECT 'orders.csv' AS file_name, COUNT(*) - 1 AS data_rows
FROM read_files('/Volumes/workspace/ecom_poc/raw_files/orders.csv', format => 'text')
UNION ALL
SELECT 'transactions.csv', COUNT(*) - 1
FROM read_files('/Volumes/workspace/ecom_poc/raw_files/transactions.csv', format => 'text');

-- File sizes (bytes) and paths
LIST '/Volumes/workspace/ecom_poc/raw_files/';
