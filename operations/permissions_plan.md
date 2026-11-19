# Least-privilege reporting access (Gold only)

**Goal:** a reporting user can read the four Gold marts and nothing else. That means no Bronze/Silver, no raw files, no write access.

| Principal | Securable | Privilege | Why |
|---|---|---|---|
| `ecom_reporting` (group) | catalog `workspace` | `USE CATALOG` | Navigate only; gives no data access |
| `ecom_reporting` | schema `workspace.ecom_poc` | `USE SCHEMA` | Navigate only |
| `ecom_reporting` | `gold_daily_sales`, `gold_revenue_breakdown`, `gold_payment_success`, `gold_top_customers` | `SELECT` | The only data they need |
| `ecom_reporting` | SQL warehouse | `CAN USE` | To run queries and dashboards |
| — | `bronze_*`, `silver_*`, `ops_*`, `gold_order_revenue`, volume `raw_files` | *(nothing)* | Raw and row-level data stay restricted |

Executable SQL: `sql/06_grants.sql`.

## Steps used
1. **Settings → Identity and access → Groups → Add group** → name `ecom_reporting`. Optionally add a second email address of yours as a test user (**Users → Add user**).
2. Run `sql/06_grants.sql` in the SQL editor.
3. **SQL Warehouses → your warehouse → Permissions** → add `ecom_reporting` with **Can use**.
4. Evidence: `SHOW GRANTS` output, plus Catalog Explorer → a Gold table → **Permissions** tab, plus a Silver table's Permissions tab showing no `ecom_reporting`.

## How it is tested
* As the test user: `SELECT COUNT(*) FROM workspace.ecom_poc.gold_daily_sales` works.
* `SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders` fails with a permission error.

## If the trial blocks group or user creation
Record the exact message (screenshot). Keep `sql/06_grants.sql` as the intended, executable plan and explain the test above.
*Trial restriction observed:* (fill in, or write "none")
