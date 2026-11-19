# Trustworthy e-commerce lakehouse: Databricks Medallion assignment

**Candidate:** ⟨FILL: your name⟩ · **Instructor:** Lipton Ahammed · **Submission:** 19 November 2026
**Repository:** ⟨FILL: GitHub URL⟩ · **Dashboard:** ⟨FILL: AI/BI dashboard link⟩

> Everything in `evidence/` comes from my own Databricks free-trial workspace. All data is synthetic (seed 42). No tokens, credentials or personal data are included.

---

## 1. Business context

An e-commerce company receives daily CSV extracts of **orders** and **payment transactions**. Nulls, invalid types, inconsistent labels, bad timestamps, orphan references and duplicates make its sales reports unreliable.
This project keeps the raw evidence (**Bronze**), cleans it with explicit and auditable rules (**Silver**), and publishes documented, reusable metrics (**Gold**). The metrics are consumed in a Databricks AI/BI dashboard, a Genie Agent and Power BI. All three report the **same revenue number**.

```
orders.csv / transactions.csv (1,000,000 rows each)
   └─► UC volume /Volumes/<catalog>/ecom_poc/raw_files/        (landing: unchanged files)
        └─► bronze_orders / bronze_transactions                 (all STRING + audit columns)
             └─► silver_* accepted | *_quarantine | *_duplicates (typed, validated, reconciled)
                  └─► gold_daily_sales · gold_revenue_breakdown · gold_payment_success · gold_top_customers
                       └─► SQL warehouse ─► AI/BI dashboard · Genie Agent · Power BI Desktop
Orchestration: Lakeflow Job (Bronze → Silver → Gold) · Governance: Unity Catalog · Versioning: Git folder
```

## 2. Trial workspace and environment

| Item | Value |
|---|---|
| Signup | Databricks **free trial** (express setup) with my own email ⟨FILL: personal/work/academic⟩, verified |
| Cloud / region | ⟨FILL: e.g. AWS · us-east-1 (Databricks-managed serverless workspace)⟩ |
| Workspace URL | ⟨FILL: https://dbc-xxxx.cloud.databricks.com (hostname masked in screenshots)⟩ |
| Trial start / expiry | ⟨FILL: start date⟩ / ⟨FILL: start + 14 days⟩ |
| Credits at start / at the end | ⟨FILL⟩ / ⟨FILL⟩ |
| Catalog / schema / volume | `⟨FILL: workspace⟩` / `ecom_poc` / `raw_files` |
| ETL compute | Serverless notebook and job compute |
| BI compute | One serverless SQL warehouse (smallest size, auto stop 10 min) |
| Observed restrictions | ⟨FILL: e.g. only one SQL warehouse allowed; external network access limited; could/could not create groups; Genie available yes/no⟩ |

**How the free trial differs from Free Edition.** The free trial is the full commercial Databricks platform for a limited time (about 14 days), paid for by trial credits (Databricks advertises up to $400 for eligible sign-ups). Express setup gives a serverless workspace with default storage, Unity Catalog, serverless notebooks and jobs, SQL warehouses, AI/BI dashboards, Genie and partner connectors. During the trial, compute scaling is limited (for example one SQL warehouse) and external network access is limited. When the credits or days run out, a payment method is needed to continue. **Free Edition** is a permanently free, quota-limited environment for learning and non-commercial use. It doesn't consume credits, but compute sizes and some features are restricted. The assignment requires the free trial, so all work was done there and Free Edition was not used.

**Naming standard and raw path.** `catalog.schema.object` with lower-snake-case names. The schema is `ecom_poc`. Tables are prefixed by layer (`bronze_`, `silver_`, `gold_`), plus `ops_` for operational evidence and `ref_` for reference data. Rejected and duplicate outcomes use the suffixes `_quarantine` / `_duplicates`. Raw files live unchanged in the managed volume:
`/Volumes/⟨catalog⟩/ecom_poc/raw_files/orders.csv` and `/Volumes/⟨catalog⟩/ecom_poc/raw_files/transactions.csv`.

**Compute choices and credit control.** ETL runs on **serverless notebook and job compute**. It starts in seconds, scales for the 1M-row transformations and stops when idle, so no cluster sits running. BI (dashboard, Genie, Power BI) uses **one small serverless SQL warehouse**. A warehouse is built for many short SQL queries and is what Genie and the Power BI connector need. To save credits I used the smallest size and a 10-minute auto stop, and stopped the warehouse manually after BI work. I ran each layer once per code change, with no parallel experiments, and ran the job on demand. The schedule was only a paused test window, and I picked Power BI **Import** mode so the warehouse can stay off.

**Git.** The repo was connected through a **Databricks Git folder** (Workspace → Create → Git folder) with a GitHub personal access token in *Settings → Linked accounts*. I committed after each stage (setup, generator, Bronze, Silver, Gold, BI, operations). ⟨FILL: limitation, or "none"⟩

## 3. Synthetic source files

* Generator: `generator/generate_data.py`, seeded (42) and reproducible. A re-run gives byte-identical files (same SHA-256).
* Checker: `generator/verify_data.py` re-detects every anomaly from the raw values alone.
* Dirty-data inventory: `generator/dirty_data_report.md` (type, field, rows, %). 14 anomaly classes per file.

| File | Data rows (excl. header) | Bytes | Volume path |
|---|---:|---:|---|
| orders.csv | 1,000,000 | 72,428,604 | `/Volumes/⟨catalog⟩/ecom_poc/raw_files/orders.csv` |
| transactions.csv | 1,000,000 | 76,459,630 | `/Volumes/⟨catalog⟩/ecom_poc/raw_files/transactions.csv` |

The counts were reproduced in Databricks by `notebooks/00_setup.py` (table `ops_source_inventory`) and `sql/01_setup.sql`.

**Why the injected anomalies stay a small, measured fraction.** About 9% of rows are anomalous. Each class is 0.2–1% (case/whitespace 3%), and each row has at most one anomaly. Because every count is known, every rule can be verified exactly: expected rows equal quarantined rows. The dataset also stays representative, so Gold metrics describe a believable business instead of being dominated by noise. With a large or unknown dirty share, you couldn't tell a broken rule from bad data, and the analytics would be meaningless.

**Why real customer data, credentials and raw extracts must not be committed to Git.** Git history is permanent and gets copied to every clone, fork and CI runner. A secret or personal record pushed once has to be treated as leaked even if it is deleted later. Real data carries privacy and legal obligations (consent, retention, right to erasure) that a code repository can't meet, and raw extracts are large and change daily. Code belongs in Git and data belongs in governed storage. **Minimum production controls:** a `.gitignore` for data and secret files (included); secret scanning and push protection on the repo; credentials kept in a secret manager (Databricks secrets or the cloud key vault), never in notebooks; data only in Unity Catalog volumes and tables with least-privilege grants and audit logs; synthetic or masked data in dev and test; short-lived tokens or OAuth; and branch protection with code review.

## 4. Bronze: lossless ingestion

`notebooks/01_bronze_ingestion.py` loads each CSV into its own Delta table, `bronze_orders` and `bronze_transactions`. It uses an **explicit all-STRING schema** (`inferSchema=false`), so no value is changed or rejected while reading.

**Why malformed data belongs in Bronze.** Bronze is the system of record for *what was received*. If bad rows were dropped while reading, nobody could prove what the source sent, measure data quality, fix a rule and reprocess, or answer an auditor. Keeping every value as a string means the load never fails on bad types. Judging the data is Silver's job, where every rejection is explicit and counted.

| Audit column | Meaning |
|---|---|
| `_ingested_at` | When this load wrote the row (load-time lineage, freshness). |
| `_source_file` | Volume path of the file the row came from (`_metadata.file_path`). Traces a row back to its file. |
| `_batch_id` | Deterministic load key `file name:size:modification time`. Identifies a delivery; used to make re-runs idempotent. |
| `_row_hash` | SHA-256 of all source values. Identical rows share a hash (exact-duplicate detection, change detection). |
| `_bronze_row_id` | Unique deterministic id per raw row = hash(`_batch_id`, `_row_hash`, occurrence). Carried into every Silver outcome for row-level reconciliation. |
| `_rescued_data` | Anything the CSV reader could not map to the schema (expected NULL, which proves nothing was lost). |

**Lossless and idempotent.** For each file, the notebook compares the *source data rows* (physical lines − 1 header, counted independently of the CSV parser) with the Bronze rows for that batch, and fails if they differ. Each load writes with `replaceWhere _batch_id = '<this batch>'`, so a re-run **replaces** its own batch instead of appending it again. `ops_bronze_load_log` shows two runs with identical counts (evidence: `evidence/05_bronze/bronze_load_log_rerun.png`). A genuinely new file version gets a new batch id and is detected by the "table holds more than one batch" check.

| Check | orders | transactions |
|---|---:|---:|
| Source data rows | 1,000,000 | 1,000,000 |
| Bronze rows | ⟨FILL: 1,000,000⟩ | ⟨FILL: 1,000,000⟩ |

## 5. Silver: cleaning, validation and reconciliation

`notebooks/02_silver_transform.py`, rules in `sql/02_business_rules_matrix.md`.

**Precedence:** standardise (trim, null tokens → NULL, canonical case) → safe typing → **validate** (any failure → quarantine) → **deduplicate the valid rows** (losers → duplicates table) → reconcile. Each Bronze row therefore has exactly one outcome.

**What happens to strings that can't be parsed?** `try_cast` and `try_to_timestamp` return **NULL** instead of raising an error, so the job never crashes on bad data. The original string stays in the row. A rule then sees "value present but typed value NULL" and quarantines the row with a specific reason (`non_numeric_quantity`, `invalid_order_date`, …). So nothing is silently converted to NULL and kept as if it were valid. Example: `quantity = "3 pcs"` → `try_cast` = NULL → quarantined with `non_numeric_quantity`; `"  DELIVERED "` → standardised to `delivered` → accepted.

**Deduplication (keep-one rule).**
* *Exact duplicate row:* every source value is identical (same `_row_hash`). The copies are indistinguishable, so the lowest `_bronze_row_id` is kept.
* *Duplicated key:* same `order_id` / `txn_id`, but different content (a re-extracted update).
  * Orders: keep the most advanced lifecycle status (pending < processing < shipped < delivered < cancelled < returned), then the lowest `_row_hash`.
  * Transactions: keep the latest `txn_ts` (latest update), then the lowest `_row_hash`.
* `ROW_NUMBER()` with a total ordering makes the choice stable across re-runs. Losers keep `winner_bronze_row_id` and `duplicate_type` (`exact_duplicate` / `key_duplicate`).

**Quarantine, never silent deletion.** Invalid rows go to `silver_orders_quarantine` / `silver_transactions_quarantine`, with all original values, a `rejection_reason` array (every failed rule) and the Bronze audit ids.

**Reconciliation** (`silver_reconciliation`; the notebook raises an error if it fails):

| Dataset | Bronze | Accepted | Quarantined | Duplicate losers | Difference | Overlap | Status |
|---|---:|---:|---:|---:|---:|---:|---|
| orders | ⟨1,000,000⟩ | ⟨938,000⟩ | ⟨52,000⟩ | ⟨10,000⟩ | 0 | 0 | PASS |
| transactions | ⟨1,000,000⟩ | ⟨891,725⟩ | ⟨98,799⟩ | ⟨9,476⟩ | 0 | 0 | PASS |

⟨FILL: replace with your run's numbers. `evidence/expected_results.md` lists the expected values.⟩ No overlap is proven on `_bronze_row_id`: the three outcome sets are disjoint and together equal the Bronze set.

## 6. Gold: business-ready models

**Revenue definition.** *Net revenue (USD)* = Σ `amount_usd` of accepted **SUCCESS** transactions − Σ `amount_usd` of accepted **REFUNDED** transactions, attributed to the order's `order_date`.
* FAILED and PENDING are never revenue. A failed attempt followed by a successful retry counts once.
* Partial or split payments: every SUCCESS row counts, so two halves add up to the order total.
* Repeat rows for the same `txn_id` are removed in Silver.
* Refunds and cancellations: REFUNDED is subtracted. A cancelled order without a successful payment contributes 0.
* Currency: converted to USD with the fixed `ref_fx_rates` table. All Gold money is USD.
* Only transactions whose order was accepted count.
* **Source of truth:** `silver_transactions` (money actually captured). `order_amount_usd` (qty × price) is booked value, **not** revenue.

*Small example* (from `sql/04_gold_metric_definitions.sql`):

| Order pattern | Transactions | Net revenue |
|---|---|---|
| Retry | FAILED 50.00 USD, then SUCCESS 50.00 USD | **50.00** |
| Split payment | SUCCESS 30.00 + SUCCESS 30.00 | **60.00** |
| Returned | SUCCESS 4,200 BDT (= 35.28 USD), then REFUNDED 4,200 BDT | **0.00** |
| Pending | PENDING 20.00 | **0.00** |

| Gold table | Grain (unique key) | Main measures | Rows (my run) |
|---|---|---|---:|
| `gold_daily_sales` | one row per `order_date` | orders, cancelled_orders, cancellation_rate, paid_orders, gross/refunds/net revenue | ⟨669⟩ |
| `gold_revenue_breakdown` | (`order_date`, `category`, `country`) | orders, cancelled_orders, paid_orders, net revenue | ⟨FILL⟩ |
| `gold_payment_success` | (`txn_date`, `payment_method`) | total_attempts (SUCCESS+FAILED), successful_attempts, success_rate | ⟨FILL⟩ |
| `gold_top_customers` | one row per `customer_id` | orders, net_revenue_usd, spend_rank | ⟨145,749⟩ |
| `gold_order_revenue` (helper) | one row per `order_id` | per-order gross, refunds, net | ⟨938,000⟩ |

**Avoiding fan-out.** An order can have several transactions (retry, split, refund). Joining raw transactions to orders and then summing order-level values would multiply them. Transactions are therefore **aggregated to one row per order first** (`gold_order_revenue`) and joined 1:1 to orders. An assertion checks that the row count equals `silver_orders` and that net revenue is identical in every Gold table and in Silver.

**How documentation prevents misinterpretation.** Each table and column has a Unity Catalog comment, and the grain and revenue definition are also table properties. That tells a reader exactly what one row means, so they don't sum a pre-computed rate, double-count by joining at the wrong grain, or confuse booked value with revenue. It also tells them which date the revenue is attributed to and what currency it is in. Genie reads these descriptions too, so it writes better SQL.

## 7. Databricks AI/BI and Genie
See `dashboards/README.md`. The dashboard has 4+ KPI counters, a daily trend, category and country breakdowns, a success-rate chart and **one shared order-date filter**. The Genie Agent answered three business questions, each cross-checked with `sql/07_genie_verification.sql`. ⟨FILL: results table / restriction⟩

## 8. Power BI Desktop
See `powerbi/README.md`. Native connector, **Import** mode, 4 Gold tables, measures in `powerbi/measures.dax`, and a one-page report.
**Revenue agreement (2026-01-01 to 2026-06-30, USD, no other filters):** SQL ⟨FILL⟩ = AI/BI ⟨FILL⟩ = Power BI ⟨FILL⟩.

## 9. Lakeflow Job, security and cleanup

Job `ecom_medallion_bronze_silver_gold` (`operations/lakeflow_job.json`): tasks `bronze_ingestion → silver_transform → gold_models`, each with 1 retry after 60 s, a 30-minute timeout, failure email to my address, and a paused schedule. Successful run: ⟨FILL: run id / date⟩.

**Why Gold doesn't run after a failed dependency.** Each downstream task's default condition is *Run if: All succeeded*. If Silver fails (for example the reconciliation gate raises an error), Gold is marked *Upstream failed* and skipped. That's intended: building Gold on incomplete or unreconciled Silver data would publish wrong numbers to dashboards that look correct. Keeping the last good Gold tables is safer than refreshing them with bad data.

Least-privilege access: `operations/permissions_plan.md` + `sql/06_grants.sql` (reporting group gets `USE CATALOG`, `USE SCHEMA`, and `SELECT` on the 4 Gold tables only). Cleanup: `operations/cleanup_checklist.md`.

## 10. SQL strategy
* PySpark for ingestion and rule logic (reusable functions, explicit schemas). SQL for Gold models, assertions, grants and BI datasets, so analysts can read them.
* Full, deterministic rebuilds of Silver and Gold (`CREATE OR REPLACE`) plus batch-scoped Bronze writes, so every layer is idempotent.
* Quality gates inside the pipeline (Bronze parity, Silver reconciliation, Gold consistency) fail the job instead of publishing bad data.
* One metric definition (`sql/04_gold_metric_definitions.sql`) reused by SQL, AI/BI, Genie instructions and Power BI DAX.

## 11. Trial limitations and fallbacks (disclosed)
| Limitation | Effect | Fallback used |
|---|---|---|
| ⟨FILL⟩ | | |

## 12. Repository map
`generator/` · `notebooks/` · `sql/` · `dashboards/` · `powerbi/` · `evidence/` · `operations/`. The source CSVs are not committed (see `.gitignore`); regenerate them with `python generator/generate_data.py`.
