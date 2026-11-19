# Silver business-rule matrix

Implemented in `notebooks/02_silver_transform.py`. A row that fails **any** rule goes to quarantine, and **every** rule it fails is listed in the `rejection_reason` array.
Row counts per rule come from the table `silver_rule_counts` (query in `notebooks/03_silver_reconciliation_evidence.py`, section 3). Paste your numbers into the last column.

**Standardisation before validation (applies to all text columns):** `trim()`. The null-like tokens `''`, `NULL`, `None`, `N/A` (any case) become real NULL. Case is normalised to the canonical value (for example `  DELIVERED ` becomes `delivered`, ` usd` becomes `USD`). Rows that only had case or whitespace problems are **accepted**, not rejected.

**Safe typing:** numbers use `try_cast(... AS INT / DECIMAL)` and dates use `try_to_timestamp(value, pattern)`. A value that can't be parsed becomes NULL instead of failing the job. The rule `non_numeric_*` / `invalid_*` then quarantines the row.

## orders → `silver_orders`

| # | Field | Rule (row passes when…) | Reason code if it fails | Rows (your run) |
|---:|---|---|---|---:|
| 1 | order_id | not null | `missing_order_id` | |
| 2 | order_id | matches `ORD-` + 7 digits | `invalid_order_id_format` | |
| 3 | customer_id | not null | `missing_customer_id` | |
| 4 | order_date | not null | `missing_order_date` | |
| 5 | order_date | parses as `yyyy-MM-dd` (real calendar date) | `invalid_order_date` | |
| 6 | order_date | not after today (non-future) | `future_order_date` | |
| 7 | category | not null | `missing_category` | |
| 8 | category | one of 8 allowed categories (case-insensitive) | `invalid_category` | |
| 9 | quantity | not null | `missing_quantity` | |
| 10 | quantity | integer (`try_cast` succeeds) | `non_numeric_quantity` | |
| 11 | quantity | ≥ 0 | `negative_quantity` | |
| 12 | quantity | between 1 and 50 | `quantity_out_of_range` | |
| 13 | unit_price | not null | `missing_unit_price` | |
| 14 | unit_price | decimal (`try_cast` succeeds) | `non_numeric_unit_price` | |
| 15 | unit_price | ≥ 0 | `negative_unit_price` | |
| 16 | unit_price | between 0.50 and 10,000.00 USD | `unit_price_out_of_range` | |
| 17 | order_status | not null | `missing_order_status` | |
| 18 | order_status | in pending, processing, shipped, delivered, cancelled, returned | `invalid_order_status` | |
| 19 | country | not null | `missing_country` | |
| 20 | country | one of 10 allowed countries | `invalid_country` | |
| D | order_id | unique among valid rows. Keep rule: highest lifecycle status (pending 1 < processing 2 < shipped 3 < delivered 4 < cancelled 5 < returned 6), then lowest `_row_hash`, then lowest `_bronze_row_id` | → `silver_orders_duplicates` (`exact_duplicate` / `key_duplicate`) | |

## transactions → `silver_transactions`

| # | Field | Rule (row passes when…) | Reason code if it fails | Rows (your run) |
|---:|---|---|---|---:|
| 1 | txn_id | not null | `missing_txn_id` | |
| 2 | txn_id | matches `TXN-` + 8 digits | `invalid_txn_id_format` | |
| 3 | order_id | not null | `missing_order_id` | |
| 4 | order_id | exists in source orders (`bronze_orders`), the **orphan** check | `orphan_order_id` | |
| 5 | order_id | its order was accepted (`silver_orders`), the relationship check | `parent_order_not_accepted` | |
| 6 | payment_method | not null | `missing_payment_method` | |
| 7 | payment_method | in credit_card, debit_card, mobile_wallet, paypal, bank_transfer, cash_on_delivery | `invalid_payment_method` | |
| 8 | amount | not null | `missing_amount` | |
| 9 | amount | decimal (`try_cast` succeeds) | `non_numeric_amount` | |
| 10 | amount | > 0 | `non_positive_amount` | |
| 11 | amount | amount converted to USD ≤ 500,000 | `amount_out_of_range` | |
| 12 | currency | not null | `missing_currency` | |
| 13 | currency | supported (in `ref_fx_rates`: USD, EUR, GBP, BDT, INR, CAD, AUD, SGD, AED) | `unsupported_currency` | |
| 14 | txn_status | not null | `missing_txn_status` | |
| 15 | txn_status | in SUCCESS, FAILED, PENDING, REFUNDED | `invalid_txn_status` | |
| 16 | txn_ts | not null | `missing_txn_ts` | |
| 17 | txn_ts | parses as `yyyy-MM-dd HH:mm:ss` | `invalid_txn_ts` | |
| 18 | txn_ts | not after now (non-future) | `future_txn_ts` | |
| D | txn_id | unique among valid rows. Keep rule: latest `txn_ts` (late-arriving update), then lowest `_row_hash`, then lowest `_bronze_row_id` | → `silver_transactions_duplicates` | |

## Processing precedence (why each row has exactly one outcome)

1. Every Bronze row is validated → it is either **invalid** (→ quarantine) or **valid**.
2. Only valid rows are deduplicated → each valid row is either the **winner** (→ accepted Silver) or a **loser** (→ duplicates table).
3. So the three outcomes are mutually exclusive and cover all rows: `bronze = accepted + quarantined + duplicate losers`. `silver_reconciliation` proves it with `_bronze_row_id` (overlap = 0, missing = 0).

Because validation runs **before** deduplication, an invalid copy of a key can never "win" and push out a valid one. A key whose only valid row is one of several copies still gets an accepted row.
