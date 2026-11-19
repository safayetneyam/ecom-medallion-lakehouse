#!/usr/bin/env python3
"""
Synthetic e-commerce source generator for the Databricks Medallion assignment.

Produces two raw CSV extracts that satisfy the "Dataset contract":

  orders.csv        order_id, customer_id, order_date, category, quantity,
                    unit_price, order_status, country
  transactions.csv  txn_id, order_id, payment_method, amount, currency,
                    txn_status, txn_ts

Each file has EXACTLY 1,000,000 data rows + 1 header row.

Everything is driven by one fixed seed, so re-running with the same arguments
produces byte-identical files (verify with the SHA-256 values in the report).

Dirty data is injected on purpose and tracked exactly: every data row carries
at most ONE injected anomaly, so the counts in dirty_data_report.json are exact
and add up. All data is synthetic - no real customers, cards or accounts.

Usage:
    python generate_data.py --out ./data_out            # defaults: seed 42, 1M rows
    python generate_data.py --out ./data_out --rows 10000   # quick test
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------------------
# Fixed reference values (documented in the report so Silver rules can use them)
# --------------------------------------------------------------------------------------
SEED = 42
N_ROWS = 1_000_000
AS_OF = np.datetime64("2026-11-15T00:00:00")        # "now" for the dataset; later = future
ORDER_START = np.datetime64("2025-01-01")
ORDER_END = np.datetime64("2026-10-31")             # inclusive last clean order date
N_CUSTOMERS = 150_000

CATEGORIES = {  # name: (weight, median_usd_price, sigma)
    "Electronics":    (0.18, 180.0, 0.70),
    "Fashion":        (0.20, 35.0, 0.55),
    "Home & Kitchen": (0.15, 45.0, 0.60),
    "Beauty":         (0.12, 18.0, 0.50),
    "Sports":         (0.09, 40.0, 0.60),
    "Books":          (0.08, 14.0, 0.40),
    "Toys":           (0.08, 22.0, 0.50),
    "Grocery":        (0.10, 8.0, 0.45),
}

COUNTRIES = {  # name: (weight, currency)
    "Bangladesh":     (0.22, "BDT"),
    "India":          (0.18, "INR"),
    "United States":  (0.17, "USD"),
    "United Kingdom": (0.09, "GBP"),
    "Germany":        (0.08, "EUR"),
    "France":         (0.06, "EUR"),
    "Canada":         (0.06, "CAD"),
    "Australia":      (0.05, "AUD"),
    "Singapore":      (0.04, "SGD"),
    "United Arab Emirates": (0.05, "AED"),
}

# Static FX table: 1 unit of currency = X USD (fixed for reproducibility; written to fx_rates.csv)
FX_TO_USD = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "BDT": 0.0084, "INR": 0.012,
    "CAD": 0.73, "AUD": 0.66, "SGD": 0.74, "AED": 0.2723,
}

ORDER_STATUSES = {  # status: weight
    "delivered": 0.55, "shipped": 0.13, "processing": 0.06,
    "pending": 0.06, "cancelled": 0.12, "returned": 0.08,
}

PAYMENT_METHODS = {  # method: (weight, failure_probability on first attempt)
    "credit_card":      (0.30, 0.06),
    "debit_card":       (0.18, 0.08),
    "mobile_wallet":    (0.20, 0.05),
    "paypal":           (0.10, 0.04),
    "bank_transfer":    (0.08, 0.15),
    "cash_on_delivery": (0.14, 0.10),
}

TXN_STATUSES_CLEAN = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]

# Business-rule limits the generator respects for CLEAN rows (Silver can enforce the same)
QTY_MIN, QTY_MAX = 1, 50
PRICE_MIN, PRICE_MAX = 0.50, 10_000.00
AMOUNT_MAX_USD = 500_000.00

# --------------------------------------------------------------------------------------
# Dirty-data plan: (anomaly_name, field, fraction_of_rows, expected Silver outcome)
# Each row gets at most one anomaly -> counts are exact and disjoint.
# --------------------------------------------------------------------------------------
ORDER_ANOMALIES = [
    ("null_customer_id",          "customer_id",  0.0080, "quarantine"),
    ("null_or_blank_order_status", "order_status", 0.0050, "quarantine"),
    ("null_country",              "country",      0.0030, "quarantine"),
    ("text_in_quantity",          "quantity",     0.0060, "quarantine"),
    ("text_in_unit_price",        "unit_price",   0.0060, "quarantine"),
    ("negative_quantity",         "quantity",     0.0050, "quarantine"),
    ("out_of_range_quantity",     "quantity",     0.0030, "quarantine"),
    ("negative_unit_price",       "unit_price",   0.0040, "quarantine"),
    ("invalid_order_date",        "order_date",   0.0050, "quarantine"),
    ("future_order_date",         "order_date",   0.0040, "quarantine"),
    ("invalid_order_status",      "order_status", 0.0030, "quarantine"),
    ("inconsistent_case_whitespace", "category/order_status/country", 0.0300, "standardise & accept"),
    ("exact_duplicate_row",       "all columns",  0.0050, "duplicate loser"),
    ("duplicate_order_id_diff_content", "order_id", 0.0050, "duplicate loser (one of the pair)"),
]

TXN_ANOMALIES = [
    ("null_order_id",             "order_id",       0.0030, "quarantine"),
    ("orphan_order_id",           "order_id",       0.0100, "quarantine"),
    ("null_payment_method",       "payment_method", 0.0040, "quarantine"),
    ("null_amount",               "amount",         0.0050, "quarantine"),
    ("text_in_amount",            "amount",         0.0050, "quarantine"),
    ("negative_amount",           "amount",         0.0040, "quarantine"),
    ("out_of_range_amount",       "amount",         0.0020, "quarantine"),
    ("unsupported_currency",      "currency",       0.0040, "quarantine"),
    ("invalid_txn_status",        "txn_status",     0.0030, "quarantine"),
    ("invalid_txn_ts",            "txn_ts",         0.0050, "quarantine"),
    ("future_txn_ts",             "txn_ts",         0.0040, "quarantine"),
    ("inconsistent_case_whitespace", "payment_method/currency/txn_status", 0.0300, "standardise & accept"),
    ("exact_duplicate_row",       "all columns",    0.0050, "duplicate loser"),
    ("duplicate_txn_id_diff_content", "txn_id",     0.0050, "duplicate loser (one of the pair)"),
]


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def weighted_choice(rng, mapping_keys, weights, n):
    w = np.asarray(weights, dtype=float)
    return rng.choice(np.asarray(mapping_keys, dtype=object), size=n, p=w / w.sum())


def fmt_money(x: np.ndarray) -> np.ndarray:
    return np.char.mod("%.2f", x).astype(object)


def messy_case(rng, values: np.ndarray) -> np.ndarray:
    """Return the same labels with inconsistent case and stray whitespace."""
    out = values.astype(object).copy()
    style = rng.integers(0, 5, size=len(out))
    for i, (v, s) in enumerate(zip(out, style)):
        if s == 0:
            out[i] = v.upper()
        elif s == 1:
            out[i] = v.lower()
        elif s == 2:
            out[i] = "  " + v + " "
        elif s == 3:
            out[i] = " " + v.swapcase()
        else:
            out[i] = v.title() + "  "
        if out[i] == v:                      # e.g. "delivered".lower() - guarantee a visible change
            out[i] = " " + v.upper() + " " if v != v.upper() else " " + v.lower() + "  "
    return out


def assign_anomalies(rng, n_rows, plan, eligible_for_dupes_mask=None):
    """
    Assign each row at most one anomaly. Duplicate targets are drawn only from rows in
    eligible_for_dupes_mask (if given). Returns an array of anomaly labels ('' = clean).
    """
    labels = np.full(n_rows, "", dtype=object)
    free = np.ones(n_rows, dtype=bool)
    # Duplicates first so they respect the eligibility mask
    ordered = sorted(plan, key=lambda p: 0 if "duplicate" in p[0] else 1)
    for name, _field, frac, _outcome in ordered:
        k = int(round(frac * n_rows))
        pool = free.copy()
        if "duplicate" in name and eligible_for_dupes_mask is not None:
            pool &= eligible_for_dupes_mask
        idx = np.flatnonzero(pool)
        if len(idx) < k:
            raise RuntimeError(f"Not enough rows to inject {name}")
        chosen = rng.choice(idx, size=k, replace=False)
        labels[chosen] = name
        free[chosen] = False
    return labels


def pick_sources(rng, labels, k):
    """Pick k distinct CLEAN rows to copy from (for duplicate injection)."""
    clean_idx = np.flatnonzero(labels == "")
    return rng.choice(clean_idx, size=k, replace=False)


def file_stats(path):
    h = hashlib.sha256()
    lines = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            lines += chunk.count(b"\n")
    return {"bytes": os.path.getsize(path), "sha256": h.hexdigest(), "physical_lines": lines}


# --------------------------------------------------------------------------------------
# Generation
# --------------------------------------------------------------------------------------
def generate(out_dir: str, n_rows: int, seed: int):
    rng = np.random.default_rng(seed)
    t0 = time.time()

    # ---------------- Clean order base ----------------
    n = n_rows
    order_ids = np.array([f"ORD-{i:07d}" for i in range(1, n + 1)], dtype=object)

    # Skewed customers so "top customers by spend" is meaningful (Zipf-like weights)
    cust_rank_w = 1.0 / np.arange(1, N_CUSTOMERS + 1) ** 0.6
    cust_idx = rng.choice(N_CUSTOMERS, size=n, p=cust_rank_w / cust_rank_w.sum())
    cust_perm = rng.permutation(N_CUSTOMERS)              # so rank != id order
    customer_ids = np.array([f"CUST-{cust_perm[c] + 1:06d}" for c in cust_idx], dtype=object)

    days_span = int((ORDER_END - ORDER_START).astype(int)) + 1
    # Mild growth trend + weekly seasonality
    day_w = np.linspace(1.0, 1.6, days_span)
    dow = (np.arange(days_span) + 2) % 7                  # 2025-01-01 was a Wednesday
    day_w *= np.where(dow >= 5, 1.25, 1.0)
    day_offsets = rng.choice(days_span, size=n, p=day_w / day_w.sum())
    order_dates = ORDER_START + day_offsets.astype("timedelta64[D]")

    cat_names = list(CATEGORIES)
    categories = weighted_choice(rng, cat_names, [CATEGORIES[c][0] for c in cat_names], n)
    med = np.array([CATEGORIES[c][1] for c in categories])
    sig = np.array([CATEGORIES[c][2] for c in categories])
    unit_price = np.clip(np.round(med * np.exp(rng.normal(0, sig)), 2), PRICE_MIN, 5_000.0)

    quantity = np.clip(rng.geometric(0.55, size=n), QTY_MIN, 12)

    st_names = list(ORDER_STATUSES)
    order_status = weighted_choice(rng, st_names, [ORDER_STATUSES[s] for s in st_names], n)
    # Recent orders are more likely still in-flight
    recent = order_dates >= (ORDER_END - np.timedelta64(10, "D"))
    flip = recent & (rng.random(n) < 0.5)
    order_status[flip] = rng.choice(np.array(["processing", "pending", "shipped"], dtype=object),
                                    size=flip.sum())

    ctry_names = list(COUNTRIES)
    countries = weighted_choice(rng, ctry_names, [COUNTRIES[c][0] for c in ctry_names], n)

    order_total_usd = np.round(unit_price * quantity, 2)

    # ---------------- Decide transactions per order ----------------
    # 0 txns: pending (no payment yet) and part of cancelled
    # 1 txn : normal payment
    # 2 txns: retry (FAILED then SUCCESS), split payment (2 x SUCCESS), or refund (SUCCESS + REFUNDED)
    pm_names = list(PAYMENT_METHODS)
    pay_method = weighted_choice(rng, pm_names, [PAYMENT_METHODS[m][0] for m in pm_names], n)
    fail_p = np.array([PAYMENT_METHODS[m][1] for m in pay_method])

    kind = np.full(n, "single", dtype=object)
    r = rng.random(n)
    kind[(order_status == "pending") & (r < 0.85)] = "none"
    kind[(order_status == "cancelled") & (r < 0.55)] = "none"
    kind[(order_status == "cancelled") & (r >= 0.55) & (r < 0.80)] = "single_failed"
    kind[(order_status == "cancelled") & (r >= 0.80)] = "refund"
    kind[order_status == "returned"] = "refund"
    paid_ok = np.isin(order_status, ["delivered", "shipped", "processing"])
    retry = paid_ok & (rng.random(n) < fail_p)
    kind[retry] = "retry"
    split = paid_ok & ~retry & (pay_method != "cash_on_delivery") & (rng.random(n) < 0.015)
    kind[split] = "split"
    kind[(order_status == "pending") & (kind == "single")] = "single_pending"

    n_txn = np.where(kind == "none", 0, np.where(np.isin(kind, ["retry", "split", "refund"]), 2, 1))
    total = int(n_txn.sum())
    # Adjust to EXACTLY n_rows transactions by converting singles <-> retries
    diff = n_rows - total
    if diff > 0:
        cand = np.flatnonzero((kind == "single") & paid_ok)
        pick = rng.choice(cand, size=diff, replace=False)
        kind[pick] = "retry"
        n_txn[pick] = 2
    elif diff < 0:
        # Trim excess from cancelled orders first (keeps retry/failure signal per payment method)
        excess = -diff
        canc_refund = np.flatnonzero((order_status == "cancelled") & (kind == "refund"))
        k2 = min(len(canc_refund), excess // 2)
        pick = rng.choice(canc_refund, size=k2, replace=False)
        kind[pick] = "none"
        n_txn[pick] = 0
        excess -= 2 * k2
        canc_failed = np.flatnonzero((order_status == "cancelled") & (kind == "single_failed"))
        k1 = min(len(canc_failed), excess)
        pick = rng.choice(canc_failed, size=k1, replace=False)
        kind[pick] = "none"
        n_txn[pick] = 0
        excess -= k1
        if excess:
            pick = rng.choice(np.flatnonzero(kind == "retry"), size=excess, replace=False)
            kind[pick] = "single"
            n_txn[pick] = 1
    assert int(n_txn.sum()) == n_rows

    # ---------------- Build clean transactions ----------------
    o_idx = np.repeat(np.arange(n), n_txn)
    seq = np.concatenate([np.arange(k) for k in n_txn if k > 0]) if n_rows else np.array([], int)
    k_of = kind[o_idx]

    currency = np.array([COUNTRIES[c][1] for c in countries[o_idx]], dtype=object)
    # ~20% of non-USD buyers pay in USD (international cards)
    usd_override = (currency != "USD") & (rng.random(n_rows) < 0.20)
    currency[usd_override] = "USD"
    fx = np.array([FX_TO_USD[c] for c in currency])

    amount_usd = order_total_usd[o_idx].copy()
    amount_usd[k_of == "split"] = np.round(amount_usd[k_of == "split"] / 2, 2)
    amount = np.round(amount_usd / fx, 2)

    t_status = np.full(n_rows, "SUCCESS", dtype=object)
    t_status[(k_of == "retry") & (seq == 0)] = "FAILED"
    t_status[k_of == "single_failed"] = "FAILED"
    t_status[k_of == "single_pending"] = "PENDING"
    t_status[(k_of == "refund") & (seq == 1)] = "REFUNDED"

    base_ts = order_dates[o_idx].astype("datetime64[s]") + rng.integers(
        0, 86_400, size=n_rows).astype("timedelta64[s]")
    offs = np.zeros(n_rows, dtype="int64")
    offs[seq == 1] = rng.integers(60, 3_600, size=int((seq == 1).sum()))          # retry / split
    refund2 = (k_of == "refund") & (seq == 1)
    offs[refund2] = rng.integers(2 * 86_400, 20 * 86_400, size=int(refund2.sum()))  # refund later
    txn_ts = base_ts + offs.astype("timedelta64[s]")
    txn_ts = np.minimum(txn_ts, AS_OF - np.timedelta64(3600, "s"))

    t_method = pay_method[o_idx]
    t_order_id = order_ids[o_idx]

    # Shuffle transaction rows (extracts are not ordered by order) and assign ids
    perm = rng.permutation(n_rows)
    t_order_id, t_method, amount, currency, t_status, txn_ts, t_order_pos = (
        t_order_id[perm], t_method[perm], amount[perm], currency[perm], t_status[perm],
        txn_ts[perm], o_idx[perm])
    txn_ids = np.array([f"TXN-{i:08d}" for i in rng.permutation(np.arange(10_000_001, 10_000_001 + n_rows))],
                       dtype=object)

    # ---------------- Inject anomalies: ORDERS ----------------
    # Duplicate targets must be orders with no transactions, otherwise overwriting their
    # order_id would silently create extra orphan transactions.
    no_txn_mask = n_txn == 0
    o_labels = assign_anomalies(rng, n, ORDER_ANOMALIES, eligible_for_dupes_mask=no_txn_mask)

    o = {
        "order_id": order_ids.copy(),
        "customer_id": customer_ids,
        "order_date": np.datetime_as_string(order_dates, unit="D").astype(object),
        "category": categories.astype(object),
        "quantity": quantity.astype(str).astype(object),
        "unit_price": fmt_money(unit_price),
        "order_status": order_status.astype(object),
        "country": countries.astype(object),
    }

    def L(name):
        return np.flatnonzero(o_labels == name)

    i = L("null_customer_id")
    o["customer_id"][i] = rng.choice(np.array(["", "NULL", "null", " "], dtype=object), size=len(i))
    i = L("null_or_blank_order_status")
    o["order_status"][i] = rng.choice(np.array(["", " ", "NULL", "None"], dtype=object), size=len(i))
    i = L("null_country")
    o["country"][i] = rng.choice(np.array(["", "N/A", "null"], dtype=object), size=len(i))
    i = L("text_in_quantity")
    o["quantity"][i] = rng.choice(np.array(["two", "abc", "1O", "3 pcs", "#", "five"], dtype=object), size=len(i))
    i = L("text_in_unit_price")
    o["unit_price"][i] = rng.choice(np.array(["N/A", "free", "$19.99", "12.5USD", "1,299.00", "TBD"], dtype=object),
                                    size=len(i))
    i = L("negative_quantity")
    o["quantity"][i] = (-rng.integers(1, 10, size=len(i))).astype(str).astype(object)
    i = L("out_of_range_quantity")
    o["quantity"][i] = rng.choice(np.array([0, 500, 999, 5000, 10000]), size=len(i)).astype(str).astype(object)
    i = L("negative_unit_price")
    o["unit_price"][i] = fmt_money(-np.round(rng.uniform(1, 500, size=len(i)), 2))
    i = L("invalid_order_date")
    o["order_date"][i] = rng.choice(np.array(["2025-13-40", "2026-02-30", "not a date", "31/12/2025",
                                              "00-00-0000", "2025/07/??"], dtype=object), size=len(i))
    i = L("future_order_date")
    fut = np.datetime64("2027-01-01") + rng.integers(0, 4 * 365, size=len(i)).astype("timedelta64[D]")
    o["order_date"][i] = np.datetime_as_string(fut, unit="D").astype(object)
    i = L("invalid_order_status")
    o["order_status"][i] = rng.choice(np.array(["unknown", "lost_in_transit", "teleported", "???"], dtype=object),
                                      size=len(i))
    i = L("inconsistent_case_whitespace")
    which = rng.integers(0, 3, size=len(i))
    for col, w in (("category", 0), ("order_status", 1), ("country", 2)):
        j = i[which == w]
        o[col][j] = messy_case(rng, o[col][j])

    # Exact duplicate rows: copy every column from a clean source row
    i = L("exact_duplicate_row")
    src_exact = pick_sources(rng, o_labels, len(i))
    for col in o:
        o[col][i] = o[col][src_exact]
    # Same order_id, different content (e.g. a later status update re-extracted)
    i = L("duplicate_order_id_diff_content")
    used = np.zeros(n, dtype=bool)
    used[src_exact] = True
    clean_left = np.flatnonzero((o_labels == "") & ~used)
    src_key = rng.choice(clean_left, size=len(i), replace=False)
    for col in o:
        o[col][i] = o[col][src_key]
    status_alt = {"processing": "shipped", "shipped": "delivered", "pending": "processing",
                  "delivered": "returned", "cancelled": "pending", "returned": "delivered"}
    o["order_status"][i] = np.array([status_alt[s] for s in o["order_status"][i]], dtype=object)
    o["quantity"][i] = (quantity[src_key] + rng.integers(1, 3, size=len(i))).astype(str).astype(object)

    # ---------------- Inject anomalies: TRANSACTIONS ----------------
    t_labels = assign_anomalies(rng, n_rows, TXN_ANOMALIES)
    t = {
        "txn_id": txn_ids,
        "order_id": t_order_id.astype(object),
        "payment_method": t_method.astype(object),
        "amount": fmt_money(amount),
        "currency": currency.astype(object),
        "txn_status": t_status.astype(object),
        "txn_ts": np.datetime_as_string(txn_ts, unit="s").astype(object),
    }
    t["txn_ts"] = np.char.replace(t["txn_ts"].astype(str), "T", " ").astype(object)

    def TL(name):
        return np.flatnonzero(t_labels == name)

    i = TL("null_order_id")
    t["order_id"][i] = rng.choice(np.array(["", "NULL", " "], dtype=object), size=len(i))
    i = TL("orphan_order_id")
    t["order_id"][i] = np.array([f"ORD-{x:07d}" for x in rng.integers(n + 1_000_000, n + 9_000_000, size=len(i))],
                                dtype=object)
    i = TL("null_payment_method")
    t["payment_method"][i] = rng.choice(np.array(["", "NULL", "n/a"], dtype=object), size=len(i))
    i = TL("null_amount")
    t["amount"][i] = rng.choice(np.array(["", "NULL", " "], dtype=object), size=len(i))
    i = TL("text_in_amount")
    t["amount"][i] = rng.choice(np.array(["abc", "ten", "1,250.00", "USD 40", "--", "12.3.4"], dtype=object),
                                size=len(i))
    i = TL("negative_amount")
    t["amount"][i] = fmt_money(-np.round(rng.uniform(1, 2000, size=len(i)), 2))
    i = TL("out_of_range_amount")
    t["amount"][i] = fmt_money(np.round(rng.uniform(5e8, 9e9, size=len(i)), 2))
    i = TL("unsupported_currency")
    t["currency"][i] = rng.choice(np.array(["XYZ", "BTC", "US$", "TK", "EURO"], dtype=object), size=len(i))
    i = TL("invalid_txn_status")
    t["txn_status"][i] = rng.choice(np.array(["UNKNOWN", "OK?", "CHARGEBACK_MAYBE", "ERR"], dtype=object),
                                    size=len(i))
    i = TL("invalid_txn_ts")
    t["txn_ts"][i] = rng.choice(np.array(["2026-02-30 10:00:00", "2025-11-31 25:61:00", "yesterday",
                                          "13/45/2025 10:00", "0000-00-00 00:00:00", ""], dtype=object),
                                size=len(i))
    i = TL("future_txn_ts")
    fut = np.datetime64("2027-01-01T00:00:00") + rng.integers(0, 3 * 365 * 86400, size=len(i)).astype(
        "timedelta64[s]")
    t["txn_ts"][i] = np.char.replace(np.datetime_as_string(fut, unit="s"), "T", " ").astype(object)
    i = TL("inconsistent_case_whitespace")
    which = rng.integers(0, 3, size=len(i))
    for col, w in (("payment_method", 0), ("currency", 1), ("txn_status", 2)):
        j = i[which == w]
        t[col][j] = messy_case(rng, t[col][j])

    i = TL("exact_duplicate_row")
    src_exact_t = pick_sources(rng, t_labels, len(i))
    for col in t:
        t[col][i] = t[col][src_exact_t]
    i = TL("duplicate_txn_id_diff_content")
    used = np.zeros(n_rows, dtype=bool)
    used[src_exact_t] = True
    clean_left = np.flatnonzero((t_labels == "") & ~used)
    src_key_t = rng.choice(clean_left, size=len(i), replace=False)
    for col in t:
        t[col][i] = t[col][src_key_t]
    # Re-delivered with a status change and a slightly later timestamp (late-arriving update)
    st_alt = {"PENDING": "SUCCESS", "FAILED": "SUCCESS", "SUCCESS": "REFUNDED", "REFUNDED": "SUCCESS"}
    t["txn_status"][i] = np.array([st_alt[s] for s in t["txn_status"][i]], dtype=object)
    later = pd.to_datetime(pd.Series(t["txn_ts"][src_key_t])) + pd.to_timedelta(
        rng.integers(60, 7200, size=len(i)), unit="s")
    later = later.where(later < pd.Timestamp(str(AS_OF)), pd.Timestamp(str(AS_OF)) - pd.Timedelta(seconds=1))
    t["txn_ts"][i] = later.dt.strftime("%Y-%m-%d %H:%M:%S").to_numpy(dtype=object)

    # ---------------- Write files ----------------
    os.makedirs(out_dir, exist_ok=True)
    orders_path = os.path.join(out_dir, "orders.csv")
    txns_path = os.path.join(out_dir, "transactions.csv")
    pd.DataFrame(o).to_csv(orders_path, index=False, lineterminator="\n")
    pd.DataFrame(t).to_csv(txns_path, index=False, lineterminator="\n")
    pd.DataFrame({"currency": list(FX_TO_USD), "usd_per_unit": list(FX_TO_USD.values())}).to_csv(
        os.path.join(out_dir, "fx_rates.csv"), index=False, lineterminator="\n")

    # ---------------- Report ----------------
    def inventory(plan, labels, total_rows):
        rows = []
        for name, field, frac, outcome in plan:
            cnt = int((labels == name).sum())
            rows.append({"anomaly": name, "field": field, "rows": cnt,
                         "pct": round(100.0 * cnt / total_rows, 3), "expected_silver_outcome": outcome})
        clean = int((labels == "").sum())
        return rows, clean

    o_inv, o_clean = inventory(ORDER_ANOMALIES, o_labels, n)
    t_inv, t_clean = inventory(TXN_ANOMALIES, t_labels, n_rows)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seed": seed,
        "dataset_as_of": str(AS_OF),
        "clean_order_date_range": [str(ORDER_START), str(ORDER_END)],
        "files": {
            "orders.csv": {"data_rows": n, **file_stats(orders_path)},
            "transactions.csv": {"data_rows": n_rows, **file_stats(txns_path)},
        },
        "orders": {"untouched_rows": o_clean, "anomaly_rows": n - o_clean, "inventory": o_inv},
        "transactions": {"untouched_rows": t_clean, "anomaly_rows": n_rows - t_clean, "inventory": t_inv},
        "transaction_patterns_per_order": {k: int((kind == k).sum()) for k in
                                           ["none", "single", "single_failed", "single_pending",
                                            "retry", "split", "refund"]},
        "reference_values": {
            "allowed_categories": cat_names,
            "allowed_order_statuses": st_names,
            "allowed_countries": ctry_names,
            "allowed_payment_methods": pm_names,
            "allowed_txn_statuses": TXN_STATUSES_CLEAN,
            "supported_currencies": list(FX_TO_USD),
            "quantity_range": [QTY_MIN, QTY_MAX],
            "unit_price_range_usd": [PRICE_MIN, PRICE_MAX],
            "max_txn_amount": AMOUNT_MAX_USD,
            "fx_to_usd": FX_TO_USD,
            "order_unit_price_currency": "USD",
        },
        "runtime_seconds": round(time.time() - t0, 1),
    }
    with open(os.path.join(out_dir, "dirty_data_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="./data_out")
    ap.add_argument("--rows", type=int, default=N_ROWS)
    ap.add_argument("--seed", type=int, default=SEED)
    args = ap.parse_args()
    rep = generate(args.out, args.rows, args.seed)
    for fname, s in rep["files"].items():
        print(f"{fname}: {s['data_rows']:,} data rows, {s['bytes']:,} bytes, sha256={s['sha256'][:16]}...")
    print(f"done in {rep['runtime_seconds']}s -> {os.path.abspath(args.out)}")
