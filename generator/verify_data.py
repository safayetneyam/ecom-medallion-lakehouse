#!/usr/bin/env python3
"""
Independent check of the generated CSVs against the Dataset contract.

It does NOT trust the generator's labels: it re-reads the raw files as strings and
re-detects every anomaly class from the values alone, then compares with
dirty_data_report.json.

Usage:  python verify_data.py --dir ./data_out
"""
import argparse
import csv
import json
import os
import re

import pandas as pd

ORDER_COLS = ["order_id", "customer_id", "order_date", "category", "quantity",
              "unit_price", "order_status", "country"]
TXN_COLS = ["txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts"]
NULL_TOKENS = {"", "null", "none", "n/a"}
AS_OF = pd.Timestamp("2026-11-15")


def count_rows(path):
    with open(path, newline="") as f:
        r = csv.reader(f)
        header = next(r)
        return header, sum(1 for _ in r)


def is_null(s):
    return s.str.strip().str.lower().isin(NULL_TOKENS)


def num(s):
    return pd.to_numeric(s.where(s.str.fullmatch(r"-?\d+(\.\d+)?")), errors="coerce")


def main(d):
    rep = json.load(open(os.path.join(d, "dirty_data_report.json")))
    ref = rep["reference_values"]
    ok = True

    def check(label, measured, expected):
        nonlocal ok
        flag = "OK " if measured == expected else "MISMATCH"
        ok &= measured == expected
        print(f"  [{flag}] {label:<36} measured={measured:>8,}  report={expected:>8,}")

    # ---------------- Contract: columns + exact row counts ----------------
    print("Contract checks")
    for fname, cols in (("orders.csv", ORDER_COLS), ("transactions.csv", TXN_COLS)):
        header, n = count_rows(os.path.join(d, fname))
        print(f"  {fname}: header={'OK' if header == cols else header}  data_rows={n:,}  "
              f"bytes={os.path.getsize(os.path.join(d, fname)):,}")
        ok &= header == cols and n == 1_000_000

    o = pd.read_csv(os.path.join(d, "orders.csv"), dtype=str, keep_default_na=False)
    t = pd.read_csv(os.path.join(d, "transactions.csv"), dtype=str, keep_default_na=False)
    o_inv = {r["anomaly"]: r["rows"] for r in rep["orders"]["inventory"]}
    t_inv = {r["anomaly"]: r["rows"] for r in rep["transactions"]["inventory"]}

    # ---------------- Orders ----------------
    print("\norders.csv anomaly re-detection")
    exact_dup = o.duplicated(keep="first")
    key_dup = o.duplicated("order_id", keep="first") & ~exact_dup
    check("exact_duplicate_row", int(exact_dup.sum()), o_inv["exact_duplicate_row"])
    check("duplicate_order_id_diff_content", int(key_dup.sum()), o_inv["duplicate_order_id_diff_content"])

    q = o["quantity"]
    qn = num(q)
    p = o["unit_price"]
    pn = num(p)
    od = pd.to_datetime(o["order_date"], format="%Y-%m-%d", errors="coerce")
    st = o["order_status"]
    allowed_st = set(ref["allowed_order_statuses"])
    check("null_customer_id", int(is_null(o["customer_id"]).sum()), o_inv["null_customer_id"])
    check("null_or_blank_order_status", int(is_null(st).sum()), o_inv["null_or_blank_order_status"])
    check("null_country", int(is_null(o["country"]).sum()), o_inv["null_country"])
    check("text_in_quantity", int(qn.isna().sum()), o_inv["text_in_quantity"])
    check("text_in_unit_price", int(pn.isna().sum()), o_inv["text_in_unit_price"])
    check("negative_quantity", int((qn < 0).sum()), o_inv["negative_quantity"])
    check("out_of_range_quantity", int(((qn == 0) | (qn > 50)).sum()), o_inv["out_of_range_quantity"])
    check("negative_unit_price", int((pn < 0).sum()), o_inv["negative_unit_price"])
    check("invalid_order_date", int(od.isna().sum()), o_inv["invalid_order_date"])
    check("future_order_date", int((od > AS_OF).sum()), o_inv["future_order_date"])
    bad_st = ~is_null(st) & ~st.str.strip().str.lower().isin(allowed_st)
    check("invalid_order_status", int(bad_st.sum()), o_inv["invalid_order_status"])
    messy = pd.Series(False, index=o.index)
    for col, allowed in (("category", ref["allowed_categories"]), ("order_status", ref["allowed_order_statuses"]),
                         ("country", ref["allowed_countries"])):
        v = o[col]
        norm_ok = v.str.strip().str.lower().isin({a.lower() for a in allowed})
        messy |= norm_ok & ~v.isin(allowed)
    check("inconsistent_case_whitespace", int(messy.sum()), o_inv["inconsistent_case_whitespace"])

    # ---------------- Transactions ----------------
    print("\ntransactions.csv anomaly re-detection")
    exact_dup = t.duplicated(keep="first")
    key_dup = t.duplicated("txn_id", keep="first") & ~exact_dup
    check("exact_duplicate_row", int(exact_dup.sum()), t_inv["exact_duplicate_row"])
    check("duplicate_txn_id_diff_content", int(key_dup.sum()), t_inv["duplicate_txn_id_diff_content"])

    order_ids = set(o["order_id"])
    oid_null = is_null(t["order_id"])
    check("null_order_id", int(oid_null.sum()), t_inv["null_order_id"])
    check("orphan_order_id", int((~oid_null & ~t["order_id"].isin(order_ids)).sum()), t_inv["orphan_order_id"])
    check("null_payment_method", int(is_null(t["payment_method"]).sum()), t_inv["null_payment_method"])
    a = t["amount"]
    an = num(a)
    check("null_amount", int(is_null(a).sum()), t_inv["null_amount"])
    check("text_in_amount", int((~is_null(a) & an.isna()).sum()), t_inv["text_in_amount"])
    check("negative_amount", int((an < 0).sum()), t_inv["negative_amount"])
    cur = t["currency"].str.strip().str.upper()
    fx = cur.map(ref["fx_to_usd"])
    check("out_of_range_amount", int(((an * fx) > ref["max_txn_amount"]).sum()), t_inv["out_of_range_amount"])
    check("unsupported_currency", int(fx.isna().sum()), t_inv["unsupported_currency"])
    ts_raw = t["txn_status"].str.strip().str.upper()
    check("invalid_txn_status", int((~ts_raw.isin(ref["allowed_txn_statuses"])).sum()), t_inv["invalid_txn_status"])
    tts = pd.to_datetime(t["txn_ts"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    check("invalid_txn_ts", int(tts.isna().sum()), t_inv["invalid_txn_ts"])
    check("future_txn_ts", int((tts > AS_OF).sum()), t_inv["future_txn_ts"])
    messy = pd.Series(False, index=t.index)
    for col, allowed in (("payment_method", ref["allowed_payment_methods"]),
                         ("currency", ref["supported_currencies"]),
                         ("txn_status", ref["allowed_txn_statuses"])):
        v = t[col]
        norm_ok = v.str.strip().str.lower().isin({x.lower() for x in allowed})
        messy |= norm_ok & ~v.isin(allowed)
    check("inconsistent_case_whitespace", int(messy.sum()), t_inv["inconsistent_case_whitespace"])

    # Derived (not injected): valid txns pointing at an order row that will be quarantined
    print("\nAll checks passed." if ok else "\nSome checks FAILED.")
    return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="./data_out")
    raise SystemExit(0 if main(ap.parse_args().dir) else 1)
