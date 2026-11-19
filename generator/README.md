# generator/

Seeded, reproducible generator for the two raw source extracts in the Dataset contract.

| File | Purpose |
|---|---|
| `generate_data.py` | Builds `orders.csv` and `transactions.csv` (exactly 1,000,000 data rows each, plus a header) and `fx_rates.csv`. Seed 42. |
| `verify_data.py` | Re-reads the CSVs as plain strings and re-detects every anomaly class from the values alone, then compares with the report. |
| `make_report_md.py` | Renders `dirty_data_report.json` into `dirty_data_report.md`. |
| `dirty_data_report.md` / `.json` | Dirty-data inventory: type, field, exact row count, % of file and expected Silver outcome. |

```bash
python generator/generate_data.py  --out ./data_out   # about 15 s, writes about 150 MB to data_out/ (git-ignored)
python generator/verify_data.py    --dir ./data_out   # should end with "All checks passed."
python generator/make_report_md.py --dir ./data_out
```

The CSVs are **not** committed (see `.gitignore`). Upload them to
`/Volumes/<catalog>/ecom_poc/raw_files/` via **New → Add or upload data → Upload files to a volume**.
Requires Python 3.9+ with `numpy` and `pandas`.
