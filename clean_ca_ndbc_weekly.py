"""
Clean ca_ndbc_weekly.csv
========================
Steps:
  1. Parse dates, coerce bad values to NaN
  2. Drop rows where ALL numeric columns are null (no measurements at all)
  3. Drop dewpoint_c (not a HAB driver, ~48% null)
  4. Drop columns that are >80% null
  5. Drop exact duplicates
  6. Drop future rows (past today's date)
  7. Drop rows with any remaining nulls (HAB features must be complete)
  8. Round numeric columns to 3 decimal places
  9. Save to ca_ndbc_weekly_clean.csv and print a summary

Usage:
    python clean_ca_ndbc_weekly.py
"""

import pandas as pd
from datetime import date

INPUT  = "ca_ndbc_weekly.csv"
OUTPUT = "ca_ndbc_weekly_clean.csv"

# ── Load ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(INPUT, parse_dates=["datetime_utc"])
print(f"Loaded: {df.shape[0]:,} rows × {df.shape[1]} cols")

# ── Report initial null counts ────────────────────────────────────────────────
numeric_cols = df.select_dtypes(include="number").columns.tolist()
print("\nNull counts per column (before cleaning):")
for col in df.columns:
    n = df[col].isnull().sum()
    pct = 100 * n / len(df)
    print(f"  {col:<25} {n:>5} ({pct:.1f}%)")

# ── Drop rows where every numeric measurement is null ─────────────────────────
# Exclude station_id — it's numeric but an identifier, never a measurement
measurement_cols = [c for c in numeric_cols if c != "station_id"]
all_null_mask = df[measurement_cols].isnull().all(axis=1)
n_all_null = all_null_mask.sum()
df = df[~all_null_mask].copy()
print(f"\nDropped {n_all_null:,} rows with no numeric data at all.")

# ── Drop dewpoint_c (not a HAB driver, ~48% null) ────────────────────────────
if "dewpoint_c" in df.columns:
    df.drop(columns=["dewpoint_c"], inplace=True)
    print("Dropped column 'dewpoint_c' (not a HAB driver, ~48% null).")

# ── Drop any remaining columns that are >80% null ────────────────────────────
dropped_cols = []
for col in df.columns:
    pct_null = df[col].isnull().mean()
    if pct_null > 0.80:
        dropped_cols.append((col, f"{100*pct_null:.1f}% null"))
        df.drop(columns=[col], inplace=True)

if dropped_cols:
    for col, reason in dropped_cols:
        print(f"Dropped column '{col}' ({reason}).")
else:
    print("No additional columns dropped (none exceeded 80% null).")

# ── Drop exact duplicates ─────────────────────────────────────────────────────
n_dupes = df.duplicated(subset=["station_id", "datetime_utc"]).sum()
df.drop_duplicates(subset=["station_id", "datetime_utc"], inplace=True)
print(f"Dropped {n_dupes:,} duplicate (station_id, datetime_utc) rows.")

# ── Drop rows with dates in the future ────────────────────────────────────────
today = pd.Timestamp(date.today())
future_mask = df["datetime_utc"] > today
n_future = future_mask.sum()
df = df[~future_mask].copy()
print(f"Dropped {n_future:,} rows with future dates (after {today.date()}).")

# ── Drop rows with any remaining nulls ───────────────────────────────────────
before = len(df)
df.dropna(inplace=True)
print(f"Dropped {before - len(df):,} rows with any remaining null values.")

# ── Re-round numeric columns ──────────────────────────────────────────────────
numeric_cols = df.select_dtypes(include="number").columns
df[numeric_cols] = df[numeric_cols].round(3)

# ── Sort and reset index ──────────────────────────────────────────────────────
df.sort_values(["station_id", "datetime_utc"], inplace=True)
df.reset_index(drop=True, inplace=True)

# ── Save ──────────────────────────────────────────────────────────────────────
df.to_csv(OUTPUT, index=False)

print(f"\n✓ Saved {len(df):,} rows × {df.shape[1]} cols → {OUTPUT}")
print(f"  Date range : {df['datetime_utc'].min().date()} → {df['datetime_utc'].max().date()}")
print(f"  Stations   : {df['station_id'].nunique()} ({sorted(df['station_id'].unique().tolist())})")
print(f"  Columns    : {list(df.columns)}")
print("\nNull counts per column (after cleaning):")
for col in df.columns:
    n = df[col].isnull().sum()
    pct = 100 * n / len(df)
    print(f"  {col:<25} {n:>5} ({pct:.1f}%)")
