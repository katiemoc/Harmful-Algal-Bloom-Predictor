import pandas as pd

df = pd.read_csv("hab_ndbc_merged.csv")
print(f"Loaded: {df.shape[0]:,} rows × {df.shape[1]} cols")
print(f"\nNulls before:\n{df.isnull().sum()[df.isnull().sum() > 0]}")

# ── silicate_nitrate_ratio: sentinel 999 for nitrate-depleted water ───────────
n = df["silicate_nitrate_ratio"].isnull().sum()
df["silicate_nitrate_ratio"] = df["silicate_nitrate_ratio"].fillna(999)
print(f"\nsilicate_nitrate_ratio: filled {n} nulls with 999.")

# ── NDBC columns: per-station median, then global median ─────────────────────
ndbc_cols = [
    "wind_speed_mps", "wave_height_m", "dominant_period_s",
    "mean_wave_dir_deg", "atm_pressure_hpa", "air_temp_c", "sea_surface_temp_c",
]

for col in ndbc_cols:
    before = df[col].isnull().sum()
    # Per-station median
    df[col] = df.groupby("station")[col].transform(
        lambda x: x.fillna(x.median())
    )
    after_station = df[col].isnull().sum()
    # Global median for any still-null rows
    df[col] = df[col].fillna(df[col].median())
    after_global = df[col].isnull().sum()
    print(f"{col:<25} {before:>4} nulls → {before - after_station:>3} filled by station median"
          f", {after_station - after_global:>3} by global median")

# ── Confirm ───────────────────────────────────────────────────────────────────
remaining = df.isnull().sum().sum()
print(f"\nTotal nulls remaining: {remaining}")
assert remaining == 0, "Still has nulls — check above!"

df.to_csv("hab_ndbc_merged_cleaned.csv", index=False)
print(f"✓ Saved hab_ndbc_merged_cleaned.csv — {df.shape[0]:,} rows × {df.shape[1]} cols, zero nulls.")