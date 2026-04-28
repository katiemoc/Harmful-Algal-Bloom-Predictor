import pandas as pd

KEEP = [
    "station_id", "datetime_utc",
    "wind_speed_mps", "wave_height_m", "dominant_period_s",
    "atm_pressure_hpa", "air_temp_c", "sea_surface_temp_c", "mean_wave_dir_deg",
]
FEATURE_COLS = [c for c in KEEP if c not in ("station_id", "datetime_utc")]

df = pd.read_csv("ca_ndbc_weekly.csv", parse_dates=["datetime_utc"], usecols=KEEP)

before = len(df)
df.dropna(subset=FEATURE_COLS, how="all", inplace=True)
print(f"Dropped {before - len(df):,} rows where all feature columns were NaN.")

df.sort_values(["station_id", "datetime_utc"], inplace=True)
df.reset_index(drop=True, inplace=True)

df.to_csv("ca_ndbc_final.csv", index=False)

print(f"\nShape: {df.shape}")
print(df.head())