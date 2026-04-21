"""
Download NDBC California Coastal Buoy Data → Weekly CSV
=========================================================
- Stations: CA coastline only (verified)
- Time range: January 2017 – April 2026
- Resolution: Weekly averages (resampled from hourly)
- Sources: Historical archive (2017–2025) + realtime feed (2026)

Usage:
    pip install requests pandas
    python download_ca_ndbc_v2.py

Output:
    ca_ndbc_weekly.csv
"""

import gzip
import io
import time
import requests
import pandas as pd

# ── Verified California-only coastal stations ─────────────────────────────────
CA_STATIONS = {
    "46025": "Santa Monica Basin",
    "46053": "East Santa Barbara Channel",
    "46054": "West Santa Barbara Channel",
    "46011": "Santa Maria",
    "46069": "South Santa Barbara Channel",
    "46086": "San Clemente Basin",
    "46047": "Tanner Bank",
    "46026": "San Francisco",
    "46013": "Bodega Bay",
    "46014": "Point Arena",
    "46012": "Half Moon Bay",
    "46042": "Monterey",
    "46028": "Point Conception",
    "46059": "Point Reyes",
    "46022": "Eel River",
    "46027": "St. George (NorCal)",
    "46029": "Point Arena Offshore",
    "46219": "Harvest Platform",
}

HIST_URL  = "https://www.ndbc.noaa.gov/data/historical/stdmet/{station}h{year}.txt.gz"
RT_URL    = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
START_YR  = 2017
END_YR    = 2025  # historical archive; 2026 comes from realtime

COLUMNS = [
    "year", "month", "day", "hour", "minute",
    "wind_dir_deg", "wind_speed_mps", "wind_gust_mps",
    "wave_height_m", "dominant_period_s", "avg_period_s",
    "mean_wave_dir_deg", "atm_pressure_hpa", "air_temp_c",
    "sea_surface_temp_c", "dewpoint_c", "visibility_nm",
    "pressure_tendency_hpa", "tide_ft",
]

KEEP_COLS = [
    "wind_dir_deg", "wind_speed_mps", "wind_gust_mps",
    "wave_height_m", "dominant_period_s", "avg_period_s",
    "mean_wave_dir_deg", "atm_pressure_hpa", "air_temp_c",
    "sea_surface_temp_c", "dewpoint_c",
]


def parse_ndbc_text(text: str, station_id: str) -> pd.DataFrame | None:
    lines = [l for l in text.splitlines() if l.strip()]
    # Skip header rows (start with #)
    data_lines = [l for l in lines if not l.startswith("#")]
    if len(data_lines) < 2:
        return None

    # First two lines may be header/units — detect by checking if first token is numeric
    start = 0
    for i, line in enumerate(data_lines):
        if line.split()[0].isdigit():
            start = i
            break
    data_lines = data_lines[start:]

    if not data_lines:
        return None

    df = pd.read_csv(
        io.StringIO("\n".join(data_lines)),
        sep=r"\s+",
        header=None,
        na_values=["MM", "999", "9999", "99.00", "999.0", "9999.0", "99", "99.0"],
    )

    n_cols = df.shape[1]
    df.columns = COLUMNS[:n_cols]

    df["datetime_utc"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=df["day"],
             hour=df["hour"], minute=df["minute"]),
        errors="coerce",
    )
    df.dropna(subset=["datetime_utc"], inplace=True)
    df.insert(0, "station_id", station_id)
    df.insert(1, "station_name", CA_STATIONS[station_id])

    # Keep only useful columns
    available = [c for c in KEEP_COLS if c in df.columns]
    return df[["station_id", "station_name", "datetime_utc"] + available]


def fetch_historical(station_id: str, year: int) -> pd.DataFrame | None:
    url = HIST_URL.format(station=station_id, year=year)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = gzip.decompress(resp.content).decode("latin-1")
        df = parse_ndbc_text(text, station_id)
        if df is not None:
            print(f"    {year}: {len(df):,} rows")
        return df
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"    {year}: no data")
        else:
            print(f"    {year}: error {e}")
        return None
    except Exception as e:
        print(f"    {year}: parse error {e}")
        return None


def fetch_realtime(station_id: str) -> pd.DataFrame | None:
    url = RT_URL.format(station=station_id.lower())
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        df = parse_ndbc_text(resp.text, station_id)
        if df is not None:
            # Keep only 2026
            df = df[df["datetime_utc"].dt.year == 2026]
            print(f"    2026 (realtime): {len(df):,} rows")
        return df
    except Exception as e:
        print(f"    2026 realtime error: {e}")
        return None


def resample_weekly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index("datetime_utc")
    numeric_cols = df.select_dtypes(include="number").columns.tolist()

    weekly = (
        df.groupby("station_id")[numeric_cols]
        .resample("W")
        .mean()
        .reset_index()
    )
    # Add station name back
    name_map = {k: v for k, v in CA_STATIONS.items()}
    weekly.insert(1, "station_name", weekly["station_id"].map(name_map))
    return weekly


def main():
    all_frames = []

    for station_id, station_name in CA_STATIONS.items():
        print(f"\n[{station_id}] {station_name}")
        station_frames = []

        # Historical years
        for year in range(START_YR, END_YR + 1):
            df = fetch_historical(station_id, year)
            if df is not None:
                station_frames.append(df)
            time.sleep(0.2)

        # 2026 realtime
        df_rt = fetch_realtime(station_id)
        if df_rt is not None and not df_rt.empty:
            station_frames.append(df_rt)
        time.sleep(0.3)

        if station_frames:
            station_df = pd.concat(station_frames, ignore_index=True)
            station_df.drop_duplicates(subset=["station_id", "datetime_utc"], inplace=True)
            all_frames.append(station_df)

    if not all_frames:
        print("\nNo data retrieved.")
        return

    print("\nCombining and resampling to weekly...")
    combined = pd.concat(all_frames, ignore_index=True)

    # Filter to exact date range
    combined = combined[
        (combined["datetime_utc"] >= "2017-01-01") &
        (combined["datetime_utc"] <= "2026-04-21")
    ]

    # Resample to weekly means
    weekly = resample_weekly(combined)
    weekly.sort_values(["station_id", "datetime_utc"], inplace=True)
    weekly.reset_index(drop=True, inplace=True)

    # Round numeric columns to 3 decimal places
    numeric_cols = weekly.select_dtypes(include="number").columns
    weekly[numeric_cols] = weekly[numeric_cols].round(3)

    out_path = "ca_ndbc_weekly.csv"
    weekly.to_csv(out_path, index=False)
    print(f"\n✓ Saved {len(weekly):,} rows across {weekly['station_id'].nunique()} stations → {out_path}")
    print(f"  Date range: {weekly['datetime_utc'].min()} → {weekly['datetime_utc'].max()}")
    print(f"  Columns: {list(weekly.columns)}")


if __name__ == "__main__":
    main()
