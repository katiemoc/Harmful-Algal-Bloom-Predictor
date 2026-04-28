from __future__ import annotations

import glob
import math
import os
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SOURCE_CANDIDATES = [
    ROOT / "data_jin" / "noaa_pmn_all_data_2017_2026_weekly.csv",
    ROOT / "data_jin" / "noaa_pmn_all_data_2001_2026.csv",
]
CALHABMAP_GLOB = str(ROOT / "CalHABMAP Data" / "cleaned_hab_data" / "*_cleaned.csv")
CALIFORNIA_WEEKLY_CSV = ROOT / "data_jin" / "noaa_pmn_california_weekly_site.csv"
CALIFORNIA_MATCHED_WEEKLY_CSV = ROOT / "data_jin" / "noaa_pmn_california_matched_weekly_site.csv"

NUMERIC_COLUMNS = [
    "latitude",
    "longitude",
    "air_temp",
    "salinity",
    "water_temp",
    "count",
    "ph",
    "dissoxygen",
    "secchidisk",
    "windspeed",
]

CALHABMAP_MATCH_THRESHOLD_KM = 0.5


def find_source_csv() -> Path:
    for path in SOURCE_CANDIDATES:
        if path.exists():
            return path
    raise FileNotFoundError("Could not find a NOAA PMN source CSV in data_jin/.")


def load_calhabmap_sites() -> pd.DataFrame:
    rows = []
    for path in sorted(glob.glob(CALHABMAP_GLOB)):
        df = pd.read_csv(path)
        rows.append(
            {
                "calhabmap_site": os.path.basename(path).replace("_cleaned.csv", "").replace("HABs-", ""),
                "latitude": pd.to_numeric(df["latitude"], errors="coerce").median(),
                "longitude": pd.to_numeric(df["longitude"], errors="coerce").median(),
            }
        )
    return pd.DataFrame(rows)


def approx_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat_scale = math.pi / 180.0
    x = (lon2 - lon1) * lat_scale * math.cos((lat1 + lat2) / 2.0 * lat_scale)
    y = (lat2 - lat1) * lat_scale
    return radius_km * math.sqrt(x * x + y * y)


def nearest_calhabmap_site(lat: float, lon: float, calhabmap_sites: pd.DataFrame) -> tuple[str | None, float | None]:
    if pd.isna(lat) or pd.isna(lon):
        return None, None

    best_site = None
    best_distance = None
    for row in calhabmap_sites.itertuples(index=False):
        distance = approx_distance_km(lat, lon, row.latitude, row.longitude)
        if best_distance is None or distance < best_distance:
            best_site = row.calhabmap_site
            best_distance = distance
    return best_site, best_distance


def build_weekly_site_dataset(raw_df: pd.DataFrame, calhabmap_sites: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df = df[df["sample_site"].fillna("").str.startswith("CA -")].copy()

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df[df["time"].notna()].copy()
    df = df[df["time"] >= pd.Timestamp("2017-01-01", tz="UTC")].copy()

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["week_start"] = df["time"].dt.tz_convert(None).dt.to_period("W-SUN").dt.start_time

    weekly = (
        df.groupby(["sample_site", "week_start"], dropna=False)
        .agg(
            latitude=("latitude", "median"),
            longitude=("longitude", "median"),
            observations=("sample_site", "size"),
            unique_species=("spec_name", "nunique"),
            unique_abundance_labels=("abundance", "nunique"),
            median_count=("count", "median"),
            max_count=("count", "max"),
            median_water_temp=("water_temp", "median"),
            median_air_temp=("air_temp", "median"),
            median_salinity=("salinity", "median"),
            median_ph=("ph", "median"),
            median_dissoxygen=("dissoxygen", "median"),
            median_secchidisk=("secchidisk", "median"),
            median_windspeed=("windspeed", "median"),
        )
        .reset_index()
        .sort_values(["sample_site", "week_start"])
        .reset_index(drop=True)
    )

    nearest_sites = []
    nearest_distances = []
    matched_sites = []
    for row in weekly.itertuples(index=False):
        nearest_site, nearest_distance = nearest_calhabmap_site(row.latitude, row.longitude, calhabmap_sites)
        nearest_sites.append(nearest_site)
        nearest_distances.append(round(nearest_distance, 3) if nearest_distance is not None else None)
        if nearest_distance is not None and nearest_distance <= CALHABMAP_MATCH_THRESHOLD_KM:
            matched_sites.append(nearest_site)
        else:
            matched_sites.append(None)

    weekly["nearest_calhabmap_site"] = nearest_sites
    weekly["nearest_calhabmap_distance_km"] = nearest_distances
    weekly["matched_calhabmap_site"] = matched_sites

    return weekly


def main() -> None:
    source_csv = find_source_csv()
    raw_df = pd.read_csv(source_csv)
    calhabmap_sites = load_calhabmap_sites()

    weekly = build_weekly_site_dataset(raw_df, calhabmap_sites)
    matched = weekly[weekly["matched_calhabmap_site"].notna()].copy()

    CALIFORNIA_WEEKLY_CSV.parent.mkdir(parents=True, exist_ok=True)
    weekly.to_csv(CALIFORNIA_WEEKLY_CSV, index=False)
    matched.to_csv(CALIFORNIA_MATCHED_WEEKLY_CSV, index=False)

    print(f"Source CSV: {source_csv}")
    print(f"California weekly rows: {len(weekly)}")
    print(f"California weekly sites: {weekly['sample_site'].nunique()}")
    print(f"Confident CalHABMAP-matched rows: {len(matched)}")
    if not matched.empty:
        print("Matched CalHABMAP sites:")
        print(matched[["sample_site", "matched_calhabmap_site"]].drop_duplicates().to_string(index=False))
    print(f"Saved: {CALIFORNIA_WEEKLY_CSV}")
    print(f"Saved: {CALIFORNIA_MATCHED_WEEKLY_CSV}")


if __name__ == "__main__":
    main()
