from __future__ import annotations

import argparse
import datetime as dt
import io
import math
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
CALHABMAP_RAW_DIR = RAW_DIR / "calhabmap_weekly_live"
OISST_RAW_DIR = RAW_DIR / "oisst_v21_avhrr"
NDBC_RAW_DIR = RAW_DIR / "ndbc_realtime"
FEED_DIR = PROCESSED_DIR / "model_feed"

CALHABMAP_BASE_URL = "https://erddap.sccoos.org/erddap/tabledap"
OISST_BASE_URL = "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr"
NDBC_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2"

CALHABMAP_COLUMNS = [
    "Location_Code",
    "latitude",
    "longitude",
    "time",
    "Temp",
    "Silicate",
    "Nitrate",
    "Avg_Chloro",
    "pDA",
]

STATIONS = [
    {
        "station": "Humboldt",
        "dataset_id": "HABs-Humboldt",
        "ndbc_station_id": "46022",
        "latitude": 40.77833,
        "longitude": -124.1967,
    },
    {
        "station": "Humboldt South Bay",
        "dataset_id": "HABs-HumboldtSouthBay",
        "ndbc_station_id": "46022",
        "latitude": 40.72343,
        "longitude": -124.2234,
    },
    {
        "station": "Trinidad Pier",
        "dataset_id": "HABs-TrinidadPier",
        "ndbc_station_id": "46022",
        "latitude": 41.05495,
        "longitude": -124.14696,
    },
    {
        "station": "Bodega Marine Lab",
        "dataset_id": "HABs-BodegaMarineLab",
        "ndbc_station_id": "46013",
        "latitude": 38.3167,
        "longitude": -123.0667,
    },
    {
        "station": "Bodega Marine Lab Buoy",
        "dataset_id": "HABs-BodegaMarineLabBuoy",
        "ndbc_station_id": "46013",
        "latitude": 38.375,
        "longitude": -123.075,
    },
    {
        "station": "Santa Cruz Wharf",
        "dataset_id": "HABs-SantaCruzWharf",
        "ndbc_station_id": "46042",
        "latitude": 36.958,
        "longitude": -122.017,
    },
    {
        "station": "Monterey Wharf",
        "dataset_id": "HABs-MontereyWharf",
        "ndbc_station_id": "46042",
        "latitude": 36.603683,
        "longitude": -121.889275,
    },
    {
        "station": "Cal Poly Pier",
        "dataset_id": "HABs-CalPolyPier",
        "ndbc_station_id": "46011",
        "latitude": 35.170204,
        "longitude": -120.740685,
    },
    {
        "station": "Stearns Wharf",
        "dataset_id": "HABs-StearnsWharf",
        "ndbc_station_id": "46053",
        "latitude": 34.408043,
        "longitude": -119.68485,
    },
    {
        "station": "Santa Monica Pier",
        "dataset_id": "HABs-SantaMonicaPier",
        "ndbc_station_id": "46025",
        "latitude": 34.008,
        "longitude": -118.499,
    },
    {
        "station": "Newport Beach Pier",
        "dataset_id": "HABs-NewportBeachPier",
        "ndbc_station_id": "46025",
        "latitude": 33.6061,
        "longitude": -117.9311,
    },
    {
        "station": "Scripps Pier",
        "dataset_id": "HABs-ScrippsPier",
        "ndbc_station_id": "46086",
        "latitude": 32.867,
        "longitude": -117.257,
    },
]


def week_start(series: pd.Series) -> pd.Series:
    """Convert any date/time series to Monday-based week_start dates.

    The HAB/OISST/NDBC model rows are merged by station/location and week_start,
    so every source has to use the same weekly date convention.
    """
    dates = pd.to_datetime(series, utc=True, errors="coerce").dt.tz_convert(None)
    return dates.dt.normalize() - pd.to_timedelta(dates.dt.weekday, unit="D")


def download_text(url: str, timeout: int = 90) -> str:
    """Download a text endpoint and raise an error if the request fails."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def download_binary(url: str, path: Path, timeout: int = 180) -> None:
    """Download a binary file, such as OISST NetCDF, and save it to disk."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)


def calhabmap_url(dataset_id: str, start_date: dt.date) -> str:
    """Build the ERDDAP CSV URL for one CalHABMAP station dataset.

    The query asks only for columns needed by the model pipeline and limits rows
    to the requested start date so weekly updates do not redownload everything.
    """
    cols = ",".join(CALHABMAP_COLUMNS)
    constraint = quote(f'&time>={start_date.isoformat()}T00:00:00Z', safe="=&:>-")
    return f"{CALHABMAP_BASE_URL}/{dataset_id}.csv?{cols}{constraint}"


def fetch_calhabmap(start_date: dt.date, use_existing: bool) -> pd.DataFrame:
    """Fetch or load CalHABMAP rows, then aggregate them to station-week rows.

    This creates the HAB-side features: sample date, station coordinates,
    temperature, silicate, nitrate, chlorophyll, pDA, and the harmful label
    when pDA is available.
    """
    frames = []
    CALHABMAP_RAW_DIR.mkdir(parents=True, exist_ok=True)

    for station in STATIONS:
        raw_path = CALHABMAP_RAW_DIR / f"{station['dataset_id']}.csv"
        if use_existing and raw_path.exists():
            text = raw_path.read_text()
        elif use_existing:
            existing_matches = sorted((RAW_DIR / "calhabmap").glob(f"{station['dataset_id']}_*.csv"))
            if not existing_matches:
                continue
            text = existing_matches[0].read_text()
        else:
            url = calhabmap_url(station["dataset_id"], start_date)
            try:
                text = download_text(url)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    print(f"Skipping {station['station']}: no CalHABMAP rows found after {start_date}")
                    continue
                raise
            raw_path.write_text(text)

        df = pd.read_csv(io.StringIO(text))
        df = df[pd.to_numeric(df["latitude"], errors="coerce").notna()].copy()
        df["station"] = station["station"]
        df["station_id"] = station["ndbc_station_id"]
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)
    raw["sample_date"] = pd.to_datetime(raw["time"], utc=True, errors="coerce").dt.tz_convert(None)
    raw["week_start"] = week_start(raw["time"])
    raw = raw.rename(
        columns={
            "Temp": "temp",
            "Silicate": "silicate",
            "Nitrate": "nitrate",
            "Avg_Chloro": "avg_chloro",
            "pDA": "pda",
        }
    )

    numeric_columns = ["latitude", "longitude", "temp", "silicate", "nitrate", "avg_chloro", "pda"]
    for column in numeric_columns:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")

    weekly = (
        raw.sort_values("sample_date")
        .groupby(["station", "week_start"], as_index=False)
        .agg(
            sample_date=("sample_date", "last"),
            latitude=("latitude", "last"),
            longitude=("longitude", "last"),
            station_id=("station_id", "last"),
            pda=("pda", "max"),
            temp=("temp", "median"),
            silicate=("silicate", "median"),
            nitrate=("nitrate", "median"),
            avg_chloro=("avg_chloro", "median"),
        )
    )
    weekly["month"] = weekly["week_start"].dt.month
    weekly["year"] = weekly["week_start"].dt.year
    weekly["potential_bloom"] = pd.Series(
        np.where(weekly["pda"].isna(), pd.NA, weekly["pda"] > 0.1),
        index=weekly.index,
        dtype="Int64",
    )
    weekly["isHarmful"] = weekly["potential_bloom"]
    weekly = weekly[weekly["week_start"] >= pd.Timestamp(start_date)].copy()
    return weekly


def oisst_url(date_value: dt.date) -> str:
    """Build the NOAA NCEI OISST v2.1 AVHRR-only NetCDF URL for one date."""
    yyyymm = date_value.strftime("%Y%m")
    yyyymmdd = date_value.strftime("%Y%m%d")
    return f"{OISST_BASE_URL}/{yyyymm}/oisst-avhrr-v02r01.{yyyymmdd}.nc"


def download_oisst_files(start_date: dt.date, end_date: dt.date, use_existing: bool) -> list[Path]:
    """Download daily OISST NetCDF files for a date range.

    Recent OISST files can lag a little, so 404s are skipped. Existing cached
    files are reused to make weekly reruns faster.
    """
    paths = []
    current = start_date
    while current <= end_date:
        yyyymm = current.strftime("%Y%m")
        yyyymmdd = current.strftime("%Y%m%d")
        path = OISST_RAW_DIR / yyyymm / f"oisst-avhrr-v02r01.{yyyymmdd}.nc"
        if not path.exists() or not use_existing:
            try:
                download_binary(oisst_url(current), path)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    current += dt.timedelta(days=1)
                    continue
                raise
        if path.exists():
            paths.append(path)
        current += dt.timedelta(days=1)
    return paths


def lon_to_oisst(lon: float) -> float:
    """Convert west-negative longitude to OISST's 0-to-360 longitude format."""
    return lon if lon >= 0 else 360 + lon


def nearest_index(values: np.ndarray, target: float) -> int:
    """Return the array index closest to the requested latitude or longitude."""
    return int(np.abs(values - target).argmin())


def extract_oisst_daily(paths: list[Path]) -> pd.DataFrame:
    """Extract daily SST/anomaly/error values at each station's nearest grid cell.

    OISST is gridded data, not station data, so this samples the nearest grid
    point for each CalHABMAP monitoring location.
    """
    try:
        import netCDF4 as nc
    except ImportError as exc:
        raise SystemExit("Install netCDF4 to fetch live OISST data: pip install netCDF4") from exc

    rows = []
    for path in paths:
        date_text = path.stem.split(".")[-1]
        date_value = pd.to_datetime(date_text, format="%Y%m%d")
        with nc.Dataset(path) as dataset:
            latitudes = np.asarray(dataset.variables["lat"][:])
            longitudes = np.asarray(dataset.variables["lon"][:])
            sst_grid = dataset.variables["sst"][0, 0, :, :]
            anom_grid = dataset.variables["anom"][0, 0, :, :]
            err_grid = dataset.variables["err"][0, 0, :, :]

            for station in STATIONS:
                lat_i = nearest_index(latitudes, station["latitude"])
                lon_i = nearest_index(longitudes, lon_to_oisst(station["longitude"]))
                rows.append(
                    {
                        "date": date_value,
                        "station": station["station"],
                        "lat": float(latitudes[lat_i]),
                        "lon": float(longitudes[lon_i]),
                        "sst": float(np.ma.filled(sst_grid[lat_i, lon_i], np.nan)),
                        "anom": float(np.ma.filled(anom_grid[lat_i, lon_i], np.nan)),
                        "err": float(np.ma.filled(err_grid[lat_i, lon_i], np.nan)),
                    }
                )
    return pd.DataFrame(rows)


def build_oisst_weekly(calhab_weekly: pd.DataFrame, use_existing: bool) -> pd.DataFrame:
    """Create weekly OISST features needed by the Random Forest model.

    In cached mode, this reuses the already processed OISST weekly CSV. In live
    mode, it downloads daily NetCDF files, extracts station SST values, builds
    rolling features, and keeps the last daily value for each station-week.
    """
    if use_existing:
        existing = PROCESSED_DIR / "oisst" / "oisst_california_all_sites_weekly.csv"
        if existing.exists():
            return pd.read_csv(existing, parse_dates=["week_start"])

    min_week = calhab_weekly["week_start"].min().date()
    max_week = calhab_weekly["week_start"].max().date()
    start_date = min_week - dt.timedelta(days=100)
    end_date = max_week + dt.timedelta(days=6)
    paths = download_oisst_files(start_date, end_date, use_existing=True)
    daily = extract_oisst_daily(paths)

    feature_frames = []
    for station, group in daily.sort_values("date").groupby("station"):
        group = group.copy()
        group["sst_roll_7d"] = group["sst"].rolling(7, min_periods=1).mean()
        group["sst_roll_14d"] = group["sst"].rolling(14, min_periods=1).mean()
        group["sst_roll_30d"] = group["sst"].rolling(30, min_periods=1).mean()
        group["sst_roll_90d"] = group["sst"].rolling(90, min_periods=1).mean()
        group["anom_roll_7d"] = group["anom"].rolling(7, min_periods=1).mean()
        group["anom_roll_14d"] = group["anom"].rolling(14, min_periods=1).mean()
        group["anom_roll_30d"] = group["anom"].rolling(30, min_periods=1).mean()
        group["sst_roc_3d"] = group["sst"].diff(3) / 3
        baseline = group["sst_roll_90d"]
        group["warm_degree_days_14d"] = (group["sst"] - baseline).clip(lower=0).rolling(14, min_periods=1).sum()
        group["above_avg"] = (group["sst"] > baseline).astype(int)
        feature_frames.append(group)

    daily = pd.concat(feature_frames, ignore_index=True)
    daily["week_start"] = week_start(daily["date"])
    weekly = (
        daily.sort_values("date")
        .groupby(["station", "week_start"], as_index=False)
        .last()
    )
    return weekly


def fetch_ndbc(use_existing: bool) -> pd.DataFrame:
    """Fetch or load NDBC buoy observations and aggregate them weekly.

    The model uses NDBC environmental features such as wind speed, wave height,
    pressure, air temperature, and sea surface temperature. Cached historical
    data are already weekly; live realtime files are hourly and get summarized
    by weekly medians.
    """
    existing = PROCESSED_DIR / "ndbc" / "ca_ndbc_final.csv"
    if use_existing and existing.exists():
        ndbc = pd.read_csv(existing, parse_dates=["datetime_utc"])
        ndbc = ndbc.rename(columns={"datetime_utc": "week_start"})
        ndbc["week_start"] = pd.to_datetime(ndbc["week_start"]).dt.normalize()
        if ndbc["week_start"].dt.weekday.mode().iat[0] == 6:
            ndbc["week_start"] = ndbc["week_start"] + pd.Timedelta(days=1)
        ndbc["station_id"] = ndbc["station_id"].astype(str)
        return ndbc

    frames = []
    NDBC_RAW_DIR.mkdir(parents=True, exist_ok=True)
    station_ids = sorted({station["ndbc_station_id"] for station in STATIONS})

    for station_id in station_ids:
        raw_path = NDBC_RAW_DIR / f"{station_id}.txt"
        if use_existing and raw_path.exists():
            text = raw_path.read_text()
        else:
            text = download_text(f"{NDBC_BASE_URL}/{station_id}.txt")
            raw_path.write_text(text)

        df = pd.read_csv(io.StringIO(text), sep=r"\s+", comment="#", header=None)
        if df.empty:
            continue

        header = text.splitlines()[0].replace("#", "").split()
        units = text.splitlines()[1].replace("#", "").split()
        if len(header) == len(df.columns):
            df.columns = header
        else:
            df.columns = ["YY", "MM", "DD", "hh", "mm"] + [f"value_{i}" for i in range(len(df.columns) - 5)]

        df["station_id"] = station_id
        df["datetime_utc"] = pd.to_datetime(
            df[["YY", "MM", "DD", "hh", "mm"]].rename(
                columns={"YY": "year", "MM": "month", "DD": "day", "hh": "hour", "mm": "minute"}
            ),
            errors="coerce",
        )

        column_map = {
            "WSPD": "wind_speed_mps",
            "WVHT": "wave_height_m",
            "DPD": "dominant_period_s",
            "MWD": "mean_wave_dir_deg",
            "PRES": "atm_pressure_hpa",
            "ATMP": "air_temp_c",
            "WTMP": "sea_surface_temp_c",
        }
        selected = ["station_id", "datetime_utc"]
        for source, target in column_map.items():
            if source in df.columns:
                df[target] = pd.to_numeric(df[source], errors="coerce").replace(99.0, np.nan).replace(999.0, np.nan)
                selected.append(target)

        frames.append(df[selected])

    if not frames:
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)
    raw["week_start"] = week_start(raw["datetime_utc"])
    value_columns = [column for column in raw.columns if column not in {"station_id", "datetime_utc", "week_start"}]
    return raw.groupby(["station_id", "week_start"], as_index=False)[value_columns].median()


def add_hab_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add previous-week and two-weeks-ago HAB feature values by station.

    These lag features let the model use recent trends in water temperature,
    nutrients, and chlorophyll instead of only the current week's value.
    """
    df = df.sort_values(["station", "week_start"]).copy()
    lag_columns = ["temp", "silicate", "nitrate", "avg_chloro"]
    for column in lag_columns:
        df[f"{column}_lag1"] = df.groupby("station")[column].shift(1)
        df[f"{column}_lag2"] = df.groupby("station")[column].shift(2)
        df[f"{column}_lag1"] = df[f"{column}_lag1"].fillna(df[column])
        df[f"{column}_lag2"] = df[f"{column}_lag2"].fillna(df[f"{column}_lag1"])

    df["silicate_nitrate_ratio"] = df["silicate"] / df["nitrate"].replace(0, np.nan)
    return df


def merge_model_feed(calhab: pd.DataFrame, oisst: pd.DataFrame, ndbc: pd.DataFrame) -> pd.DataFrame:
    """Merge CalHABMAP, OISST, and NDBC into the model's expected schema.

    CalHABMAP and OISST merge by station and week_start. NDBC merges by buoy
    station_id and week_start. The output column order matches hab_ndbc_merged.csv
    so the existing Random Forest/dashboard code can use it directly.
    """
    oisst_columns = [
        "station",
        "week_start",
        "sst_roll_14d",
        "anom_roll_14d",
        "sst_roc_3d",
        "warm_degree_days_14d",
        "above_avg",
    ]
    merged = calhab.merge(oisst[oisst_columns], on=["station", "week_start"], how="left")
    merged = add_hab_lag_features(merged)
    merged["station_id"] = merged["station_id"].astype(str)

    if not ndbc.empty:
        ndbc = ndbc.copy()
        ndbc["station_id"] = ndbc["station_id"].astype(str)
        merged = merged.merge(ndbc, on=["station_id", "week_start"], how="left")

    ordered_columns = [
        "week_start",
        "station",
        "sample_date",
        "latitude",
        "longitude",
        "month",
        "year",
        "pda",
        "temp",
        "silicate",
        "nitrate",
        "avg_chloro",
        "potential_bloom",
        "sst_roll_14d",
        "anom_roll_14d",
        "sst_roc_3d",
        "warm_degree_days_14d",
        "above_avg",
        "temp_lag1",
        "temp_lag2",
        "silicate_lag1",
        "silicate_lag2",
        "nitrate_lag1",
        "nitrate_lag2",
        "avg_chloro_lag1",
        "avg_chloro_lag2",
        "silicate_nitrate_ratio",
        "isHarmful",
        "station_id",
        "wind_speed_mps",
        "wave_height_m",
        "dominant_period_s",
        "mean_wave_dir_deg",
        "atm_pressure_hpa",
        "air_temp_c",
        "sea_surface_temp_c",
    ]
    for column in ordered_columns:
        if column not in merged.columns:
            merged[column] = np.nan
    return merged[ordered_columns].sort_values(["week_start", "station"])


def parse_args() -> argparse.Namespace:
    """Read command-line options for date range, cache mode, and output path."""
    parser = argparse.ArgumentParser(description="Download weekly HAB/OISST/NDBC data and build model feed rows.")
    parser.add_argument("--start-date", default=None, help="Fetch CalHABMAP rows from this date, YYYY-MM-DD.")
    parser.add_argument("--use-existing", action="store_true", help="Use cached/raw or processed files instead of downloading.")
    parser.add_argument("--output", default=str(FEED_DIR / "weekly_model_feed.csv"), help="Output CSV path.")
    return parser.parse_args()


def main() -> None:
    """Run the full weekly pipeline and write both full and latest feed CSVs."""
    args = parse_args()
    today = dt.date.today()
    start_date = dt.date.fromisoformat(args.start_date) if args.start_date else today - dt.timedelta(days=90)

    FEED_DIR.mkdir(parents=True, exist_ok=True)
    calhab = fetch_calhabmap(start_date, use_existing=args.use_existing)
    if calhab.empty:
        raise SystemExit("No CalHABMAP rows were available for the requested date range.")

    oisst = build_oisst_weekly(calhab, use_existing=args.use_existing)
    ndbc = fetch_ndbc(use_existing=args.use_existing)
    feed = merge_model_feed(calhab, oisst, ndbc)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    feed.to_csv(output_path, index=False)

    latest_output = output_path.parent / "latest_model_feed.csv"
    latest = feed.sort_values(["station", "week_start"]).groupby("station", as_index=False).tail(1)
    latest.to_csv(latest_output, index=False)

    print(f"Wrote {len(feed)} weekly rows to {output_path}")
    print(f"Wrote {len(latest)} latest station rows to {latest_output}")
    print(f"Date range: {feed['week_start'].min().date()} to {feed['week_start'].max().date()}")


if __name__ == "__main__":
    main()
