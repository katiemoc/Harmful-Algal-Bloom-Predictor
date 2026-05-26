import pandas as pd
import numpy as np

STATION_COORDS = {
    "Cal Poly Pier":        (35.170, -120.741),
    "Humboldt":             (40.778, -124.197),
    "Humboldt South Bay":   (40.723, -124.223),
    "Monterey Wharf":       (36.604, -121.889),
    "Newport Beach Pier":   (33.606, -117.931),
    "Santa Cruz Wharf":     (36.958, -122.017),
    "Santa Monica Pier":    (34.008, -118.499),
    "Scripps Pier":         (32.867, -117.257),
    "Stearns Wharf":        (34.408, -119.685),
    "Trinidad Pier":        (41.055, -124.147),
}

BUOY_COORDS = {
    46025: (33.749, -119.053), 46053: (34.242, -119.847), 46054: (34.274, -120.459),
    46011: (34.868, -120.867), 46069: (33.669, -120.209), 46086: (32.491, -118.034),
    46047: (32.433, -119.527), 46026: (37.759, -122.833), 46013: (38.242, -123.301),
    46014: (39.196, -123.969), 46012: (37.361, -122.881), 46042: (36.785, -122.398),
    46028: (34.448, -120.967), 46059: (38.038, -122.976), 46022: (40.749, -124.577),
    46027: (41.851, -124.381), 46029: (40.268, -124.537), 46219: (34.455, -120.671),
}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))

# ── Build station → nearest buoy mapping ─────────────────────────────────────
buoy_ids  = list(BUOY_COORDS.keys())
buoy_lats = np.array([BUOY_COORDS[b][0] for b in buoy_ids])
buoy_lons = np.array([BUOY_COORDS[b][1] for b in buoy_ids])

station_to_buoy = {}
print(f"{'CalHABMAP Station':<25} {'Nearest Buoy':>12}  {'Distance (km)':>14}")
print("-" * 55)
for station, (lat, lon) in STATION_COORDS.items():
    dists = haversine(lat, lon, buoy_lats, buoy_lons)
    idx = np.argmin(dists)
    nearest = buoy_ids[idx]
    station_to_buoy[station] = nearest
    print(f"{station:<25} {nearest:>12}  {dists[idx]:>14.1f} km")

# ── Load data ─────────────────────────────────────────────────────────────────
hab  = pd.read_csv("merged_hab_oisst_features.csv", parse_dates=["week_start"])
ndbc = pd.read_csv("ca_ndbc_final.csv", parse_dates=["datetime_utc"])

# ── Map each HAB row to its nearest buoy ─────────────────────────────────────
hab["station_id"] = hab["station"].map(station_to_buoy)

# ── Align NDBC dates to Monday (HAB uses Mon, NDBC uses Sun) ─────────────────
ndbc["week_start"] = ndbc["datetime_utc"] + pd.Timedelta(days=1)

# ── Left join on week_start + station_id ─────────────────────────────────────
merged = hab.merge(
    ndbc.drop(columns=["datetime_utc"]),
    on=["week_start", "station_id"],
    how="left",
)

merged.to_csv("hab_ndbc_merged.csv", index=False)

ndbc_cols = ["wind_speed_mps", "wave_height_m", "dominant_period_s",
             "mean_wave_dir_deg", "atm_pressure_hpa", "air_temp_c", "sea_surface_temp_c"]
match_rate = 100 * (1 - merged["wind_speed_mps"].isnull().mean())
print(f"\nMatch rate: {match_rate:.1f}% of rows have NDBC data")
print(f"\nShape: {merged.shape}")
print(f"Columns: {list(merged.columns)}")