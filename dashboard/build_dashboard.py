from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from jinja2 import Template
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "processed" / "merged" / "hab_ndbc_merged.csv"
OUT_DIR = ROOT / "dashboard" / "site"
OUT_PATH = OUT_DIR / "index.html"

TARGET = "isHarmful"
EXCLUDE_COLUMNS = ["week_start", "sample_date", TARGET, "pda", "potential_bloom"]
TRAIN_END = pd.Timestamp("2025-01-01")
VALIDATION_END = pd.Timestamp("2026-01-01")
MAP_ZOOM = 6
MAP_BOUNDS = {
    "west": -126.5,
    "east": -114.0,
    "south": 31.3,
    "north": 42.7,
}


HTML_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>California HAB Risk Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #10202a;
      --muted: #5d6b73;
      --line: #d9e2e7;
      --water: #dceff4;
      --land: #f4efe5;
      --panel: #ffffff;
      --low: #2f8f68;
      --medium: #c4881a;
      --high: #c63d3d;
      --shadow: 0 14px 40px rgba(27, 45, 55, 0.12);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: #f7fafb;
    }

    header {
      padding: 28px clamp(18px, 4vw, 56px) 20px;
      border-bottom: 1px solid var(--line);
      background: #ffffff;
    }

    h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 44px);
      line-height: 1.05;
      letter-spacing: 0;
    }

    .subtitle {
      margin: 10px 0 0;
      color: var(--muted);
      max-width: 880px;
      line-height: 1.5;
      font-size: 15px;
    }

    main {
      display: grid;
      grid-template-columns: minmax(320px, 0.95fr) minmax(360px, 1.35fr);
      gap: 22px;
      padding: 22px clamp(18px, 4vw, 56px) 42px;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      min-width: 0;
    }

    .map-panel {
      padding: 18px;
    }

    .map-wrap {
      position: relative;
      height: min(560px, calc(100vh - 220px));
      min-height: 430px;
      overflow: hidden;
      border-radius: 8px;
      background: var(--water);
      border: 1px solid #c9dde4;
    }

    .map-canvas {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      z-index: 1;
      overflow: hidden;
    }

    .map-tile {
      position: absolute;
      width: auto;
      height: auto;
      user-select: none;
      pointer-events: none;
    }

    #station-overlay {
      position: absolute;
      inset: 0;
      pointer-events: none;
      z-index: 650;
    }

    .coast-label {
      position: absolute;
      left: 20px;
      top: 18px;
      z-index: 500;
      font-weight: 700;
      color: #275365;
      font-size: 14px;
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
    }

    .station-marker {
      display: block;
      width: 24px;
      height: 24px;
      border-radius: 999px;
      border: 3px solid #ffffff;
      box-shadow: 0 6px 16px rgba(15, 30, 38, 0.42);
    }

    .station-marker.low { background: var(--low); }
    .station-marker.medium { background: var(--medium); }
    .station-marker.high { background: var(--high); }

    .hab-marker-icon {
      background: transparent;
      border: 0;
      z-index: 900 !important;
    }

    .hab-station-label {
      background: rgba(255, 255, 255, 0.94);
      border: 1px solid var(--line);
      border-radius: 6px;
      color: var(--ink);
      font-size: 11px;
      font-weight: 700;
      padding: 3px 6px;
      box-shadow: 0 4px 10px rgba(15, 30, 38, 0.16);
    }

    .overlay-station {
      position: absolute;
      transform: translate(-50%, -50%);
      display: flex;
      align-items: center;
      gap: 7px;
      white-space: nowrap;
    }

    .overlay-label {
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid var(--line);
      border-radius: 6px;
      color: var(--ink);
      font-size: 11px;
      font-weight: 700;
      padding: 3px 6px;
      box-shadow: 0 4px 10px rgba(15, 30, 38, 0.16);
    }

    .legend {
      position: absolute;
      left: 18px;
      bottom: 18px;
      z-index: 500;
      display: grid;
      gap: 8px;
      padding: 12px;
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid var(--line);
      border-radius: 8px;
      font-size: 12px;
    }

    .legend-row { display: flex; align-items: center; gap: 8px; }
    .swatch { width: 12px; height: 12px; border-radius: 999px; }
    .swatch.low { background: var(--low); }
    .swatch.medium { background: var(--medium); }
    .swatch.high { background: var(--high); }

    .side {
      display: grid;
      gap: 16px;
      align-content: start;
    }

    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      padding: 16px;
    }

    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      min-height: 84px;
    }

    .metric strong {
      display: block;
      font-size: 24px;
      line-height: 1;
      margin-bottom: 7px;
    }

    .metric span {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.3;
    }

    .station-list {
      display: grid;
      gap: 10px;
      padding: 16px;
    }

    .station-card {
      border: 1px solid var(--line);
      border-left-width: 7px;
      border-radius: 8px;
      padding: 14px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: center;
      background: #ffffff;
    }

    .station-card.low { border-left-color: var(--low); }
    .station-card.medium { border-left-color: var(--medium); }
    .station-card.high { border-left-color: var(--high); }

    .station-card h2 {
      margin: 0 0 4px;
      font-size: 17px;
      line-height: 1.2;
      letter-spacing: 0;
    }

    .meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }

    .risk {
      text-align: right;
      min-width: 118px;
    }

    .risk strong {
      display: block;
      font-size: 18px;
    }

    .risk span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
    }

    .features {
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 8px;
    }

    .feature {
      background: #f7fafb;
      border: 1px solid #e3ebef;
      border-radius: 7px;
      padding: 8px;
      min-width: 0;
    }

    .feature b {
      display: block;
      font-size: 13px;
    }

    .feature span {
      color: var(--muted);
      font-size: 11px;
    }

    .notice {
      padding: 16px;
      color: var(--muted);
      line-height: 1.5;
      font-size: 13px;
      border-top: 1px solid var(--line);
    }

    @media (max-width: 980px) {
      main { grid-template-columns: 1fr; }
      .map-wrap { height: 520px; }
    }

    @media (max-width: 640px) {
      .summary, .features { grid-template-columns: 1fr 1fr; }
      .station-card { grid-template-columns: 1fr; }
      .risk { text-align: left; }
      .map-wrap { height: 460px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>California Harmful Algal Bloom Risk</h1>
    <p class="subtitle">
      Demo dashboard using the Random Forest model trained on CalHABMAP, NOAA OISST, and NDBC weekly station data.
      Predictions are shown for trained monitoring stations only, using the latest available row in the project dataset.
    </p>
  </header>

  <main>
    <section class="panel map-panel" aria-label="California station risk map">
      <div class="map-wrap">
        <div class="map-canvas" aria-hidden="true">
          {% for tile in map_tiles %}
            <img
              class="map-tile"
              src="{{ tile.url }}"
              style="left: {{ tile.left }}%; top: {{ tile.top }}%; width: {{ tile.width }}%; height: {{ tile.height }}%;"
              alt=""
            />
          {% endfor %}
        </div>
        <div id="station-overlay" aria-hidden="true"></div>
        <div class="coast-label">Trained California HAB stations</div>
        <div class="legend">
          <div class="legend-row"><span class="swatch low"></span><span>Low risk</span></div>
          <div class="legend-row"><span class="swatch medium"></span><span>Medium risk</span></div>
          <div class="legend-row"><span class="swatch high"></span><span>High / harmful threshold</span></div>
        </div>
      </div>
    </section>

    <section class="side">
      <div class="panel summary">
        <div class="metric"><strong>{{ summary.station_count }}</strong><span>trained stations</span></div>
        <div class="metric"><strong>{{ summary.high_count }}</strong><span>high risk stations</span></div>
        <div class="metric"><strong>{{ summary.threshold_pct }}%</strong><span>harmful threshold</span></div>
        <div class="metric"><strong>{{ summary.latest_update }}</strong><span>latest available week</span></div>
      </div>

      <div class="panel station-list">
        {% for station in stations %}
          <article class="station-card {{ station.risk_class }}">
            <div>
              <h2>{{ station.station }}</h2>
              <div class="meta">
                Week: {{ station.week_start }} · NDBC buoy: {{ station.station_id }}<br>
                Prediction: {{ station.prediction }}
              </div>
            </div>
            <div class="risk">
              <strong>{{ station.risk_label }}</strong>
              <span>{{ station.probability_pct }}% harmful probability</span>
            </div>
            <div class="features">
              <div class="feature"><b>{{ station.sst }}</b><span>OISST rolling SST</span></div>
              <div class="feature"><b>{{ station.ndbc_sst }}</b><span>NDBC sea temp</span></div>
              <div class="feature"><b>{{ station.wind }}</b><span>wind speed</span></div>
              <div class="feature"><b>{{ station.chloro }}</b><span>chlorophyll</span></div>
            </div>
          </article>
        {% endfor %}
      </div>

      <div class="panel notice">
        This is a local demo, not a public health advisory. The model is trained on ten CalHABMAP stations,
        so the dashboard should not be interpreted as full-coverage California coast prediction yet.
        Live daily updates will require automated OISST, NDBC, and HAB feature refreshes.
      </div>
    </section>
  </main>

  <script type="application/json" id="dashboard-data">{{ dashboard_json }}</script>
  <script>
    const dashboardData = JSON.parse(document.getElementById("dashboard-data").textContent);
    const overlay = document.getElementById("station-overlay");
    function renderStationOverlay() {
      overlay.innerHTML = "";
      dashboardData.stations.forEach((station) => {
        const el = document.createElement("div");
        el.className = "overlay-station";
        el.style.left = `${station.map_x_pct}%`;
        el.style.top = `${station.map_y_pct}%`;
        el.title = `${station.station}: ${station.risk_label} (${station.probability_pct}%)`;
        el.innerHTML = `
          <span class="station-marker ${station.risk_class}"></span>
          <span class="overlay-label">${station.station}</span>
        `;
        overlay.appendChild(el);
      });
    }
    renderStationOverlay();
  </script>
</body>
</html>
"""


def risk_label(probability: float, threshold: float) -> tuple[str, str, str]:
    if probability >= threshold:
        return "High", "high", "Harmful"
    if probability >= threshold * 0.5:
        return "Medium", "medium", "Not harmful"
    return "Low", "low", "Not harmful"


def format_value(value: float, unit: str, digits: int = 1) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value:.{digits}f} {unit}"


def lon_to_world_x(lon: float, zoom: int) -> float:
    return (lon + 180.0) / 360.0 * 256 * (2**zoom)


def lat_to_world_y(lat: float, zoom: int) -> float:
    lat_rad = math.radians(lat)
    mercator = math.log(math.tan(math.pi / 4 + lat_rad / 2))
    return (1 - mercator / math.pi) / 2 * 256 * (2**zoom)


def map_pixel_bounds() -> tuple[float, float, float, float]:
    west = lon_to_world_x(MAP_BOUNDS["west"], MAP_ZOOM)
    east = lon_to_world_x(MAP_BOUNDS["east"], MAP_ZOOM)
    north = lat_to_world_y(MAP_BOUNDS["north"], MAP_ZOOM)
    south = lat_to_world_y(MAP_BOUNDS["south"], MAP_ZOOM)
    return west, north, east, south


def project_station(lat: float, lon: float) -> tuple[float, float]:
    west, north, east, south = map_pixel_bounds()
    x = (lon_to_world_x(lon, MAP_ZOOM) - west) / (east - west) * 100
    y = (lat_to_world_y(lat, MAP_ZOOM) - north) / (south - north) * 100
    return round(x, 2), round(y, 2)


def build_map_tiles() -> list[dict[str, float | str]]:
    west, north, east, south = map_pixel_bounds()
    tile_min_x = math.floor(west / 256)
    tile_max_x = math.floor(east / 256)
    tile_min_y = math.floor(north / 256)
    tile_max_y = math.floor(south / 256)
    width = east - west
    height = south - north

    tiles = []
    for x in range(tile_min_x, tile_max_x + 1):
        for y in range(tile_min_y, tile_max_y + 1):
            tiles.append(
                {
                    "url": f"https://tile.openstreetmap.org/{MAP_ZOOM}/{x}/{y}.png",
                    "left": round((x * 256 - west) / width * 100, 3),
                    "top": round((y * 256 - north) / height * 100, 3),
                    "width": round(256 / width * 100, 3),
                    "height": round(256 / height * 100, 3),
                }
            )
    return tiles


def build_model(df: pd.DataFrame):
    features = [column for column in df.columns if column not in EXCLUDE_COLUMNS]
    categorical = ["station", "station_id"]
    numeric = [column for column in features if column not in categorical]

    train_mask = df["week_start"] < TRAIN_END
    validation_mask = (df["week_start"] >= TRAIN_END) & (df["week_start"] < VALIDATION_END)

    x_train = df.loc[train_mask, features]
    y_train = df.loc[train_mask, TARGET].astype(int)
    x_validation = df.loc[validation_mask, features]
    y_validation = df.loc[validation_mask, TARGET].astype(int)

    preprocess = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), numeric),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical,
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocess", preprocess),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=700,
                    max_depth=6,
                    max_features=0.5,
                    min_samples_leaf=10,
                    class_weight="balanced_subsample",
                    random_state=42,
                    n_jobs=1,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)

    validation_proba = model.predict_proba(x_validation)[:, 1]
    precision, recall, thresholds = precision_recall_curve(y_validation, validation_proba)
    f1_scores = 2 * precision[:-1] * recall[:-1] / np.maximum(precision[:-1] + recall[:-1], 1e-12)
    threshold = float(thresholds[int(np.nanargmax(f1_scores))])

    validation_pred = (validation_proba >= threshold).astype(int)
    metrics = {
        "threshold": threshold,
        "accuracy": float(accuracy_score(y_validation, validation_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_validation, validation_pred)),
        "f1": float(f1_score(y_validation, validation_pred, zero_division=0)),
        "recall": float(recall_score(y_validation, validation_pred, zero_division=0)),
        "precision": float(precision_score(y_validation, validation_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_validation, validation_proba)),
        "confusion_matrix": confusion_matrix(y_validation, validation_pred).tolist(),
    }
    return model, features, threshold, metrics


def build_dashboard() -> None:
    df = pd.read_csv(DATA_PATH, parse_dates=["week_start", "sample_date"])
    df["station_id"] = df["station_id"].astype(str)

    model, features, threshold, metrics = build_model(df)

    latest_rows = (
        df.sort_values(["station", "week_start"])
        .groupby("station", as_index=False)
        .tail(1)
        .sort_values("station")
        .copy()
    )
    probabilities = model.predict_proba(latest_rows[features])[:, 1]
    latest_rows["probability"] = probabilities

    stations = []
    for row in latest_rows.sort_values("probability", ascending=False).itertuples(index=False):
        label, risk_class, prediction = risk_label(row.probability, threshold)
        map_x_pct, map_y_pct = project_station(row.latitude, row.longitude)
        stations.append(
            {
                "station": row.station,
                "week_start": row.week_start.date().isoformat(),
                "station_id": row.station_id,
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "map_x_pct": map_x_pct,
                "map_y_pct": map_y_pct,
                "probability": float(row.probability),
                "probability_pct": round(row.probability * 100, 1),
                "risk_label": label,
                "risk_class": risk_class,
                "prediction": prediction,
                "sst": format_value(row.sst_roll_14d, "C"),
                "ndbc_sst": format_value(row.sea_surface_temp_c, "C"),
                "wind": format_value(row.wind_speed_mps, "m/s"),
                "chloro": format_value(row.avg_chloro, ""),
            }
        )

    summary = {
        "station_count": len(stations),
        "high_count": sum(1 for station in stations if station["risk_class"] == "high"),
        "threshold_pct": round(threshold * 100, 1),
        "latest_update": latest_rows["week_start"].max().date().isoformat(),
        "validation_metrics": metrics,
    }

    payload = {"summary": summary, "stations": stations}
    html = Template(HTML_TEMPLATE).render(
        summary=summary,
        stations=stations,
        map_tiles=build_map_tiles(),
        dashboard_json=json.dumps(payload),
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(html)
    print(f"Wrote {OUT_PATH}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    build_dashboard()
