from __future__ import annotations

import json
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
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = ROOT / "data" / "processed" / "merged" / "hab_ndbc_merged.csv"
PREDICTION_FEED_PATH = ROOT / "data" / "processed" / "model_feed" / "latest_model_feed.csv"
OUT_DIR = ROOT / "website" / "dashboard" / "site"
OUT_PATH = OUT_DIR / "index.html"

TARGET = "isHarmful"
EXCLUDE_COLUMNS = ["week_start", "sample_date", TARGET, "pda", "potential_bloom"]
TRAIN_END = pd.Timestamp("2025-01-01")
VALIDATION_END = pd.Timestamp("2026-01-01")
RECENT_DATA_COLUMNS = ["avg_chloro", "wind_speed_mps", "sea_surface_temp_c"]
MAX_MISSING_RECENT_COLUMNS = 1

# Real CalHABMAP station coordinates (from dataset)
STATION_COORDS: dict[str, tuple[float, float]] = {
    "Trinidad Pier":      (41.054950, -124.146960),
    "Humboldt":           (40.778330, -124.196700),
    "Humboldt South Bay": (40.723430, -124.223400),
    "Santa Cruz Wharf":   (36.958000, -122.017000),
    "Monterey Wharf":     (36.603683, -121.889275),
    "Cal Poly Pier":      (35.170204, -120.740685),
    "Stearns Wharf":      (34.408043, -119.684850),
    "Santa Monica Pier":  (34.008000, -118.499000),
    "Newport Beach Pier": (33.606100, -117.931100),
    "Scripps Pier":       (32.867000, -117.257000),
}


HTML_TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>California HAB Risk Dashboard</title>

  <!-- Fonts -->
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,200..700&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet" />

  <!-- Leaflet -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

  <style>
    :root {
      --void:       #0b1920;
      --navy:       #10202a;
      --grid:       #1e3340;
      --hero-text:  #e8ece8;
      --muted-dark: #8fa3ad;
      --water:      #dceff4;
      --panel:      #ffffff;
      --ink:        #10202a;
      --muted:      #5d6b73;
      --line:       #d9e2e7;
      --bg:         #f7fafb;
      --teal:       #1b8fa8;
      --low:        #2f8f68;
      --medium:     #c4881a;
      --high:       #c63d3d;
      --insufficient:#6d7780;
      --high-glow:  rgba(198,61,61,0.18);
      --shadow:     0 2px 12px rgba(16,32,42,0.08);
      --shadow-lg:  0 8px 32px rgba(16,32,42,0.14);
      --topbar-h:   52px;
      --sidebar-w:  390px;
    }

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; overflow: hidden; font-family: 'DM Sans', ui-sans-serif, sans-serif; font-size: 14px; color: var(--ink); background: var(--bg); }

    /* ── TOPBAR ── */
    .topbar {
      position: fixed; top: 0; left: 0; right: 0;
      height: var(--topbar-h);
      background: var(--navy);
      border-bottom: 1px solid var(--grid);
      display: flex; align-items: center; padding: 0 20px; z-index: 1000;
    }
    .topbar-back {
      display: flex; align-items: center; gap: 6px;
      font-family: 'DM Mono', monospace; font-size: 11px; color: var(--muted-dark);
      text-decoration: none; padding-right: 14px; margin-right: 16px;
      border-right: 1px solid var(--grid); height: 100%; transition: color 0.15s;
    }
    .topbar-back:hover { color: var(--water); }
    .topbar-logo { font-family: 'Fraunces', serif; font-size: 16px; font-weight: 500; color: var(--hero-text); }
    .topbar-sub {
      font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted-dark);
      margin-left: 12px; padding-left: 12px; border-left: 1px solid var(--grid); letter-spacing: 0.04em;
    }
    .topbar-right { margin-left: auto; display: flex; align-items: center; gap: 16px; }
    .topbar-update { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted-dark); letter-spacing: 0.04em; }
    .topbar-alert {
      display: flex; align-items: center; gap: 6px;
      background: rgba(198,61,61,0.15); border: 1px solid rgba(198,61,61,0.3);
      border-radius: 4px; padding: 4px 10px;
      font-family: 'DM Mono', monospace; font-size: 10px; color: #f08080; letter-spacing: 0.04em;
    }
    .alert-dot {
      width: 6px; height: 6px; border-radius: 50%; background: var(--high);
      animation: blink 2s ease-in-out infinite;
    }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.4} }

    /* ── LAYOUT ── */
    .app { position: fixed; top: var(--topbar-h); left: 0; right: 0; bottom: 0; display: flex; }
    #map { flex: 1; height: 100%; }

    /* ── LEAFLET OVERRIDES ── */
    .leaflet-control-zoom { border: none !important; border-radius: 8px !important; overflow: hidden; box-shadow: var(--shadow-lg) !important; }
    .leaflet-control-zoom a { background: var(--navy) !important; color: var(--hero-text) !important; border: none !important; font-family: 'DM Mono', monospace !important; font-size: 16px !important; width: 36px !important; height: 36px !important; line-height: 36px !important; transition: background 0.15s; }
    .leaflet-control-zoom a:hover { background: var(--grid) !important; color: var(--water) !important; }
    .leaflet-control-attribution { font-family: 'DM Mono', monospace !important; font-size: 9px !important; background: rgba(11,25,32,0.7) !important; color: #8fa3ad !important; border-radius: 4px 0 0 0 !important; }
    .leaflet-control-attribution a { color: var(--water) !important; }
    .leaflet-popup-content-wrapper { background: var(--navy) !important; border: 1px solid var(--grid) !important; border-radius: 8px !important; box-shadow: var(--shadow-lg) !important; padding: 0 !important; }
    .leaflet-popup-content { margin: 0 !important; width: 248px !important; }
    .leaflet-popup-tip { background: var(--navy) !important; }
    .leaflet-popup-close-button { color: var(--muted-dark) !important; font-size: 18px !important; top: 8px !important; right: 10px !important; }

    /* Station dots */
    .sdot { width: 20px; height: 20px; border-radius: 50%; border: 2.5px solid white; box-shadow: 0 2px 8px rgba(0,0,0,0.4); cursor: pointer; transition: transform 0.15s; }
    .sdot.low    { background: var(--low); }
    .sdot.medium { background: var(--medium); }
    .sdot.high   { background: var(--high); box-shadow: 0 2px 12px rgba(198,61,61,0.5); }
    .sdot.insufficient { background: var(--insufficient); box-shadow: 0 2px 8px rgba(0,0,0,0.28); }
    .sdot.active { transform: scale(1.5); border-color: white; z-index: 1000 !important; }
    .sdot.high.active { box-shadow: 0 0 20px rgba(198,61,61,0.7); }

    /* Map overlays */
    .map-overlay { position: absolute; top: 12px; left: 12px; z-index: 800; display: flex; flex-direction: column; gap: 8px; pointer-events: none; }
    .map-chip {
      background: rgba(11,25,32,0.88); backdrop-filter: blur(4px);
      border: 1px solid var(--grid); border-radius: 6px;
      padding: 6px 12px; font-family: 'DM Mono', monospace; font-size: 10px;
      color: var(--muted-dark); letter-spacing: 0.08em; text-transform: uppercase;
    }
    .map-legend { background: rgba(11,25,32,0.88); backdrop-filter: blur(4px); border: 1px solid var(--grid); border-radius: 6px; padding: 10px 12px; display: flex; flex-direction: column; gap: 6px; }
    .legend-row { display: flex; align-items: center; gap: 8px; font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted-dark); }
    .l-dot { width: 10px; height: 10px; border-radius: 50%; border: 1.5px solid rgba(255,255,255,0.4); flex-shrink: 0; }
    .l-dot.low { background: var(--low); } .l-dot.medium { background: var(--medium); } .l-dot.high { background: var(--high); } .l-dot.insufficient { background: var(--insufficient); }

    /* ── SIDEBAR ── */
    .sidebar { width: var(--sidebar-w); flex-shrink: 0; height: 100%; background: var(--panel); border-left: 1px solid var(--line); display: flex; flex-direction: column; overflow: hidden; box-shadow: -4px 0 16px rgba(16,32,42,0.06); }

    /* Metrics strip */
    .metrics-strip { display: grid; grid-template-columns: repeat(4, 1fr); border-bottom: 1px solid var(--line); flex-shrink: 0; }
    .metric-cell { padding: 14px 12px; border-right: 1px solid var(--line); }
    .metric-cell:last-child { border-right: none; }
    .metric-val { font-family: 'Fraunces', serif; font-size: 26px; font-weight: 600; line-height: 1; margin-bottom: 4px; }
    .metric-val.danger { color: var(--high); } .metric-val.caution { color: var(--medium); } .metric-val.ok { color: var(--low); }
    .metric-label { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); line-height: 1.3; }

    /* Model toggle */
    .model-toggle { display: flex; align-items: center; padding: 8px 16px; border-bottom: 1px solid var(--line); gap: 8px; flex-shrink: 0; background: var(--bg); }
    .toggle-label { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); letter-spacing: 0.06em; margin-right: 4px; }
    .toggle-btn {
      padding: 5px 12px; border-radius: 4px; font-family: 'DM Mono', monospace; font-size: 10px;
      letter-spacing: 0.06em; cursor: pointer; border: 1px solid var(--line);
      background: transparent; color: var(--muted); transition: all 0.15s;
    }
    .toggle-btn.active { background: var(--navy); color: var(--hero-text); border-color: var(--navy); }
    .toggle-btn:hover:not(.active) { border-color: var(--teal); color: var(--teal); }

    /* Filter tabs */
    .filter-bar { display: flex; align-items: center; padding: 0 16px; border-bottom: 1px solid var(--line); flex-shrink: 0; }
    .filter-tab { padding: 10px 12px; font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: 0.06em; color: var(--muted); cursor: pointer; border-bottom: 2px solid transparent; transition: all 0.15s; background: none; border-top: none; border-left: none; border-right: none; }
    .filter-tab:hover { color: var(--ink); }
    .filter-tab.active { color: var(--teal); border-bottom-color: var(--teal); }
    .filter-tab.f-high.active { color: var(--high); border-bottom-color: var(--high); }
    .filter-tab.f-medium.active { color: var(--medium); border-bottom-color: var(--medium); }
    .fcnt { display: inline-flex; align-items: center; justify-content: center; width: 15px; height: 15px; border-radius: 3px; font-size: 9px; margin-left: 4px; font-weight: 600; }
    .filter-tab.active .fcnt { background: rgba(27,143,168,0.12); color: var(--teal); }
    .filter-tab.f-high.active .fcnt { background: rgba(198,61,61,0.12); color: var(--high); }
    .filter-tab.f-medium.active .fcnt { background: rgba(196,136,26,0.12); color: var(--medium); }

    /* Station list */
    .station-list { flex: 1; overflow-y: auto; padding: 12px; }
    .station-list::-webkit-scrollbar { width: 4px; }
    .station-list::-webkit-scrollbar-track { background: transparent; }
    .station-list::-webkit-scrollbar-thumb { background: var(--line); border-radius: 2px; }

    /* Station cards */
    .scard { border: 1px solid var(--line); border-left: 4px solid transparent; border-radius: 8px; padding: 14px; margin-bottom: 8px; background: white; cursor: pointer; transition: all 0.15s; }
    .scard:hover { box-shadow: var(--shadow); }
    .scard.active { background: #f4f8fa; box-shadow: var(--shadow-lg); }
    .scard.low { border-left-color: var(--low); } .scard.medium { border-left-color: var(--medium); } .scard.high { border-left-color: var(--high); } .scard.insufficient { border-left-color: var(--insufficient); }
    .scard.active.high { background: rgba(198,61,61,0.03); }
    .card-top { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 10px; }
    .card-name { font-weight: 600; font-size: 14px; color: var(--ink); line-height: 1.2; }
    .card-meta { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); margin-top: 3px; }
    .card-risk { text-align: right; flex-shrink: 0; margin-left: 10px; }
    .card-pct { font-family: 'Fraunces', serif; font-size: 24px; font-weight: 700; line-height: 1; }
    .card-pct.low { color: var(--low); } .card-pct.medium { color: var(--medium); } .card-pct.high { color: var(--high); } .card-pct.insufficient { color: var(--insufficient); font-size: 16px; }
    .card-rlabel { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.06em; text-transform: uppercase; margin-top: 2px; }
    .card-rlabel.low { color: var(--low); } .card-rlabel.medium { color: var(--medium); } .card-rlabel.high { color: var(--high); } .card-rlabel.insufficient { color: var(--insufficient); }
    .card-features { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; padding-top: 10px; border-top: 1px solid var(--line); }
    .feat { background: var(--bg); border-radius: 4px; padding: 6px 8px; }
    .feat-val { font-family: 'DM Mono', monospace; font-size: 11px; font-weight: 500; color: var(--ink); }
    .feat-label { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted); letter-spacing: 0.04em; margin-top: 2px; }

    /* Comparison row (shown when both models active) */
    .model-compare { display: flex; gap: 8px; margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--line); }
    .mc-item { flex: 1; background: var(--bg); border-radius: 4px; padding: 6px 8px; }
    .mc-label { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted); letter-spacing: 0.04em; }
    .mc-val { font-family: 'DM Mono', monospace; font-size: 11px; font-weight: 500; margin-top: 2px; }
    .mc-val.low { color: var(--low); } .mc-val.medium { color: var(--medium); } .mc-val.high { color: var(--high); } .mc-val.insufficient { color: var(--insufficient); }

    /* Model performance panel */
    .model-panel { border-top: 1px solid var(--line); padding: 14px 16px; flex-shrink: 0; background: var(--bg); }
    .mp-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
    .mp-title { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--muted); }
    .mp-badge { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--teal); background: rgba(27,143,168,0.08); border: 1px solid rgba(27,143,168,0.2); border-radius: 4px; padding: 2px 7px; }
    .mp-metrics { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }
    .mp-metric { text-align: center; }
    .mp-val { font-family: 'DM Mono', monospace; font-size: 13px; font-weight: 500; color: var(--ink); }
    .mp-label { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 2px; }

    /* Popup */
    .pu-inner { padding: 16px; }
    .pu-name { font-family: 'DM Sans', sans-serif; font-weight: 600; font-size: 13px; color: var(--hero-text); margin-bottom: 4px; }
    .pu-meta { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted-dark); margin-bottom: 12px; }
    .pu-risk { display: flex; align-items: baseline; gap: 8px; margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid var(--grid); }
    .pu-pct { font-family: 'Fraunces', serif; font-size: 32px; font-weight: 700; line-height: 1; }
    .pu-pct.low { color: var(--low); } .pu-pct.medium { color: var(--medium); } .pu-pct.high { color: var(--high); }
    .pu-rlabel { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.06em; text-transform: uppercase; }
    .pu-rlabel.low { color: var(--low); } .pu-rlabel.medium { color: var(--medium); } .pu-rlabel.high { color: var(--high); } .pu-rlabel.insufficient { color: var(--insufficient); }
    .pu-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 10px; }
    .pu-val { font-family: 'DM Mono', monospace; font-size: 12px; color: var(--hero-text); }
    .pu-label { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted-dark); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 2px; }
    .pu-compare { display: flex; gap: 8px; padding-top: 10px; border-top: 1px solid var(--grid); }
    .pu-citem { flex: 1; }
    .pu-cval { font-family: 'DM Mono', monospace; font-size: 11px; }
    .pu-cval.low { color: var(--low); } .pu-cval.medium { color: var(--medium); } .pu-cval.high { color: var(--high); } .pu-cval.insufficient { color: var(--insufficient); }
    .pu-clabel { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted-dark); margin-top: 2px; }

    .notice { padding: 10px 16px; border-top: 1px solid var(--line); font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); line-height: 1.6; flex-shrink: 0; }
  </style>
</head>
<body>

  <!-- TOPBAR -->
  <header class="topbar">
    <a href="../../website/index.html" class="topbar-back">← Overview</a>
    <span class="topbar-logo">HAB Predictor</span>
    <span class="topbar-sub">California Coastal Risk Dashboard</span>
    <div class="topbar-right">
     <span class="topbar-update">Latest available dataset week: {{ summary.latest_update }}</span>
      {% if summary.high_count > 0 %}
      <div class="topbar-alert">
        <div class="alert-dot"></div>
        {{ summary.high_count }} HIGH-RISK ESTIMATE{% if summary.high_count != 1 %}S{% endif %}
      </div>
      {% endif %}
    </div>
  </header>

  <!-- APP -->
  <div class="app">
    <div style="position:relative;flex:1;height:100%;">
      <div id="map"></div>
      <div class="map-overlay">
        <div class="map-chip">{{ summary.station_count }} CalHABMAP-linked station locations</div>
        <div class="map-legend">
          <div class="legend-row"><div class="l-dot high"></div>High risk</div>
          <div class="legend-row"><div class="l-dot medium"></div>Elevated</div>
          <div class="legend-row"><div class="l-dot low"></div>Low risk</div>
          <div class="legend-row"><div class="l-dot insufficient"></div>Insufficient data</div>
        </div>
      </div>
    </div>

    <!-- SIDEBAR -->
    <aside class="sidebar">

      <!-- Metrics -->
      <div class="metrics-strip">
        <div class="metric-cell">
          <div class="metric-val">{{ summary.station_count }}</div>
          <div class="metric-label">stations<br>monitored</div>
        </div>
        <div class="metric-cell">
          <div class="metric-val danger">{{ summary.high_count }}</div>
          <div class="metric-label">high-risk<br>estimates</div>
        </div>
        <div class="metric-cell">
          <div class="metric-val caution">{{ summary.medium_count }}</div>
          <div class="metric-label">elevated<br>stations</div>
        </div>
        <div class="metric-cell">
          <div class="metric-val">{{ summary.insufficient_count }}</div>
          <div class="metric-label">insufficient<br>recent data</div>
        </div>
      </div>

      <!-- Model toggle -->
      <div class="model-toggle">
        <span class="toggle-label">Model:</span>
        <button class="toggle-btn active" id="btn-rf" onclick="setModel('rf')">Random Forest</button>
        <button class="toggle-btn" id="btn-xgb" onclick="setModel('xgb')">XGBoost</button>
        <button class="toggle-btn" id="btn-both" onclick="setModel('both')">Both</button>
      </div>

      <!-- Filter tabs -->
      <div class="filter-bar">
        <button class="filter-tab active" onclick="filterStations('all',this)">All<span class="fcnt" id="cnt-all">{{ summary.station_count }}</span></button>
        <button class="filter-tab f-high" onclick="filterStations('high',this)">High<span class="fcnt" id="cnt-high">{{ summary.high_count }}</span></button>
        <button class="filter-tab f-medium" onclick="filterStations('medium',this)">Elevated<span class="fcnt" id="cnt-med">{{ summary.medium_count }}</span></button>
        <button class="filter-tab" onclick="filterStations('low',this)">Low<span class="fcnt" id="cnt-low">{{ summary.low_count }}</span></button>
        <button class="filter-tab" onclick="filterStations('insufficient',this)">Insufficient<span class="fcnt" id="cnt-insufficient">{{ summary.insufficient_count }}</span></button>
      </div>

      <!-- Station list -->
      <div class="station-list" id="station-list"></div>

      <!-- Model performance -->
      <div class="model-panel">
        <div class="mp-header">
          <span class="mp-title" id="model-perf-title">Random Forest Performance</span>
          <span class="mp-badge" id="model-perf-badge">validation set</span>
        </div>
        <div class="mp-metrics" id="model-perf-metrics">
          <div class="mp-metric"><div class="mp-val">{{ "%.2f"|format(summary.rf_metrics.roc_auc) }}</div><div class="mp-label">AUC-ROC</div></div>
          <div class="mp-metric"><div class="mp-val">{{ "%.2f"|format(summary.rf_metrics.f1) }}</div><div class="mp-label">F1</div></div>
          <div class="mp-metric"><div class="mp-val">{{ "%.2f"|format(summary.rf_metrics.recall) }}</div><div class="mp-label">Recall</div></div>
          <div class="mp-metric"><div class="mp-val">{{ "%.2f"|format(summary.rf_metrics.precision) }}</div><div class="mp-label">Precision</div></div>
        </div>
      </div>

      <!-- Disclaimer -->
      <div class="notice">
        Research demo — not a public health advisory. Risk levels are model-estimated from CalHABMAP-linked observations and environmental features. Live updates would require automated OISST + NDBC data refresh.
      </div>

    </aside>
  </div>

  <script type="application/json" id="dashboard-data">{{ dashboard_json }}</script>
  <script>
    const DATA = JSON.parse(document.getElementById('dashboard-data').textContent);
    const STATIONS = DATA.stations;

    let activeModel = 'rf';
    let activeStation = null;
    let currentFilter = 'all';
    const markers = {};

    // ── MAP INIT ──
    const map = L.map('map', { center: [37.2, -121.5], zoom: 6, zoomControl: true });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 18,
    }).addTo(map);
    map.zoomControl.setPosition('bottomright');

    function riskClass(prob, threshold) {
      if (prob >= threshold) return 'high';
      if (prob >= threshold * 0.5) return 'medium';
      return 'low';
    }
    function riskLabel(cls) {
      if (cls === 'insufficient') return 'INSUFFICIENT';
      return cls === 'medium' ? 'ELEVATED' : cls.toUpperCase();
    }
    function displayRiskClass(s) {
      if (s.insufficient_recent_data) return 'insufficient';
      return riskClass(getProb(s), getThreshold(s));
    }
    function displayProbability(s) {
      return s.insufficient_recent_data ? 'DATA' : `${(getProb(s) * 100).toFixed(1)}%`;
    }

    function getProb(s) {
      return activeModel === 'xgb' ? s.xgb_probability : s.rf_probability;
    }
    function getThreshold(s) {
      return activeModel === 'xgb' ? DATA.summary.xgb_threshold : DATA.summary.rf_threshold;
    }

    // ── MARKERS ──
    STATIONS.forEach(s => {
      const cls = s.insufficient_recent_data ? 'insufficient' : riskClass(s.rf_probability, DATA.summary.rf_threshold);

      const icon = L.divIcon({
        className: '',
        html: `<div class="sdot ${cls}" id="dot-${s.id}"></div>`,
        iconSize: [20, 20], iconAnchor: [10, 10], popupAnchor: [0, -14],
      });

      const marker = L.marker([s.latitude, s.longitude], { icon });
      marker.bindPopup(buildPopup(s), { maxWidth: 268, minWidth: 248 });
      marker.on('click', () => activateStation(s.id));
      map.addLayer(marker);
      markers[s.id] = marker;
    });

    function buildPopup(s) {
      const cls = displayRiskClass(s);
      const rfCls = s.insufficient_recent_data ? 'insufficient' : riskClass(s.rf_probability, DATA.summary.rf_threshold);
      const xgbCls = s.insufficient_recent_data ? 'insufficient' : riskClass(s.xgb_probability, DATA.summary.xgb_threshold);
      return `
        <div class="pu-inner">
          <div class="pu-name">${s.station}</div>
          <div class="pu-meta">${s.station_id} · ${s.latitude.toFixed(3)}°N · Week ${s.week_start}</div>
          <div class="pu-risk">
            <span class="pu-pct ${cls}">${displayProbability(s)}</span>
            <div>
              <div class="pu-rlabel ${cls}">${riskLabel(cls)}</div>
              <div style="font-family:'DM Mono',monospace;font-size:9px;color:#8fa3ad;margin-top:2px;">${s.insufficient_recent_data ? `missing ${s.missing_recent_count} key inputs` : 'harmful probability'}</div>
            </div>
          </div>
          <div class="pu-grid">
            <div><div class="pu-val">${s.sst}</div><div class="pu-label">OISST SST</div></div>
            <div><div class="pu-val">${s.ndbc_sst}</div><div class="pu-label">NDBC temp</div></div>
            <div><div class="pu-val">${s.wind}</div><div class="pu-label">Wind speed</div></div>
            <div><div class="pu-val">${s.chloro}</div><div class="pu-label">Chlorophyll</div></div>
          </div>
          <div class="pu-compare">
            <div class="pu-citem">
              <div class="pu-cval ${rfCls}">${(s.rf_probability*100).toFixed(1)}% · ${riskLabel(rfCls)}</div>
              <div class="pu-clabel">Random Forest</div>
            </div>
            <div class="pu-citem">
              <div class="pu-cval ${xgbCls}">${(s.xgb_probability*100).toFixed(1)}% · ${riskLabel(xgbCls)}</div>
              <div class="pu-clabel">XGBoost</div>
            </div>
          </div>
        </div>`;
    }

    // ── RENDER LIST ──
    function renderList() {
      const list = document.getElementById('station-list');
      const filtered = currentFilter === 'all'
        ? STATIONS
        : STATIONS.filter(s => displayRiskClass(s) === currentFilter);

      // Sort by current model probability descending
      filtered.sort((a, b) => getProb(b) - getProb(a));

      list.innerHTML = filtered.map(s => {
        const prob = getProb(s);
        const thr = getThreshold(s);
        const cls = displayRiskClass(s);
        const rfCls = s.insufficient_recent_data ? 'insufficient' : riskClass(s.rf_probability, DATA.summary.rf_threshold);
        const xgbCls = s.insufficient_recent_data ? 'insufficient' : riskClass(s.xgb_probability, DATA.summary.xgb_threshold);
        const compareRow = activeModel === 'both' ? `
          <div class="model-compare">
            <div class="mc-item">
              <div class="mc-label">Random Forest</div>
              <div class="mc-val ${rfCls}">${(s.rf_probability*100).toFixed(1)}% · ${riskLabel(rfCls)}</div>
            </div>
            <div class="mc-item">
              <div class="mc-label">XGBoost</div>
              <div class="mc-val ${xgbCls}">${(s.xgb_probability*100).toFixed(1)}% · ${riskLabel(xgbCls)}</div>
            </div>
          </div>` : '';

        return `
          <div class="scard ${cls} ${activeStation === s.id ? 'active' : ''}" id="card-${s.id}" onclick="activateStation('${s.id}', true)">
            <div class="card-top">
              <div>
                <div class="card-name">${s.station}</div>
                <div class="card-meta">${s.station_id} · ${s.week_start}</div>
              </div>
              <div class="card-risk">
                <div class="card-pct ${cls}">${displayProbability(s)}</div>
                <div class="card-rlabel ${cls}">${riskLabel(cls)}</div>
              </div>
            </div>
            <div class="card-features">
              <div class="feat"><div class="feat-val">${s.sst}</div><div class="feat-label">OISST SST</div></div>
              <div class="feat"><div class="feat-val">${s.ndbc_sst}</div><div class="feat-label">NDBC temp</div></div>
              <div class="feat"><div class="feat-val">${s.wind}</div><div class="feat-label">Wind</div></div>
              <div class="feat"><div class="feat-val">${s.chloro}</div><div class="feat-label">Chloro</div></div>
            </div>
            ${compareRow}
          </div>`;
      }).join('');
    }

    // ── UPDATE MARKERS ──
    function updateMarkers() {
      STATIONS.forEach(s => {
        const cls = displayRiskClass(s);
        const dot = document.getElementById(`dot-${s.id}`);
        if (dot) {
          dot.className = `sdot ${cls}${activeStation === s.id ? ' active' : ''}`;
        }
        markers[s.id].setPopupContent(buildPopup(s));
      });
    }

    // ── UPDATE FILTER COUNTS ──
    function updateCounts() {
      const hi = STATIONS.filter(s => displayRiskClass(s) === 'high').length;
      const med = STATIONS.filter(s => displayRiskClass(s) === 'medium').length;
      const lo = STATIONS.filter(s => displayRiskClass(s) === 'low').length;
      const insufficient = STATIONS.filter(s => displayRiskClass(s) === 'insufficient').length;
      document.getElementById('cnt-all').textContent = STATIONS.length;
      document.getElementById('cnt-high').textContent = hi;
      document.getElementById('cnt-med').textContent = med;
      document.getElementById('cnt-low').textContent = lo;
      document.getElementById('cnt-insufficient').textContent = insufficient;
    }

    // ── MODEL SWITCH ──
    function setModel(model) {
      activeModel = model;
      ['rf','xgb','both'].forEach(m => {
        document.getElementById(`btn-${m}`).classList.toggle('active', m === model);
      });

      // Update perf panel
      const metrics = model === 'xgb' ? DATA.summary.xgb_metrics : DATA.summary.rf_metrics;
      const title = model === 'both' ? 'Model Comparison'
                  : model === 'xgb' ? 'XGBoost Performance'
                  : 'Random Forest Performance';
      document.getElementById('model-perf-title').textContent = title;

      if (model !== 'both') {
        document.getElementById('model-perf-metrics').innerHTML = `
          <div class="mp-metric"><div class="mp-val">${metrics.roc_auc.toFixed(2)}</div><div class="mp-label">AUC-ROC</div></div>
          <div class="mp-metric"><div class="mp-val">${metrics.f1.toFixed(2)}</div><div class="mp-label">F1</div></div>
          <div class="mp-metric"><div class="mp-val">${metrics.recall.toFixed(2)}</div><div class="mp-label">Recall</div></div>
          <div class="mp-metric"><div class="mp-val">${metrics.precision.toFixed(2)}</div><div class="mp-label">Precision</div></div>`;
      } else {
        const rf = DATA.summary.rf_metrics;
        const xgb = DATA.summary.xgb_metrics;
        document.getElementById('model-perf-metrics').innerHTML = `
          <div class="mp-metric"><div class="mp-val" style="font-size:10px">RF ${rf.roc_auc.toFixed(2)}<br>XGB ${xgb.roc_auc.toFixed(2)}</div><div class="mp-label">AUC-ROC</div></div>
          <div class="mp-metric"><div class="mp-val" style="font-size:10px">RF ${rf.f1.toFixed(2)}<br>XGB ${xgb.f1.toFixed(2)}</div><div class="mp-label">F1</div></div>
          <div class="mp-metric"><div class="mp-val" style="font-size:10px">RF ${rf.recall.toFixed(2)}<br>XGB ${xgb.recall.toFixed(2)}</div><div class="mp-label">Recall</div></div>
          <div class="mp-metric"><div class="mp-val" style="font-size:10px">RF ${rf.precision.toFixed(2)}<br>XGB ${xgb.precision.toFixed(2)}</div><div class="mp-label">Precision</div></div>`;
      }

      updateMarkers();
      updateCounts();
      renderList();
    }

    // ── FILTER ──
    function filterStations(filter, tabEl) {
      currentFilter = filter;
      document.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
      tabEl.classList.add('active');
      renderList();
    }

    // ── ACTIVATE STATION ──
    function activateStation(id, panToMap = false) {
      const s = STATIONS.find(x => x.id === id);
      if (!s) return;

      if (activeStation) {
        const oldDot = document.getElementById(`dot-${activeStation}`);
        if (oldDot) oldDot.classList.remove('active');
      }
      activeStation = id;

      const dot = document.getElementById(`dot-${id}`);
      if (dot) dot.classList.add('active');

      renderList();
      setTimeout(() => {
        const card = document.getElementById(`card-${id}`);
        if (card) card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }, 50);

      if (panToMap) map.flyTo([s.latitude, s.longitude], Math.max(map.getZoom(), 9), { duration: 0.8 });
      markers[id].openPopup();
    }

    // ── INIT ──
    renderList();
    // Highlight the highest-risk station in the sidebar without panning the map,
    // so the initial view shows all of California.
    if (STATIONS.length > 0) {
      setTimeout(() => {
        const s = STATIONS[0];
        activeStation = s.id;
        const dot = document.getElementById(`dot-${s.id}`);
        if (dot) dot.classList.add('active');
        renderList();
      }, 600);
    }
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
    return f"{value:.{digits}f} {unit}".strip()


def missing_recent_count(row) -> int:
    return sum(pd.isna(getattr(row, column)) for column in RECENT_DATA_COLUMNS)


def compute_metrics(y_true, y_pred, y_proba) -> dict:
    return {
        "threshold": float(y_proba.mean()),  # placeholder, overwritten below
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, y_proba)),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
    }


def find_threshold(y_true, y_proba) -> float:
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    f1_scores = 2 * precision[:-1] * recall[:-1] / np.maximum(
        precision[:-1] + recall[:-1], 1e-12
    )
    best = float(thresholds[int(np.nanargmax(f1_scores))])
    # Floor at the mean positive-class probability so near-zero thresholds
    # (which occur when XGBoost is overconfident on imbalanced data) are
    # replaced with something semantically meaningful.
    floor = float(np.mean(y_proba[y_true == 1])) if (y_true == 1).any() else 0.2
    return max(best, floor)


def build_preprocessor(numeric: list[str], categorical: list[str]):
    return ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), numeric),
            (
                "cat",
                Pipeline([
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("onehot", OneHotEncoder(handle_unknown="ignore")),
                ]),
                categorical,
            ),
        ]
    )


def build_models(df: pd.DataFrame):
    features = [c for c in df.columns if c not in EXCLUDE_COLUMNS]
    categorical = ["station", "station_id"]
    numeric = [c for c in features if c not in categorical]

    train_mask = df["week_start"] < TRAIN_END
    val_mask = (df["week_start"] >= TRAIN_END) & (df["week_start"] < VALIDATION_END)

    x_train = df.loc[train_mask, features]
    y_train = df.loc[train_mask, TARGET].astype(int)
    x_val = df.loc[val_mask, features]
    y_val = df.loc[val_mask, TARGET].astype(int)

    prep = build_preprocessor(numeric, categorical)

    # ── Random Forest ──
    rf_pipe = Pipeline([
        ("preprocess", build_preprocessor(numeric, categorical)),
        ("model", RandomForestClassifier(
            n_estimators=700, max_depth=6, max_features=0.5,
            min_samples_leaf=10, class_weight="balanced_subsample",
            random_state=42, n_jobs=-1,
        )),
    ])
    rf_pipe.fit(x_train, y_train)
    rf_proba_val = rf_pipe.predict_proba(x_val)[:, 1]
    rf_threshold = find_threshold(y_val, rf_proba_val)
    rf_pred_val = (rf_proba_val >= rf_threshold).astype(int)
    rf_metrics = compute_metrics(y_val, rf_pred_val, rf_proba_val)
    rf_metrics["threshold"] = rf_threshold

    # ── XGBoost ──
    # scale_pos_weight handles class imbalance
    neg_count = int((y_train == 0).sum())
    pos_count = int((y_train == 1).sum())
    scale_pos_weight = neg_count / max(pos_count, 1)

    xgb_pipe = Pipeline([
        ("preprocess", build_preprocessor(numeric, categorical)),
        ("model", XGBClassifier(
            n_estimators=500, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            random_state=42, n_jobs=-1,
            eval_metric="logloss", verbosity=0,
        )),
    ])
    xgb_pipe.fit(x_train, y_train)
    xgb_proba_val = xgb_pipe.predict_proba(x_val)[:, 1]
    xgb_threshold = find_threshold(y_val, xgb_proba_val)
    xgb_pred_val = (xgb_proba_val >= xgb_threshold).astype(int)
    xgb_metrics = compute_metrics(y_val, xgb_pred_val, xgb_proba_val)
    xgb_metrics["threshold"] = xgb_threshold

    return rf_pipe, xgb_pipe, features, rf_metrics, xgb_metrics


def build_dashboard() -> None:
    # Try the merged path first, fall back to root-level file
    data_path = DATA_PATH if DATA_PATH.exists() else ROOT / "hab_ndbc_merged.csv"
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found at {DATA_PATH} or {ROOT / 'hab_ndbc_merged.csv'}")

    df = pd.read_csv(data_path, parse_dates=["week_start", "sample_date"])
    df["station_id"] = df["station_id"].astype(str)

    rf_model, xgb_model, features, rf_metrics, xgb_metrics = build_models(df)

    # Train on the historical merged dataset, but predict on the weekly feed if it exists.
    # This lets the map use the newest rows from scripts/build_weekly_model_feed.py.
    if PREDICTION_FEED_PATH.exists():
        latest = pd.read_csv(PREDICTION_FEED_PATH, parse_dates=["week_start", "sample_date"])
        latest["station_id"] = latest["station_id"].astype(str)
        latest = latest.sort_values("station").copy()
        missing_features = [column for column in features if column not in latest.columns]
        if missing_features:
            raise ValueError(f"Prediction feed is missing model features: {missing_features}")
        print(f"Predicting from weekly feed: {PREDICTION_FEED_PATH}")
    else:
        # Fallback: use the newest historical row per station.
        latest = (
            df.sort_values(["station", "week_start"])
            .groupby("station", as_index=False)
            .tail(1)
            .sort_values("station")
            .copy()
        )
        print(f"Prediction feed not found, using historical data: {data_path}")

    rf_probas  = rf_model.predict_proba(latest[features])[:, 1]
    xgb_probas = xgb_model.predict_proba(latest[features])[:, 1]
    latest["rf_probability"]  = rf_probas
    latest["xgb_probability"] = xgb_probas

    stations = []
    for row in latest.sort_values("rf_probability", ascending=False).itertuples(index=False):
        rf_label, rf_class, rf_pred = risk_label(row.rf_probability, rf_metrics["threshold"])
        missing_count = missing_recent_count(row)
        insufficient_recent_data = missing_count > MAX_MISSING_RECENT_COLUMNS
        lat, lng = STATION_COORDS.get(row.station, (float(row.latitude), float(row.longitude)))
        stations.append({
            "id": row.station.lower().replace(" ", "-"),
            "station": row.station,
            "week_start": row.week_start.date().isoformat(),
            "station_id": f"NDBC-{row.station_id}",
            "latitude": lat,
            "longitude": lng,
            "rf_probability": round(float(row.rf_probability), 4),
            "xgb_probability": round(float(row.xgb_probability), 4),
            "rf_risk_class": rf_class,
            "insufficient_recent_data": insufficient_recent_data,
            "missing_recent_count": missing_count,
            "sst": format_value(row.sst_roll_14d, "C"),
            "ndbc_sst": format_value(row.sea_surface_temp_c, "C"),
            "wind": format_value(row.wind_speed_mps, "m/s"),
            "chloro": format_value(row.avg_chloro, ""),
        })

    rf_thr  = rf_metrics["threshold"]
    xgb_thr = xgb_metrics["threshold"]

    eligible_stations = [s for s in stations if not s["insufficient_recent_data"]]
    high_count   = sum(1 for s in eligible_stations if risk_label(s["rf_probability"], rf_thr)[1] == "high")
    medium_count = sum(1 for s in eligible_stations if risk_label(s["rf_probability"], rf_thr)[1] == "medium")
    low_count    = sum(1 for s in eligible_stations if risk_label(s["rf_probability"], rf_thr)[1] == "low")
    insufficient_count = sum(1 for s in stations if s["insufficient_recent_data"])

    summary = {
        "station_count": len(stations),
        "high_count": high_count,
        "medium_count": medium_count,
        "low_count": low_count,
        "insufficient_count": insufficient_count,
        "latest_update": latest["week_start"].max().date().isoformat(),
        "rf_threshold": rf_thr,
        "xgb_threshold": xgb_thr,
        "rf_metrics": rf_metrics,
        "xgb_metrics": xgb_metrics,
    }

    payload = {"summary": summary, "stations": stations}
    html = Template(HTML_TEMPLATE).render(
        summary=summary,
        stations=stations,
        dashboard_json=json.dumps(payload),
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_PATH}")
    print(f"  RF  AUC-ROC={rf_metrics['roc_auc']:.3f}  F1={rf_metrics['f1']:.3f}  threshold={rf_thr:.3f}")
    print(f"  XGB AUC-ROC={xgb_metrics['roc_auc']:.3f}  F1={xgb_metrics['f1']:.3f}  threshold={xgb_thr:.3f}")
    print(f"  Stations: {len(stations)} total — {high_count} high / {medium_count} medium / {low_count} low / {insufficient_count} insufficient")


if __name__ == "__main__":
    build_dashboard()
