# CLAUDE.md — Harmful Algal Bloom Predictor

## Design System
Always read `DESIGN.md` before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.

Key rules:
- Fonts: Fraunces (display) + DM Sans (body) + DM Mono (data/metrics) — no Inter, no system-ui
- Risk colors are fixed: `#2f8f68` low / `#c4881a` medium / `#c63d3d` high
- Landing page hero is dark (`#0b1920`); dashboard and body content are light
- Dark topbar (`#10202a`) on the dashboard for visual continuity with the landing page
- In QA mode, flag any code that doesn't match DESIGN.md

## Project Structure
- `dashboard/build_dashboard.py` — Python script that trains the Random Forest model and renders the dashboard HTML via Jinja2
- `dashboard/site/index.html` — Built dashboard output (do not edit directly, regenerate via build script)
- `DESIGN.md` — Design system source of truth
- `hab_ndbc_merged.csv` — Merged HAB + NDBC dataset (root level)
- `notebooks/` — Jupyter notebooks for model development
- `random_forest_hab_ndbc.ipynb` — Random Forest model notebook

## Models
- **Random Forest** — implemented in `dashboard/build_dashboard.py` (RandomForestClassifier, 700 estimators, max_depth=6)
- **XGBoost** — to be added alongside Random Forest; website should support both

## Website Architecture (planned)
- **Landing page** — `website/index.html` (to be built): showcases importance of HAB prediction, links to dashboard
- **Dashboard** — `dashboard/site/index.html`: redesigned with Leaflet.js geo navigation and new design system

## Data
- Stations: 10 CalHABMAP stations (Trinidad Pier to Scripps Pier)
- Target: `isHarmful` (binary classification)
- Key features: SST, sea_surface_temp_c, wind_speed_mps, avg_chloro, silicate, nitrate, lag features
- Training/validation split: pre-2025-01-01 / 2025–2026

## Skill Routing
When the user's request matches an available skill, invoke it via the Skill tool.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Architecture/technical design → invoke /plan-eng-review
- Design system/visual changes → read DESIGN.md first, then /plan-design-review
- Bugs/errors → invoke /investigate
- QA/testing → invoke /qa or /qa-only
- Code review → invoke /review
- Ship/deploy/PR → invoke /ship
