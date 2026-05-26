# Harmful-Algal-Bloom-Predictor

## Project Layout

- `data/raw/calhabmap/` - original CalHABMAP station CSV exports.
- `data/processed/calhabmap/` - cleaned CalHABMAP station CSVs.
- `data/processed/oisst/` - processed OISST site-week datasets.
- `data/processed/noaa_pmn/` - NOAA PMN California and matched weekly datasets.
- `data/processed/ndbc/` - processed California NDBC buoy dataset.
- `data/processed/merged/` - modeling-ready merged HAB/OISST datasets.
- `notebooks/` - exploratory and dataset-building notebooks.
- `scripts/` - reusable data-building scripts.

## Weekly Dashboard Update

The GitHub Actions workflow in `.github/workflows/update-weekly-model-feed.yml`
updates the deployed dashboard once per week. It runs every Tuesday, rebuilds the
HAB/OISST/NDBC model feed, uploads `data/processed/model_feed/weekly_model_feed.csv`
to Supabase when secrets are configured, regenerates
`website/dashboard/site/index.html`, and commits the generated files back to the
branch. Vercel can then redeploy the static dashboard from that commit.

Required GitHub repository secrets for Supabase upload:

- `SUPABASE_URL`
- `SUPABASE_KEY` or `SUPABASE_SERVICE_ROLE_KEY`

You can also run it manually from GitHub Actions with the
`Update weekly HAB model feed` workflow.
