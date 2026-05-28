from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEED_PATH = ROOT / "data" / "processed" / "model_feed" / "weekly_model_feed.csv"
DEFAULT_TABLE = "hab_model_feed"
INTEGER_COLUMNS = {"month", "year", "potential_bloom", "above_avg", "isharmful"}


def load_dotenv(path: Path) -> None:
    """Load local .env values without requiring an extra dependency.

    This keeps SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY out of source code
    while still letting the upload script run from your laptop.
    """
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def clean_value(value: Any) -> Any:
    """Convert pandas/numpy missing values into JSON-safe None values."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    return value


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Turn the model feed dataframe into JSON records for Supabase."""
    records = []
    for row in df.to_dict(orient="records"):
        record = {}
        for key, value in row.items():
            value = clean_value(value)
            if value is not None and key in INTEGER_COLUMNS:
                value = int(value)
            if value is not None and key == "station_id":
                value = str(value)
            record[key] = value
        records.append(record)
    return records


def chunked(records: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """Split records into smaller upload batches to avoid large HTTP requests."""
    return [records[start : start + size] for start in range(0, len(records), size)]


def upsert_records(
    supabase_url: str,
    service_role_key: str,
    table: str,
    records: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Upload records into Supabase, updating matching station-week rows.

    The table needs a unique constraint on (station, week_start), which lets
    Supabase update an existing week instead of creating duplicate rows.
    """
    base_url = supabase_url.rstrip("/")
    if base_url.endswith("/rest/v1"):
        base_url = base_url.removesuffix("/rest/v1")
    endpoint = f"{base_url}/rest/v1/{table}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    params = {"on_conflict": "station,week_start"}

    for batch_number, batch in enumerate(chunked(records, batch_size), start=1):
        response = requests.post(endpoint, headers=headers, params=params, json=batch, timeout=120)
        if not response.ok:
            raise SystemExit(
                f"Supabase upload failed for batch {batch_number}: "
                f"{response.status_code} {response.text}"
            )
        print(f"Uploaded batch {batch_number}: {len(batch)} rows")


def parse_args() -> argparse.Namespace:
    """Read command-line options for the feed path, table, and upload behavior."""
    parser = argparse.ArgumentParser(description="Upload the HAB weekly model feed CSV to Supabase.")
    parser.add_argument("--feed", default=str(DEFAULT_FEED_PATH), help="CSV file to upload.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Supabase table name.")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per upload request.")
    parser.add_argument("--dry-run", action="store_true", help="Validate the CSV/env but do not upload.")
    return parser.parse_args()


def main() -> None:
    """Load the weekly feed CSV and upsert it into the Supabase table."""
    args = parse_args()
    load_dotenv(ROOT / "backend" / ".env")
    load_dotenv(ROOT / ".env")

    supabase_url = os.environ.get("SUPABASE_URL")
    service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not supabase_url or not service_role_key:
        raise SystemExit("Missing SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY in .env/backend/.env")

    feed_path = Path(args.feed)
    if not feed_path.exists():
        raise SystemExit(f"Could not find feed CSV: {feed_path}")

    df = pd.read_csv(feed_path)
    df = df.rename(columns={"isHarmful": "isharmful"})
    records = dataframe_to_records(df)
    print(f"Prepared {len(records)} rows from {feed_path}")
    print(f"Target table: {args.table}")

    if args.dry_run:
        print("Dry run only. No rows were uploaded.")
        return

    upsert_records(
        supabase_url=supabase_url,
        service_role_key=service_role_key,
        table=args.table,
        records=records,
        batch_size=args.batch_size,
    )
    print("Upload complete.")


if __name__ == "__main__":
    main()
