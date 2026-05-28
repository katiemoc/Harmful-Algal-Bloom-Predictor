# FastAPI backend

This backend gives the project a modular JSON pipeline between any sender, Supabase, and the frontend.
It talks to Supabase through the database REST API

## What it includes

- `POST /api/v1/pipeline/ingest` to receive JSON and insert or upsert it into Supabase
- `POST /api/v1/pipeline/transform` to preview source-specific normalization before writing
- `POST /api/v1/pipeline/query` to fetch rows back for the frontend using JSON filters
- `GET /api/v1/pipeline/sources` to list registered payload transformers
- `GET /api/v1/health` for a quick configuration check

## Structure

```text
backend/
  app/
    api/routes/         # FastAPI route handlers
    core/               # settings and config
    models/             # request/response schemas
    services/           # Supabase and ingestion orchestration
    dependencies.py     # shared dependency providers
    main.py             # app entrypoint
```

## Setup

1. Create an env file:

   ```bash
   cp backend/.env.example backend/.env
   ```

2. Install dependencies:

   ```bash
   pip install -r backend/requirements.txt
   ```

3. Start the API from the `backend/` folder:

   ```bash
   uvicorn app.main:app --reload
   ```

4. Open Swagger UI:

   ```text
   http://127.0.0.1:8000/api/docs
   ```

## Example ingest request

```json
{
  "source": "generic",
  "table": "station_readings",
  "operation": "upsert",
  "upsert_on": ["station_id", "sampled_at"],
  "payload": {
    "station_id": "TRINIDAD_PIER",
    "sampled_at": "2026-05-24T00:00:00Z",
    "chlorophyll": 2.1,
    "risk_level": "medium"
  },
  "metadata": {
    "sender": "frontend-form"
  }
}
```

## Example query request

```json
{
  "table": "station_readings",
  "columns": ["station_id", "sampled_at", "risk_level"],
  "filters": [
    {
      "column": "risk_level",
      "operator": "eq",
      "value": "high"
    }
  ],
  "order_by": "sampled_at",
  "ascending": false,
  "limit": 25
}
```

## Adding a new source transformer

Add a new function in `app/services/transformers.py` and register it:

```python
@transformer_registry.register("buoy_webhook")
def buoy_webhook_transformer(records, metadata):
    normalized = []
    for record in records:
        normalized.append(
            {
                "station_id": record["station"]["id"],
                "sampled_at": record["timestamp"],
                "water_temp_c": record["measurements"]["water_temp_c"],
            }
        )
    return normalized
```

That keeps route logic unchanged while letting each source shape its own payload format.
