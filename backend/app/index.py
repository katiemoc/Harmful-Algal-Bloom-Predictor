"""Legacy compatibility entrypoint for the moved FastAPI backend.

The backend now lives under ``website/backend/app``. Keep this shim so
existing deploy or import paths that still reference ``backend/app/index.py``
continue to resolve the same FastAPI application.
"""

from website.backend.app.main import app
