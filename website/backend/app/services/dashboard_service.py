from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


class DashboardDataError(RuntimeError):
    """Raised when the dashboard payload cannot be loaded."""


class DashboardService:
    def __init__(self) -> None:
        project_root = Path(__file__).resolve().parents[4]
        self.dashboard_site_dir = project_root / "website" / "dashboard" / "site"
        self.dashboard_json_path = self.dashboard_site_dir / "dashboard_data.json"
        self.dashboard_html_path = self.dashboard_site_dir / "index.html"

    def get_payload(self) -> dict[str, Any]:
        payload = self._load_from_json_file()
        if payload is not None:
            return payload

        payload = self._load_from_embedded_html()
        if payload is not None:
            return payload

        raise DashboardDataError(
            "Dashboard data is unavailable. Expected website/dashboard/site/dashboard_data.json "
            "or embedded dashboard-data JSON in website/dashboard/site/index.html."
        )

    def _load_from_json_file(self) -> dict[str, Any] | None:
        if not self.dashboard_json_path.exists():
            return None

        return self._parse_payload(self.dashboard_json_path.read_text(encoding="utf-8"))

    def _load_from_embedded_html(self) -> dict[str, Any] | None:
        if not self.dashboard_html_path.exists():
            return None

        html = self.dashboard_html_path.read_text(encoding="utf-8")
        match = re.search(
            r'<script type="application/json"\s+id="dashboard-data">(.+?)</script>',
            html,
            flags=re.DOTALL,
        )
        if match is None:
            return None

        return self._parse_payload(match.group(1).strip())

    @staticmethod
    def _parse_payload(raw_payload: str) -> dict[str, Any]:
        parsed = json.loads(raw_payload)
        if not isinstance(parsed, dict):
            raise DashboardDataError("Dashboard payload must be a JSON object.")
        return parsed
