from __future__ import annotations

import subprocess
import sys
from pathlib import Path


class ModelFeedServiceError(RuntimeError):
    """Raised when a model feed command fails."""


class ModelFeedService:
    def __init__(self) -> None:
        self.root = Path(__file__).resolve().parents[3]

    def build_weekly_feed(
        self,
        start_date: str,
        use_existing: bool,
        output: str | None,
    ) -> dict[str, object]:
        command = [
            sys.executable,
            "scripts/build_weekly_model_feed.py",
            "--start-date",
            start_date,
        ]
        if use_existing:
            command.append("--use-existing")
        if output:
            command.extend(["--output", output])
        return self._run(command, timeout_seconds=900)

    def upload_weekly_feed(
        self,
        feed: str | None,
        table: str,
        batch_size: int,
        dry_run: bool,
    ) -> dict[str, object]:
        command = [
            sys.executable,
            "scripts/upload_model_feed_to_supabase.py",
            "--table",
            table,
            "--batch-size",
            str(batch_size),
        ]
        if feed:
            command.extend(["--feed", feed])
        if dry_run:
            command.append("--dry-run")
        return self._run(command, timeout_seconds=300)

    def rebuild_dashboard(self) -> dict[str, object]:
        return self._run(
            [sys.executable, "website/dashboard/build_dashboard.py"],
            timeout_seconds=300,
        )

    def _run(self, command: list[str], timeout_seconds: int) -> dict[str, object]:
        result = subprocess.run(
            command,
            cwd=self.root,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        response = {
            "command": command,
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        if result.returncode != 0:
            raise ModelFeedServiceError(result.stderr or result.stdout or "Model feed command failed.")
        return response
