"""Small logging helpers for the pipeline."""

from __future__ import annotations

from datetime import datetime


def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {message}", flush=True)
