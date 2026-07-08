"""Load the editable YAML settings used by the pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("pipeline_config.yml")


def load_pipeline_settings(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    path = Path(config_path).resolve()
    with path.open("r", encoding="utf-8") as handle:
        settings = yaml.safe_load(handle) or {}

    if not isinstance(settings, dict):
        raise ValueError(f"Pipeline settings file must contain a top-level mapping: {path}")

    settings["_config_dir"] = str(path.parent)
    return settings


def resolve_path(settings: dict[str, Any], value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (Path(settings["_config_dir"]) / path).resolve()


def get_data_raw_path(settings: dict[str, Any]) -> Path:
    paths = settings.get("paths") or {}
    return resolve_path(settings, str(paths.get("data_raw", "../data-raw")))


def get_reference_dataset_path(settings: dict[str, Any]) -> Path:
    paths = settings.get("paths") or {}
    return resolve_path(settings, str(paths.get("reference_dataset", "../reference_dataset")))


def get_pidsmaker_export_path(settings: dict[str, Any]) -> Path:
    paths = settings.get("paths") or {}
    return resolve_path(settings, str(paths.get("pidsmaker_export", "../pidsmaker_export")))


def get_processing_workers(settings: dict[str, Any]) -> int:
    processing = dict(settings.get("processing") or {})
    workers = int(processing.get("workers", 1))
    return max(1, workers)


def get_pidsmaker_window_size_seconds(settings: dict[str, Any]) -> int:
    export_settings = dict(settings.get("pidsmaker_export") or {})
    window_size_seconds = int(export_settings.get("window_size_seconds", 60))
    if window_size_seconds <= 0:
        raise ValueError("pidsmaker_export.window_size_seconds must be a positive integer.")
    return window_size_seconds


def get_pidsmaker_tools(settings: dict[str, Any]) -> list[str] | None:
    export_settings = dict(settings.get("pidsmaker_export") or {})
    tools = export_settings.get("tools")
    if tools is None:
        return None
    if isinstance(tools, str):
        return [tools]
    return [str(tool) for tool in tools]


def get_pipeline_steps(settings: dict[str, Any]) -> dict[str, bool]:
    steps = dict(settings.get("steps") or {})
    return {
        "build_reference_dataset": bool(steps.get("build_reference_dataset", True)),
        "build_splits": bool(steps.get("build_splits", True)),
        "export_to_pidsmaker": bool(steps.get("export_to_pidsmaker", True)),
    }


def get_split_settings(settings: dict[str, Any]) -> dict[str, Any]:
    split_settings = dict(settings.get("splits") or {})
    split_settings.setdefault("validation_has_attacks", False)
    split_settings.setdefault(
        "benign_ratios",
        {
            "train": 0.70,
            "val": 0.15,
            "test": 0.15,
        },
    )
    return split_settings
