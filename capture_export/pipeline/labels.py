"""Load and clean Orange CVE label windows."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from capture_export.pipeline.models import EdgeRecord, RunMetadata
from capture_export.pipeline.time_utils import (
    datetime_text_to_ns,
    decimal_seconds_to_ns,
    ranges_overlap,
    timestamp_event_interval_ns,
)


SCENARIO_ALIASES = {"opensmtpd": "smtpd"}


def normalize_label_scenario(scenario: str) -> str:
    return SCENARIO_ALIASES.get(scenario, scenario)


def split_cve_scenario(scenario: str) -> tuple[str, str] | None:
    marker = "_CVE-"
    if marker not in scenario:
        return None
    scenario_name, cve_suffix = scenario.split(marker, 1)
    return scenario_name, f"CVE-{cve_suffix}"


def load_label_rows(data_raw_dir: str | Path) -> list[dict[str, str]]:
    labels_path = Path(data_raw_dir) / "labels.csv"
    if not labels_path.exists():
        return []

    with labels_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    for row in rows:
        row["scenario"] = normalize_label_scenario(row["scenario"])
    return rows


def label_window_to_ns(row: dict[str, str], tool: str) -> tuple[int, int]:
    if tool == "recap":
        return datetime_text_to_ns(row["cve_start"]), datetime_text_to_ns(row["cve_end"])
    return decimal_seconds_to_ns(row["cve_start_epoch"]), decimal_seconds_to_ns(row["cve_end_epoch"])


def candidate_label_rows(
    metadata: RunMetadata,
    label_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    parsed = split_cve_scenario(metadata.scenario)
    if parsed is None:
        return []

    scenario_name, cve = parsed
    return [
        row
        for row in label_rows
        if row.get("scenario") == scenario_name
        and row.get("cve") == cve
        and row.get("tool") == metadata.tool
    ]


def edge_time_range(edges: list[EdgeRecord]) -> tuple[int, int] | None:
    edge_intervals = [
        timestamp_event_interval_ns(edge.timestamp)
        for edge in edges
        if edge.timestamp is not None
    ]
    if not edge_intervals:
        return None
    return min(start_ns for start_ns, _ in edge_intervals), max(end_ns for _, end_ns in edge_intervals)


def select_attack_windows(
    metadata: RunMetadata,
    edges: list[EdgeRecord],
    label_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    if metadata.kind == "benign":
        return []

    candidates = candidate_label_rows(metadata, label_rows)
    run_range = edge_time_range(edges)
    if not candidates or run_range is None:
        return []

    selected: list[dict[str, Any]] = []
    for row in candidates:
        start_ns, end_ns = label_window_to_ns(row, metadata.tool)
        if not ranges_overlap(start_ns, end_ns, run_range[0], run_range[1]):
            continue

        selected.append(
            {
                "cve": row["cve"],
                "scenario": row["scenario"],
                "tool": row["tool"],
                "start_ns": start_ns,
                "end_ns": end_ns,
                "source": "labels.csv",
                "raw": dict(row),
            }
        )

    return selected
