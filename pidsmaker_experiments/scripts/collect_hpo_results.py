#!/usr/bin/env python3
"""Collect HPO task JSON files into CSV/JSON leaderboards."""

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_METRIC = "adp_score"
DEFAULT_MODE = "maximize"


def load_rows(results_dir: Path) -> List[Dict[str, object]]:
    rows = []
    for path in sorted(results_dir.glob("*.json")):
        if path.name in {"results.json", "leaderboard.json"}:
            continue
        rows.append(json.loads(path.read_text()))
    return rows


def write_table(path: Path, rows: List[Dict[str, object]], keys: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def metric_value(row: Dict[str, object], metric: str) -> Optional[float]:
    value = row.get(metric)
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def rank_rows(rows: List[Dict[str, object]], metric: str, mode: str) -> List[Dict[str, object]]:
    reverse = mode == "maximize"

    def score(row: Dict[str, object]) -> float:
        value = metric_value(row, metric)
        if value is None:
            return float("-inf") if reverse else float("inf")
        return value

    return sorted(rows, key=score, reverse=reverse)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--metric", default=DEFAULT_METRIC)
    parser.add_argument("--mode", choices=["minimize", "maximize"], default=DEFAULT_MODE)
    args = parser.parse_args()

    rows = load_rows(args.results_dir.resolve())
    leaderboard = rank_rows(rows, args.metric, args.mode)
    keys = ["name", "phase", "exit_code", "oom", "duration_sec", "node", "artifact", "best_epoch", "best_val_loss", "test_epoch", "test_loss", "adp_score", "precision", "recall", "tp", "fp", "tn", "fn"]
    keys += sorted({key for row in rows for key in row if key not in keys and key != "overrides"})
    args.results_dir.mkdir(parents=True, exist_ok=True)
    (args.results_dir / "results.json").write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    (args.results_dir / "leaderboard.json").write_text(json.dumps(leaderboard, indent=2, sort_keys=True), encoding="utf-8")
    write_table(args.results_dir / "results.csv", rows, keys)
    write_table(args.results_dir / "leaderboard.csv", leaderboard, keys)

    good = [row for row in leaderboard if row.get("phase") == "hpo" and row.get("exit_code") == 0 and metric_value(row, args.metric) is not None]
    print(f"rows={len(rows)} good_hpo={len(good)} metric={args.metric} mode={args.mode}")
    if good:
        print(f"best={good[0]['name']} {args.metric}={good[0][args.metric]}")


if __name__ == "__main__":
    main()
