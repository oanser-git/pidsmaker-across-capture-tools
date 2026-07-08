#!/usr/bin/env python3
"""Generate RECAP raw 100% ASHA sweeps for PIDSMaker methods.

The generated rung files keep 81 deterministic run names per method so the ASHA
controller can promote by name from epoch-1 to epoch-3 and epoch-9 rungs.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any, Callable

import argparse
import csv

import yaml


DATASET = "ORANGE_RECAP_RAW"
OUT_DIR = Path("pidsmaker_experiments/experiments/pidsmaker")
ASHA_DIR = Path("pidsmaker_experiments/experiments/asha")
GRID_DIR = ASHA_DIR / "grids"
DEFAULT_SELECTION_METRIC = "adp_score"
DEFAULT_SELECTION_MODE = "maximize"


def token(value: float | int) -> str:
    text = f"{value:g}"
    return text.replace(".", "p").replace("-", "m")


@dataclass(frozen=True)
class Axis:
    name: str
    values: tuple[float | int, ...]
    apply: Callable[[float | int], dict[str, Any]]


@dataclass(frozen=True)
class MethodGrid:
    label: str
    method: str
    memory: str
    axes: tuple[Axis, ...]
    default: tuple[float | int, ...]


def simple(key: str) -> Callable[[float | int], dict[str, Any]]:
    return lambda value: {key: value}


def model_dim(value: float | int) -> dict[str, Any]:
    dim = int(value)
    return {"training.node_hid_dim": dim, "training.node_out_dim": dim}


def node_out_dim(value: float | int) -> dict[str, Any]:
    return {"training.node_out_dim": int(value)}


def tgn_dim(value: float | int) -> dict[str, Any]:
    dim = int(value)
    return {
        "training.encoder.tgn.tgn_memory_dim": dim,
        "training.encoder.tgn.tgn_time_dim": dim,
    }


METHODS: tuple[MethodGrid, ...] = (
    MethodGrid(
        label="magic",
        method="magic_paper",
        memory="96G",
        axes=(
            Axis("lr", (0.0003, 0.001, 0.003), simple("training.lr")),
            Axis("wd", (0.0001, 0.0005, 0.001), simple("training.weight_decay")),
            Axis("dim", (32, 64, 128), model_dim),
            Axis(
                "mask",
                (0.3, 0.5, 0.7),
                simple("training.decoder.reconstruct_masked_features.mask_rate"),
            ),
        ),
        default=(0.001, 0.0005, 64, 0.5),
    ),
    MethodGrid(
        label="velox",
        method="velox",
        memory="180G",
        axes=(
            Axis("lr", (0.0001, 0.0003, 0.001), simple("training.lr")),
            Axis("wd", (0.0, 0.000001, 0.00001), simple("training.weight_decay")),
            Axis("dim", (64, 128, 256), model_dim),
            Axis("emb", (64, 128, 256), simple("featurization.emb_dim")),
        ),
        default=(0.0001, 0.00001, 128, 128),
    ),
    MethodGrid(
        label="orthrus",
        method="orthrus_paper",
        memory="320G",
        axes=(
            Axis("lr", (0.00001, 0.00003, 0.0001), simple("training.lr")),
            Axis("wd", (0.0, 0.000001, 0.00001), simple("training.weight_decay")),
            Axis("out", (64, 128, 256), node_out_dim),
            Axis("emb", (64, 128, 256), simple("featurization.emb_dim")),
        ),
        default=(0.00001, 0.00001, 64, 128),
    ),
    MethodGrid(
        label="kairos",
        method="kairos",
        memory="180G",
        axes=(
            Axis("lr", (0.00002, 0.00005, 0.0001), simple("training.lr")),
            Axis("wd", (0.001, 0.01, 0.1), simple("training.weight_decay")),
            Axis("dim", (64, 100, 128), model_dim),
            Axis("tgn", (64, 100, 128), tgn_dim),
        ),
        default=(0.00005, 0.01, 100, 100),
    ),
)

RUNG_WALLTIMES = {
    "magic": ("00:35:00", "01:00:00", "02:30:00"),
    "velox": ("00:35:00", "00:55:00", "02:20:00"),
    "orthrus": ("00:40:00", "01:10:00", "03:05:00"),
    "kairos": ("00:45:00", "01:15:00", "03:20:00"),
}


def combo_name(index: int, grid: MethodGrid, combo: tuple[float | int, ...]) -> str:
    parts = [f"c{index:03d}"]
    for axis, value in zip(grid.axes, combo):
        parts.append(f"{axis.name}{token(value)}")
    parts.append("seed0")
    return "_".join(parts)


def combo_overrides(grid: MethodGrid, combo: tuple[float | int, ...]) -> dict[str, Any]:
    overrides: dict[str, Any] = {"training.seed": 0}
    for axis, value in zip(grid.axes, combo):
        overrides.update(axis.apply(value))
    return overrides


def ordered_combos(grid: MethodGrid) -> list[tuple[float | int, ...]]:
    all_combos = list(product(*(axis.values for axis in grid.axes)))
    return [grid.default] + [combo for combo in all_combos if combo != grid.default]


def base_sweep(grid: MethodGrid, rung: int, epochs: int, selection_metric: str) -> dict[str, Any]:
    name = f"orange_recap_raw_{grid.label}_100_asha_r{rung}_e{epochs}"
    return {
        "name": name,
        "repo_root": "${P_EDR_ROOT}/external/PIDSMaker",
        "artifact_root": f"${{P_EDR_ROOT}}/artifacts/pidsmaker/{grid.label}",
        "log_root": "${P_EDR_ROOT}/run_logs",
        "results_dir": f"${{P_EDR_ROOT}}/pidsmaker_experiments/hpo_results/{name}",
        "selection_metric": selection_metric,
        "completion_metric": selection_metric,
        "method": grid.method,
        "dataset": DATASET,
        "epochs": epochs,
    }


def write_sweep(grid: MethodGrid, rung: int, epochs: int, runs: list[dict[str, Any]], selection_metric: str) -> Path:
    sweep = base_sweep(grid, rung, epochs, selection_metric)
    sweep["runs"] = runs
    path = OUT_DIR / f"{grid.label}_recap_raw_100_asha_r{rung}_e{epochs}.yml"
    path.write_text(yaml.safe_dump(sweep, sort_keys=False), encoding="utf-8")
    return path


def write_final_sweep(grid: MethodGrid, runs: list[dict[str, Any]], selection_metric: str) -> Path:
    name = f"orange_recap_raw_{grid.label}_100_final_test_e12"
    sweep = {
        "name": name,
        "repo_root": "${P_EDR_ROOT}/external/PIDSMaker",
        "artifact_root": f"${{P_EDR_ROOT}}/artifacts/pidsmaker/{grid.label}",
        "log_root": "${P_EDR_ROOT}/run_logs",
        "results_dir": f"${{P_EDR_ROOT}}/pidsmaker_experiments/hpo_results/{name}",
        "selection_metric": selection_metric,
        "completion_metric": selection_metric,
        "method": grid.method,
        "dataset": DATASET,
        "epochs": 12,
        "runs": runs,
    }
    path = OUT_DIR / f"{grid.label}_recap_raw_100_final_test_e12.yml"
    path.write_text(yaml.safe_dump(sweep, sort_keys=False), encoding="utf-8")
    return path


def write_grid_csv(grid: MethodGrid, combos: list[tuple[float | int, ...]]) -> Path:
    path = GRID_DIR / f"{grid.label}_recap_raw_100_grid.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["index", "name", *[axis.name for axis in grid.axes]])
        for index, combo in enumerate(combos):
            writer.writerow([index, combo_name(index, grid, combo), *combo])
    return path


def write_asha_config(grid: MethodGrid, rung_paths: list[Path], selection_metric: str, selection_mode: str) -> Path:
    rungs = []
    rung_walltimes = RUNG_WALLTIMES[grid.label]
    for rung, (epochs, path) in enumerate(zip((1, 3, 9), rung_paths)):
        sbatch_options = {"time": rung_walltimes[rung]}
        if rung == 2 and grid.label == "kairos":
            sbatch_options.update({"mem": "128G"})
        rung_config = {
            "name": f"r{rung}_e{epochs}",
            "sweep": f"${{P_EDR_ROOT}}/{path.as_posix()}",
            "results_dir": f"${{P_EDR_ROOT}}/pidsmaker_experiments/asha_runs/{grid.label}_recap_raw_100/r{rung}_e{epochs}",
            "tag": f"{grid.label}_recap_raw100_r{rung}_e{epochs}",
            "job_name": f"{grid.label}_recap_raw100_r{rung}",
            "sbatch_options": sbatch_options,
        }
        rungs.append(rung_config)
    config = {
        "name": f"{grid.label}_recap_raw_100_asha",
        "results_root": f"${{P_EDR_ROOT}}/pidsmaker_experiments/asha_runs/{grid.label}_recap_raw_100",
        "metric": selection_metric,
        "mode": selection_mode,
        "promotion_policy": "sync",
        "reduction_factor": 3,
        "poll_seconds": 120,
        "array_concurrency": 81,
        "sbatch_options": {
            "mem": grid.memory,
            "gres": "gpu:1",
            "cpus_per_task": 7,
        },
        "rungs": rungs,
    }
    path = ASHA_DIR / f"{grid.label}_recap_raw_100_asha.yml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metric", default=DEFAULT_SELECTION_METRIC)
    parser.add_argument("--mode", choices=["minimize", "maximize"], default=DEFAULT_SELECTION_MODE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ASHA_DIR.mkdir(parents=True, exist_ok=True)
    GRID_DIR.mkdir(parents=True, exist_ok=True)

    for grid in METHODS:
        combos = ordered_combos(grid)
        if len(combos) != 81:
            raise RuntimeError(f"{grid.label} has {len(combos)} configs, expected 81")
        runs = [
            {"name": combo_name(index, grid, combo), "overrides": combo_overrides(grid, combo)}
            for index, combo in enumerate(combos)
        ]
        rung_paths = [write_sweep(grid, rung, epochs, runs, args.metric) for rung, epochs in enumerate((1, 3, 9))]
        final_path = write_final_sweep(grid, runs, args.metric)
        csv_path = write_grid_csv(grid, combos)
        asha_path = write_asha_config(grid, rung_paths, args.metric, args.mode)
        print(
            f"{grid.label}: {len(runs)} runs, rungs={', '.join(str(p) for p in rung_paths)}, "
            f"final={final_path}, grid={csv_path}, asha={asha_path}"
        )


if __name__ == "__main__":
    main()
