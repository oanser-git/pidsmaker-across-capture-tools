#!/usr/bin/env python3
"""Generate MeluXina-local RECAP raw ASHA configs for native PIDSMaker methods."""

from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any, Callable

import argparse
import csv

import yaml


DATASET = "ORANGE_RECAP_RAW"
BASE_DIR = Path("meluxina/pidsmaker")
SWEEP_DIR = BASE_DIR / "sweeps" / "asha"
ASHA_DIR = BASE_DIR / "asha"
GRID_DIR = ASHA_DIR / "grids"
DEFAULT_SELECTION_METRIC = "adp_score"
DEFAULT_SELECTION_MODE = "maximize"
DEFAULT_PROMOTION_POLICY = "sync"
DEFAULT_REDUCTION_FACTOR = 2
DEFAULT_ACCOUNT = "p201223"
DEFAULT_SUBMIT_ACCOUNTS = ("p201223", "p201219")
DEFAULT_ARRAY_CONCURRENCY = 100
DEFAULT_SUBMIT_CHUNK_SIZE = 100
DEFAULT_MAX_SUBMIT_JOBS_PER_ACCOUNT = 100
RUNG_EPOCHS = (1, 2, 4, 9)
FINAL_EPOCHS = 12
FINAL_TOP_K = 3
MAGIC_WINDOW_EXPORTS = {
    "1m": ("${ORANGE_EXPORT_ROOT}", 60),
    "2m": ("${P_EDR_ROOT}/capture_export/pidsmaker_export_variants/recap_raw/window_2m", 120),
    "4m": ("${P_EDR_ROOT}/capture_export/pidsmaker_export_variants/recap_raw/window_4m", 240),
}


def token(value: Any) -> str:
    text = f"{value:g}" if isinstance(value, float | int) else str(value)
    return text.replace(".", "p").replace("-", "m")


@dataclass(frozen=True)
class Axis:
    name: str
    values: tuple[Any, ...]
    apply: Callable[[Any], dict[str, Any]]


@dataclass(frozen=True)
class MethodGrid:
    label: str
    method: str
    memory: str
    axes: tuple[Axis, ...]
    default: tuple[Any, ...]
    expected_configs: int


def simple(key: str) -> Callable[[Any], dict[str, Any]]:
    return lambda value: {key: value}


def model_dim(value: Any) -> dict[str, Any]:
    dim = int(value)
    return {"training.node_hid_dim": dim, "training.node_out_dim": dim}


def magic_window(value: Any) -> dict[str, Any]:
    name = str(value)
    export_root, seconds = MAGIC_WINDOW_EXPORTS[name]
    return {
        "__run__": {
            "export_variant": f"window_{name}",
            "export_window_size_seconds": seconds,
            "export_root": export_root,
        }
    }


def orthrus_capacity(value: Any) -> dict[str, Any]:
    profiles = {
        "default": {"training.node_out_dim": 64, "featurization.emb_dim": 128},
        "medium": {"training.node_out_dim": 128, "featurization.emb_dim": 128},
        "large": {"training.node_out_dim": 256, "featurization.emb_dim": 256},
    }
    return profiles[str(value)]


def kairos_capacity(value: Any) -> dict[str, Any]:
    profiles = {
        "small": 64,
        "default": 100,
        "large": 128,
    }
    dim = profiles[str(value)]
    return {
        "training.node_hid_dim": dim,
        "training.node_out_dim": dim,
        "training.encoder.tgn.tgn_memory_dim": dim,
        "training.encoder.tgn.tgn_time_dim": dim,
    }


def tgn_dim(value: Any) -> dict[str, Any]:
    dim = int(value)
    return {
        "training.encoder.tgn.tgn_memory_dim": dim,
        "training.encoder.tgn.tgn_time_dim": dim,
    }


METHODS: tuple[MethodGrid, ...] = (
    MethodGrid(
        label="magic",
        method="magic",
        memory="96G",
        axes=(
            Axis("win", tuple(MAGIC_WINDOW_EXPORTS.keys()), magic_window),
            Axis("lr", (0.0003, 0.001, 0.003), simple("training.lr")),
            Axis("wd", (0.0001, 0.0005, 0.001), simple("training.weight_decay")),
            Axis("dim", (32, 64, 128), model_dim),
            Axis(
                "mask",
                (0.3, 0.5, 0.7),
                simple("training.decoder.reconstruct_masked_features.mask_rate"),
            ),
        ),
        default=("1m", 0.001, 0.0005, 64, 0.5),
        expected_configs=243,
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
            Axis("batch", (512, 1024, 2048), simple("batching.intra_graph_batching.edges.intra_graph_batch_size")),
        ),
        default=(0.0001, 0.00001, 128, 128, 1024),
        expected_configs=243,
    ),
    MethodGrid(
        label="orthrus",
        method="orthrus",
        memory="320G",
        axes=(
            Axis("lr", (0.00001, 0.00003, 0.0001), simple("training.lr")),
            Axis("wd", (0.0, 0.000001, 0.00001), simple("training.weight_decay")),
            Axis("cap", ("default", "medium", "large"), orthrus_capacity),
            Axis("batch", (512, 1024, 2048), simple("batching.intra_graph_batching.edges.intra_graph_batch_size")),
            Axis("neigh", (10, 20, 50), simple("batching.intra_graph_batching.tgn_last_neighbor.tgn_neighbor_size")),
        ),
        default=(0.00001, 0.00001, "default", 1024, 20),
        expected_configs=243,
    ),
    MethodGrid(
        label="kairos",
        method="kairos",
        memory="180G",
        axes=(
            Axis("lr", (0.00002, 0.00005, 0.0001), simple("training.lr")),
            Axis("wd", (0.001, 0.01, 0.1), simple("training.weight_decay")),
            Axis("cap", ("small", "default", "large"), kairos_capacity),
            Axis("batch", (512, 1024, 2048), simple("batching.intra_graph_batching.edges.intra_graph_batch_size")),
            Axis("neigh", (10, 20, 50), simple("batching.intra_graph_batching.tgn_last_neighbor.tgn_neighbor_size")),
        ),
        default=(0.00005, 0.01, "default", 1024, 20),
        expected_configs=243,
    ),
)

RUNG_WALLTIMES = {
    "magic": ("00:45:00", "01:00:00", "02:00:00", "04:00:00"),
    "velox": ("00:45:00", "01:00:00", "02:00:00", "03:30:00"),
    "orthrus": ("01:00:00", "01:30:00", "03:00:00", "06:00:00"),
    "kairos": ("01:00:00", "01:30:00", "03:00:00", "06:00:00"),
}
FINAL_WALLTIMES = {
    "magic": "06:00:00",
    "velox": "06:00:00",
    "orthrus": "12:00:00",
    "kairos": "12:00:00",
}


def combo_name(index: int, grid: MethodGrid, combo: tuple[Any, ...]) -> str:
    parts = [f"c{index:03d}"]
    for axis, value in zip(grid.axes, combo):
        parts.append(f"{axis.name}{token(value)}")
    parts.append("seed0")
    return "_".join(parts)


def combo_payload(grid: MethodGrid, combo: tuple[Any, ...]) -> dict[str, Any]:
    overrides: dict[str, Any] = {"training.seed": 0, "featurization.seed": 0}
    run_fields: dict[str, Any] = {}
    for axis, value in zip(grid.axes, combo):
        payload = dict(axis.apply(value))
        run_fields.update(dict(payload.pop("__run__", {})))
        overrides.update(payload)
    return {**run_fields, "overrides": overrides}


def ordered_combos(grid: MethodGrid) -> list[tuple[Any, ...]]:
    all_combos = list(product(*(axis.values for axis in grid.axes)))
    return [grid.default] + [combo for combo in all_combos if combo != grid.default]


def checkpoint_dir(grid: MethodGrid, rung: int, epochs: int) -> str:
    return f"${{P_EDR_ROOT}}/meluxina/pidsmaker/asha_runs/{grid.label}_recap_raw_100/r{rung}_e{epochs}_checkpoints"


def final_checkpoint_dir(grid: MethodGrid) -> str:
    return f"${{P_EDR_ROOT}}/meluxina/pidsmaker/asha_runs/{grid.label}_recap_raw_100/final_e{FINAL_EPOCHS}_checkpoints"


def base_sweep(
    grid: MethodGrid,
    rung: int,
    epochs: int,
    selection_metric: str,
    previous_rung: int | None = None,
    previous_epochs: int | None = None,
) -> dict[str, Any]:
    name = f"orange_recap_raw_{grid.label}_100_asha_r{rung}_e{epochs}"
    sweep = {
        "name": name,
        "repo_root": "${P_EDR_ROOT}/external/PIDSMaker",
        "artifact_root": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/artifacts/asha/{grid.label}",
        "results_dir": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/results/asha/{name}",
        "checkpoint_dir": checkpoint_dir(grid, rung, epochs),
        "selection_metric": selection_metric,
        "completion_metric": selection_metric,
        "method": grid.method,
        "dataset": DATASET,
        "epochs": epochs,
    }
    if previous_rung is not None and previous_epochs is not None:
        sweep["resume_checkpoint_dir"] = checkpoint_dir(grid, previous_rung, previous_epochs)
    return sweep


def write_sweep(
    grid: MethodGrid,
    rung: int,
    epochs: int,
    runs: list[dict[str, Any]],
    selection_metric: str,
    previous_rung: int | None = None,
    previous_epochs: int | None = None,
) -> Path:
    sweep = base_sweep(grid, rung, epochs, selection_metric, previous_rung, previous_epochs)
    sweep["runs"] = runs
    path = SWEEP_DIR / f"{grid.label}_recap_raw_100_asha_r{rung}_e{epochs}.yml"
    path.write_text(yaml.safe_dump(sweep, sort_keys=False), encoding="utf-8")
    return path


def write_final_sweep(grid: MethodGrid, runs: list[dict[str, Any]], selection_metric: str) -> Path:
    name = f"orange_recap_raw_{grid.label}_100_final_e{FINAL_EPOCHS}"
    sweep = {
        "name": name,
        "repo_root": "${P_EDR_ROOT}/external/PIDSMaker",
        "artifact_root": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/artifacts/final/{grid.label}",
        "results_dir": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/results/final/{name}",
        "checkpoint_dir": final_checkpoint_dir(grid),
        "selection_metric": selection_metric,
        "completion_metric": selection_metric,
        "method": grid.method,
        "dataset": DATASET,
        "epochs": FINAL_EPOCHS,
        "runs": runs,
    }
    path = SWEEP_DIR / f"{grid.label}_recap_raw_100_final_e{FINAL_EPOCHS}.yml"
    path.write_text(yaml.safe_dump(sweep, sort_keys=False), encoding="utf-8")
    return path


def write_grid_csv(grid: MethodGrid, combos: list[tuple[Any, ...]]) -> Path:
    path = GRID_DIR / f"{grid.label}_recap_raw_100_grid.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["index", "name", *[axis.name for axis in grid.axes]])
        for index, combo in enumerate(combos):
            writer.writerow([index, combo_name(index, grid, combo), *combo])
    return path


def write_asha_config(
    grid: MethodGrid,
    rung_paths: list[Path],
    final_sweep_path: Path,
    selection_metric: str,
    selection_mode: str,
    promotion_policy: str,
    account: str,
    submit_accounts: list[str],
    array_concurrency: int,
    submit_chunk_size: int,
    max_submit_jobs_per_account: int,
) -> Path:
    rungs = []
    rung_walltimes = RUNG_WALLTIMES[grid.label]
    for rung, (epochs, path) in enumerate(zip(RUNG_EPOCHS, rung_paths)):
        rungs.append(
            {
                "name": f"r{rung}_e{epochs}",
                "sweep": f"${{P_EDR_ROOT}}/{path.as_posix()}",
                "results_dir": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/asha_runs/{grid.label}_recap_raw_100/r{rung}_e{epochs}",
                "tag": f"{grid.label}_recap_raw100_r{rung}_e{epochs}",
                "job_name": f"{grid.label}_recap_raw100_r{rung}",
                "sbatch_options": {"time": rung_walltimes[rung]},
            }
        )

    config = {
        "name": f"{grid.label}_recap_raw_100_asha",
        "results_root": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/asha_runs/{grid.label}_recap_raw_100",
        "metric": selection_metric,
        "mode": selection_mode,
        "promotion_policy": promotion_policy,
        "reduction_factor": DEFAULT_REDUCTION_FACTOR,
        "poll_seconds": 120,
        "submit_accounts": submit_accounts,
        "submit_chunk_size": submit_chunk_size,
        "max_submit_jobs_per_account": max_submit_jobs_per_account,
        "array_concurrency": array_concurrency,
        "sbatch_script": "${P_EDR_ROOT}/meluxina/pidsmaker/run_array.sbatch",
        "sbatch_options": {
            "account": account,
            "partition": "gpu",
            "qos": "default",
            "mem": grid.memory,
            "gres": "gpu:1",
            "cpus_per_task": 7,
            "output": "${P_EDR_ROOT}/run_logs/%x-%A_%a.out",
            "error": "${P_EDR_ROOT}/run_logs/%x-%A_%a.err",
        },
        "export_env": {
            "MELUXINA_PIDSMAKER_IMAGE": "${P_EDR_ROOT}/containers/pidsmaker-pids-psycopg2.sif",
            "MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE": 1,
        },
        "rungs": rungs,
        "final": {
            "name": f"final_e{FINAL_EPOCHS}",
            "source_rung": f"r{len(RUNG_EPOCHS) - 1}_e{RUNG_EPOCHS[-1]}",
            "top_k": FINAL_TOP_K,
            "sweep": f"${{P_EDR_ROOT}}/{final_sweep_path.as_posix()}",
            "results_dir": f"${{P_EDR_ROOT}}/meluxina/pidsmaker/asha_runs/{grid.label}_recap_raw_100/final_e{FINAL_EPOCHS}",
            "tag": f"{grid.label}_recap_raw100_final_e{FINAL_EPOCHS}",
            "job_name": f"{grid.label}_recap_raw100_final_e{FINAL_EPOCHS}",
            "export_env": {"MELUXINA_PIDSMAKER_PHASE": "final"},
            "sbatch_options": {"time": FINAL_WALLTIMES[grid.label]},
        },
    }
    path = ASHA_DIR / f"{grid.label}_recap_raw_100_asha.yml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metric", default=DEFAULT_SELECTION_METRIC)
    parser.add_argument("--mode", choices=["minimize", "maximize"], default=DEFAULT_SELECTION_MODE)
    parser.add_argument("--promotion-policy", choices=["async", "sync"], default=DEFAULT_PROMOTION_POLICY)
    parser.add_argument("--account", default=DEFAULT_ACCOUNT)
    parser.add_argument("--submit-accounts", default=",".join(DEFAULT_SUBMIT_ACCOUNTS))
    parser.add_argument("--array-concurrency", type=int, default=DEFAULT_ARRAY_CONCURRENCY)
    parser.add_argument("--submit-chunk-size", type=int, default=DEFAULT_SUBMIT_CHUNK_SIZE)
    parser.add_argument("--max-submit-jobs-per-account", type=int, default=DEFAULT_MAX_SUBMIT_JOBS_PER_ACCOUNT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    submit_accounts = [item.strip() for item in args.submit_accounts.split(",") if item.strip()]
    if not submit_accounts:
        raise RuntimeError("At least one submit account is required")
    SWEEP_DIR.mkdir(parents=True, exist_ok=True)
    ASHA_DIR.mkdir(parents=True, exist_ok=True)
    GRID_DIR.mkdir(parents=True, exist_ok=True)

    for grid in METHODS:
        combos = ordered_combos(grid)
        if len(combos) != grid.expected_configs:
            raise RuntimeError(f"{grid.label} has {len(combos)} configs, expected {grid.expected_configs}")
        runs = [
            {"name": combo_name(index, grid, combo), **combo_payload(grid, combo)}
            for index, combo in enumerate(combos)
        ]
        rung_paths = []
        for rung, epochs in enumerate(RUNG_EPOCHS):
            previous_rung = rung - 1 if rung > 0 else None
            previous_epochs = RUNG_EPOCHS[rung - 1] if rung > 0 else None
            rung_paths.append(write_sweep(grid, rung, epochs, runs, args.metric, previous_rung, previous_epochs))
        final_sweep_path = write_final_sweep(grid, runs, args.metric)
        csv_path = write_grid_csv(grid, combos)
        asha_path = write_asha_config(
            grid,
            rung_paths,
            final_sweep_path,
            args.metric,
            args.mode,
            args.promotion_policy,
            args.account,
            submit_accounts,
            args.array_concurrency,
            args.submit_chunk_size,
            args.max_submit_jobs_per_account,
        )
        print(
            f"{grid.label}: {len(runs)} runs, rungs={', '.join(str(path) for path in rung_paths)}, final={final_sweep_path}, "
            f"grid={csv_path}, asha={asha_path}"
        )


if __name__ == "__main__":
    main()
