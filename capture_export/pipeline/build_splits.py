"""Create scenario-based train/validation/test split files per provenance tool."""

from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
from typing import Any

from capture_export.pipeline.logging_utils import log_message
from capture_export.pipeline.settings import (
    DEFAULT_CONFIG_PATH,
    get_reference_dataset_path,
    get_split_settings,
    load_pipeline_settings,
)


DEFAULT_REFERENCE_DATASET_DIR = get_reference_dataset_path(load_pipeline_settings(DEFAULT_CONFIG_PATH))
DEFAULT_SPLITS_DIRNAME = "splits"
SPLIT_MANIFEST_FILE = "split_manifest.json"
PID_SPLITS = ("train", "val", "test")


JsonDict = dict[str, Any]


def load_dataset_index(reference_dataset_dir: str | Path = DEFAULT_REFERENCE_DATASET_DIR) -> list[JsonDict]:
    index_path = Path(reference_dataset_dir) / "dataset_index.json"
    if not index_path.exists():
        raise FileNotFoundError(f"Dataset index not found: {index_path}")

    with index_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    runs = payload.get("runs")
    if not isinstance(runs, list):
        raise ValueError("dataset_index.json is missing its 'runs' list.")
    return runs


def sort_run_entries(run_entries: list[JsonDict]) -> list[JsonDict]:
    return sorted(
        run_entries,
        key=lambda entry: (
            str(entry.get("tool", "")),
            str(entry.get("kind", "")),
            str(entry.get("scenario", "")),
            str(entry.get("run_id", "")),
        ),
    )


def sorted_unique_values(run_entries: list[JsonDict], key: str) -> list[str]:
    return sorted({str(entry.get(key, "")) for entry in run_entries if str(entry.get(key, "")).strip()})


def compute_split_counts(
    total: int,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> tuple[int, int, int]:
    if total <= 0:
        return 0, 0, 0
    if total == 1:
        return 1, 0, 0
    if total == 2:
        return 1, 0, 1

    train_count = max(1, int(total * train_ratio))
    val_count = max(1, int(total * val_ratio))
    test_count = total - train_count - val_count

    while test_count <= 0:
        if train_count >= val_count and train_count > 1:
            train_count -= 1
        elif val_count > 1:
            val_count -= 1
        else:
            break
        test_count = total - train_count - val_count

    return train_count, val_count, test_count


def split_benign_scenarios(
    benign_run_entries: list[JsonDict],
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> dict[str, list[str]]:
    benign_scenarios = sorted_unique_values(benign_run_entries, "scenario")
    train_count, val_count, _ = compute_split_counts(
        len(benign_scenarios),
        train_ratio=train_ratio,
        val_ratio=val_ratio,
    )
    train_end = train_count
    val_end = train_count + val_count
    return {
        "train": benign_scenarios[:train_end],
        "val": benign_scenarios[train_end:val_end],
        "test": benign_scenarios[val_end:],
    }


def split_attack_scenarios(
    attack_run_entries: list[JsonDict],
    validation_has_attacks: bool,
    val_ratio: float,
    test_ratio: float,
) -> dict[str, list[str]]:
    attack_scenarios = sorted_unique_values(attack_run_entries, "scenario")
    split_to_scenarios: dict[str, list[str]] = {"val": [], "test": []}

    if not validation_has_attacks:
        split_to_scenarios["test"] = attack_scenarios
        return split_to_scenarios

    if len(attack_scenarios) <= 1:
        split_to_scenarios["test"] = attack_scenarios
        return split_to_scenarios

    val_share = 0.5 if (val_ratio + test_ratio) == 0 else val_ratio / (val_ratio + test_ratio)
    val_count = max(1, int(len(attack_scenarios) * val_share))
    if val_count >= len(attack_scenarios):
        val_count = len(attack_scenarios) - 1

    split_to_scenarios["val"] = attack_scenarios[:val_count]
    split_to_scenarios["test"] = attack_scenarios[val_count:]
    return split_to_scenarios


def group_run_entries_by_tool(run_entries: list[JsonDict]) -> dict[str, list[JsonDict]]:
    buckets: dict[str, list[JsonDict]] = defaultdict(list)
    for entry in sort_run_entries(run_entries):
        buckets[str(entry.get("tool", ""))].append(entry)
    return dict(buckets)


def select_runs_for_tool(
    tool_run_entries: list[JsonDict],
    benign_scenarios_by_split: dict[str, list[str]],
    attack_scenarios_by_split: dict[str, list[str]],
) -> dict[str, list[JsonDict]]:
    benign_scenario_sets = {
        split_name: set(benign_scenarios_by_split.get(split_name, [])) for split_name in PID_SPLITS
    }
    attack_scenario_sets = {
        split_name: set(attack_scenarios_by_split.get(split_name, [])) for split_name in PID_SPLITS
    }

    split_to_runs: dict[str, list[JsonDict]] = {split_name: [] for split_name in PID_SPLITS}
    for entry in sort_run_entries(tool_run_entries):
        kind = str(entry.get("kind", ""))
        scenario = str(entry.get("scenario", ""))

        if kind == "benign":
            for split_name in PID_SPLITS:
                if scenario in benign_scenario_sets[split_name]:
                    split_to_runs[split_name].append(entry)
                    break
            continue

        for split_name in PID_SPLITS:
            if scenario in attack_scenario_sets[split_name]:
                split_to_runs[split_name].append(entry)
                break

    return {split_name: sort_run_entries(entries) for split_name, entries in split_to_runs.items()}


def summarize_split(run_entries: list[JsonDict]) -> JsonDict:
    runs_per_tool: dict[str, int] = {}
    runs_per_kind: dict[str, int] = {}

    for entry in run_entries:
        tool = str(entry.get("tool", ""))
        kind = str(entry.get("kind", ""))
        runs_per_tool[tool] = runs_per_tool.get(tool, 0) + 1
        runs_per_kind[kind] = runs_per_kind.get(kind, 0) + 1

    scenario_ids = sorted({str(entry.get("scenario", "")) for entry in run_entries})
    return {
        "run_count": len(run_entries),
        "scenario_count": len(scenario_ids),
        "runs_per_tool": runs_per_tool,
        "runs_per_kind": runs_per_kind,
        "scenario_ids": scenario_ids,
        "run_ids": [str(entry.get("run_id", "")) for entry in run_entries],
        "runs": run_entries,
    }


def write_split_file(
    split_name: str,
    run_entries: list[JsonDict],
    output_dir: str | Path,
    tool_name: str,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    split_path = output_path / f"{split_name}.json"
    with split_path.open("w", encoding="utf-8") as handle:
        payload = {"split": split_name, "tool": tool_name, **summarize_split(run_entries)}
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    return split_path


def write_split_manifest(
    splits_dir: str | Path,
    tools: list[str],
    benign_scenarios_by_split: dict[str, list[str]],
    attack_scenarios_by_split: dict[str, list[str]],
) -> Path:
    manifest_path = Path(splits_dir) / SPLIT_MANIFEST_FILE
    payload = {
        "mode": "per_tool",
        "tools": tools,
        "benign_scenarios": benign_scenarios_by_split,
        "attack_scenarios": attack_scenarios_by_split,
    }
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return manifest_path


def build_splits(
    reference_dataset_dir: str | Path = DEFAULT_REFERENCE_DATASET_DIR,
    split_settings: JsonDict | None = None,
) -> dict[str, dict[str, Path]]:
    run_entries = load_dataset_index(reference_dataset_dir)
    effective_split_settings = split_settings or get_split_settings(load_pipeline_settings(DEFAULT_CONFIG_PATH))

    validation_has_attacks = bool(effective_split_settings.get("validation_has_attacks", False))
    benign_ratios = effective_split_settings.get("benign_ratios", {})
    train_ratio = float(benign_ratios.get("train", 0.70))
    val_ratio = float(benign_ratios.get("val", 0.15))
    test_ratio = float(benign_ratios.get("test", 0.15))

    log_message("[build_splits] Building scenario-based per-tool splits")
    log_message(f"[build_splits] reference_dataset_dir={reference_dataset_dir}")
    log_message(f"[build_splits] validation_has_attacks={validation_has_attacks}")
    log_message(
        f"[build_splits] benign_ratios=train:{train_ratio:.2f} val:{val_ratio:.2f} test:{test_ratio:.2f}"
    )

    benign_runs = [entry for entry in run_entries if entry.get("kind") == "benign"]
    attack_runs = [entry for entry in run_entries if entry.get("kind") != "benign"]
    tool_to_runs = group_run_entries_by_tool(run_entries)
    tool_names = sorted(tool_to_runs)

    benign_scenarios_by_split = split_benign_scenarios(
        benign_runs,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
    )
    attack_scenarios_by_split = split_attack_scenarios(
        attack_runs,
        validation_has_attacks=validation_has_attacks,
        val_ratio=val_ratio,
        test_ratio=test_ratio,
    )

    for split_name in PID_SPLITS:
        log_message(
            f"[build_splits] benign scenarios {split_name}: {len(benign_scenarios_by_split.get(split_name, []))}"
        )
    for split_name in ("val", "test"):
        log_message(
            f"[build_splits] attack scenarios {split_name}: {len(attack_scenarios_by_split.get(split_name, []))}"
        )

    splits_dir = Path(reference_dataset_dir) / DEFAULT_SPLITS_DIRNAME
    for split_name in PID_SPLITS:
        legacy_split_path = splits_dir / f"{split_name}.json"
        if legacy_split_path.exists():
            legacy_split_path.unlink()

    split_paths: dict[str, dict[str, Path]] = {}
    for tool_name in tool_names:
        tool_split_dir = splits_dir / tool_name
        split_to_runs = select_runs_for_tool(
            tool_to_runs[tool_name],
            benign_scenarios_by_split,
            attack_scenarios_by_split,
        )
        split_paths[tool_name] = {
            split_name: write_split_file(split_name, split_to_runs[split_name], tool_split_dir, tool_name)
            for split_name in PID_SPLITS
        }
        for split_name in PID_SPLITS:
            log_message(
                f"[build_splits] tool={tool_name} split={split_name} runs={len(split_to_runs[split_name])}"
            )

    manifest_path = write_split_manifest(
        splits_dir,
        tool_names,
        benign_scenarios_by_split,
        attack_scenarios_by_split,
    )
    log_message(f"[build_splits] Wrote split manifest to {manifest_path}")
    return split_paths


def build_splits_from_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, dict[str, Path]]:
    settings = load_pipeline_settings(config_path)
    reference_dataset_dir = get_reference_dataset_path(settings)
    return build_splits(reference_dataset_dir, split_settings=get_split_settings(settings))


def main() -> None:
    build_splits_from_config(DEFAULT_CONFIG_PATH)


if __name__ == "__main__":
    main()
