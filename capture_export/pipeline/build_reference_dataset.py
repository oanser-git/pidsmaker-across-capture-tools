"""Build the reference dataset run by run from the corrected Orange raw files."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from pathlib import Path
from typing import Any

from capture_export.pipeline.assign_ids import assign_stable_ids
from capture_export.pipeline.logging_utils import log_message
from capture_export.pipeline.models import EdgeRecord, JsonDict, NodeRecord, ReferenceRun
from capture_export.pipeline.normalize_edges import normalize_edges
from capture_export.pipeline.normalize_features import normalize_run_features
from capture_export.pipeline.parse_raw_run import parse_raw_run
from capture_export.pipeline.repair_graph import repair_graph
from capture_export.pipeline.settings import (
    DEFAULT_CONFIG_PATH,
    get_data_raw_path,
    get_processing_workers,
    get_reference_dataset_path,
    load_pipeline_settings,
)


DEFAULT_DATA_RAW = get_data_raw_path(load_pipeline_settings(DEFAULT_CONFIG_PATH))
DEFAULT_OUTPUT_DIR = get_reference_dataset_path(load_pipeline_settings(DEFAULT_CONFIG_PATH))
REFERENCE_SCHEMA_VERSION = 3


def discover_raw_runs(data_raw: str | Path = DEFAULT_DATA_RAW) -> list[Path]:
    return sorted(Path(data_raw).glob("*.jsonl"))


def serialize_node(node: NodeRecord) -> JsonDict:
    return {
        "raw_node_id": node.raw_node_id,
        "prov_type": node.prov_type,
        "line_number": node.line_number,
        "global_node_id": node.global_node_id,
        "is_placeholder": node.is_placeholder,
        "attributes": node.attributes,
        "features": node.features,
        "raw": node.raw,
    }


def serialize_edge(edge: EdgeRecord) -> JsonDict:
    return {
        "raw_edge_id": edge.raw_edge_id,
        "src_raw_node_id": edge.src_raw_node_id,
        "dst_raw_node_id": edge.dst_raw_node_id,
        "prov_type": edge.prov_type,
        "line_number": edge.line_number,
        "timestamp": edge.timestamp,
        "global_edge_id": edge.global_edge_id,
        "src_global_node_id": edge.src_global_node_id,
        "dst_global_node_id": edge.dst_global_node_id,
        "edge_label": edge.edge_label,
        "attributes": edge.attributes,
        "features": edge.features,
        "raw": edge.raw,
    }


def summarize_reference_run(reference_run: ReferenceRun) -> JsonDict:
    placeholder_count = sum(node.is_placeholder for node in reference_run.nodes)
    duplicate_logical_node_count = sum(
        1
        for node in reference_run.nodes
        if node.raw.get("_repair", {}).get("duplicate_count", 1) > 1
    )

    return {
        "node_count": reference_run.node_count,
        "edge_count": reference_run.edge_count,
        "record_count": reference_run.record_count,
        "placeholder_node_count": placeholder_count,
        "duplicate_logical_node_count": duplicate_logical_node_count,
    }


def serialize_reference_run(reference_run: ReferenceRun) -> JsonDict:
    return {
        "metadata": {
            "schema_version": REFERENCE_SCHEMA_VERSION,
            "run_id": reference_run.metadata.run_id,
            "tool": reference_run.metadata.tool,
            "scenario": reference_run.metadata.scenario,
            "kind": reference_run.metadata.kind,
            "source_file": reference_run.metadata.source_file,
            "file_name": reference_run.metadata.file_name,
            "attack_windows": reference_run.metadata.attack_windows,
        },
        "stats": summarize_reference_run(reference_run),
        "nodes": [serialize_node(node) for node in reference_run.nodes],
        "edges": [serialize_edge(edge) for edge in reference_run.edges],
    }


def dataset_index_entry(reference_run: ReferenceRun, run_path: Path) -> JsonDict:
    return {
        "run_id": reference_run.metadata.run_id,
        "schema_version": REFERENCE_SCHEMA_VERSION,
        "tool": reference_run.metadata.tool,
        "scenario": reference_run.metadata.scenario,
        "kind": reference_run.metadata.kind,
        "source_file": reference_run.metadata.source_file,
        "file_name": reference_run.metadata.file_name,
        "run_file": str(run_path),
        "attack_window_count": len(reference_run.metadata.attack_windows),
        "attack_windows": reference_run.metadata.attack_windows,
        **summarize_reference_run(reference_run),
    }


def dataset_index_entry_from_saved_run(run_path: str | Path) -> JsonDict:
    with Path(run_path).open("r", encoding="utf-8") as handle:
        saved_run = json.load(handle)
    metadata = dict(saved_run.get("metadata") or {})
    stats = dict(saved_run.get("stats") or {})

    return {
        "run_id": metadata.get("run_id"),
        "schema_version": metadata.get("schema_version"),
        "tool": metadata.get("tool"),
        "scenario": metadata.get("scenario"),
        "kind": metadata.get("kind"),
        "source_file": metadata.get("source_file"),
        "file_name": metadata.get("file_name"),
        "run_file": str(Path(run_path).resolve()),
        "attack_window_count": len(metadata.get("attack_windows") or []),
        "attack_windows": metadata.get("attack_windows") or [],
        **stats,
    }


def write_dataset_index(index_entries: list[JsonDict], output_dir: str | Path) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    index_path = output_path / "dataset_index.json"

    by_tool: dict[str, int] = {}
    for entry in index_entries:
        tool = entry["tool"]
        by_tool[tool] = by_tool.get(tool, 0) + 1

    payload: dict[str, Any] = {
        "run_count": len(index_entries),
        "runs_per_tool": by_tool,
        "runs": index_entries,
    }

    with index_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    return index_path


def build_reference_run(jsonl_path: str | Path) -> ReferenceRun:
    reference_run = parse_raw_run(jsonl_path)
    reference_run.nodes, reference_run.edges = repair_graph(
        reference_run.nodes,
        reference_run.edges,
        reference_run.metadata,
    )
    normalize_edges(reference_run.edges)
    normalize_run_features(reference_run)
    return assign_stable_ids(reference_run)


def get_reference_run_output_path(output_dir: str | Path, run_id: str) -> Path:
    return Path(output_dir) / "runs" / f"{run_id}.json"


def save_reference_run(reference_run: ReferenceRun, output_dir: str | Path) -> Path:
    runs_dir = Path(output_dir) / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    run_path = get_reference_run_output_path(output_dir, reference_run.metadata.run_id)
    temp_path = run_path.with_suffix(".json.tmp")

    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(serialize_reference_run(reference_run), handle, separators=(",", ":"))
        handle.write("\n")

    temp_path.replace(run_path)

    return run_path


def build_and_save_reference_run(jsonl_path: str | Path, output_dir: str | Path) -> JsonDict:
    reference_run = build_reference_run(jsonl_path)
    run_path = save_reference_run(reference_run, output_dir)
    return dataset_index_entry(reference_run, run_path)


def process_reference_run(jsonl_path: str | Path, output_dir: str | Path) -> JsonDict:
    jsonl_path = Path(jsonl_path)
    run_path = get_reference_run_output_path(output_dir, jsonl_path.stem)

    if run_path.exists():
        try:
            index_entry = dataset_index_entry_from_saved_run(run_path)
            if index_entry.get("schema_version") != REFERENCE_SCHEMA_VERSION:
                index_entry = build_and_save_reference_run(jsonl_path, output_dir)
                return {
                    "status": "rebuilt",
                    "file_name": jsonl_path.name,
                    "run_path": str(Path(index_entry["run_file"]).resolve()),
                    "index_entry": index_entry,
                }
            saved_source = index_entry.get("source_file")
            if not saved_source or Path(str(saved_source)).resolve() != jsonl_path.resolve():
                index_entry = build_and_save_reference_run(jsonl_path, output_dir)
                return {
                    "status": "rebuilt",
                    "file_name": jsonl_path.name,
                    "run_path": str(Path(index_entry["run_file"]).resolve()),
                    "index_entry": index_entry,
                }
            return {
                "status": "skipped",
                "file_name": jsonl_path.name,
                "run_path": str(run_path.resolve()),
                "index_entry": index_entry,
            }
        except Exception:
            index_entry = build_and_save_reference_run(jsonl_path, output_dir)
            return {
                "status": "rebuilt",
                "file_name": jsonl_path.name,
                "run_path": str(Path(index_entry["run_file"]).resolve()),
                "index_entry": index_entry,
            }

    index_entry = build_and_save_reference_run(jsonl_path, output_dir)
    return {
        "status": "built",
        "file_name": jsonl_path.name,
        "run_path": str(Path(index_entry["run_file"]).resolve()),
        "index_entry": index_entry,
    }


def build_reference_dataset(
    data_raw: str | Path | None = None,
    output_dir: str | Path | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> list[Path]:
    settings = load_pipeline_settings(config_path)
    resolved_data_raw = Path(data_raw) if data_raw is not None else get_data_raw_path(settings)
    resolved_output_dir = (
        Path(output_dir) if output_dir is not None else get_reference_dataset_path(settings)
    )
    workers = get_processing_workers(settings)
    raw_runs = discover_raw_runs(resolved_data_raw)

    log_message("[build_reference_dataset] Starting reference dataset build")
    log_message(f"[build_reference_dataset] data_raw={resolved_data_raw}")
    log_message(f"[build_reference_dataset] output_dir={resolved_output_dir}")
    log_message(f"[build_reference_dataset] runs={len(raw_runs)}")
    log_message(f"[build_reference_dataset] workers={workers}")

    saved_runs: list[Path] = []
    index_entries: list[JsonDict] = []
    skipped_runs = 0
    rebuilt_runs = 0
    built_runs = 0

    if workers == 1:
        for index, jsonl_path in enumerate(raw_runs, start=1):
            result = process_reference_run(jsonl_path, resolved_output_dir)
            saved_runs.append(Path(result["run_path"]))
            index_entries.append(result["index_entry"])

            if result["status"] == "skipped":
                skipped_runs += 1
                verb = "skipping existing"
            elif result["status"] == "rebuilt":
                rebuilt_runs += 1
                verb = "rebuilt"
            else:
                built_runs += 1
                verb = "built"

            log_message(
                f"[build_reference_dataset] [{index}/{len(raw_runs)}] {verb} {result['file_name']}"
            )
    else:
        log_message(
            f"[build_reference_dataset] Processing {len(raw_runs)} run(s) with multiprocessing"
        )
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_run = {
                executor.submit(process_reference_run, str(jsonl_path), str(resolved_output_dir)): jsonl_path
                for jsonl_path in raw_runs
            }

            for index, future in enumerate(as_completed(future_to_run), start=1):
                result = future.result()
                saved_runs.append(Path(result["run_path"]))
                index_entries.append(result["index_entry"])

                if result["status"] == "skipped":
                    skipped_runs += 1
                    verb = "skipped"
                elif result["status"] == "rebuilt":
                    rebuilt_runs += 1
                    verb = "rebuilt"
                else:
                    built_runs += 1
                    verb = "built"

                log_message(
                    f"[build_reference_dataset] [{index}/{len(raw_runs)}] {verb} {result['file_name']}"
                )

    index_entries.sort(key=lambda entry: str(entry.get("run_id", "")))
    index_path = write_dataset_index(index_entries, resolved_output_dir)
    log_message(f"[build_reference_dataset] Wrote dataset index to {index_path}")
    log_message(f"[build_reference_dataset] Built {built_runs} new run file(s)")
    log_message(f"[build_reference_dataset] Rebuilt {rebuilt_runs} invalid run file(s)")
    log_message(f"[build_reference_dataset] Reused {skipped_runs} existing run file(s)")
    log_message("[build_reference_dataset] Reference dataset build completed")
    return saved_runs


def main() -> None:
    build_reference_dataset(config_path=DEFAULT_CONFIG_PATH)


if __name__ == "__main__":
    main()
