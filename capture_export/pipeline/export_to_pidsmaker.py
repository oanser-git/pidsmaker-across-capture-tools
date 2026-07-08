"""Export the reference dataset to the minimal artifact set PIDSMaker expects after graph construction."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import shutil
from pathlib import Path
from typing import Any

import networkx as nx
import torch

from capture_export.pipeline.logging_utils import log_message
from capture_export.pipeline.time_utils import (
    ns_to_window_component,
    ranges_overlap,
    timestamp_event_interval_ns,
    timestamp_model_time,
    timestamp_sort_key,
    timestamp_wall_time_ns,
)


DEFAULT_REFERENCE_DATASET_DIR = Path(__file__).resolve().parents[1] / "reference_dataset"
DEFAULT_EXPORT_DIR = Path(__file__).resolve().parents[1] / "pidsmaker_export"
RAW_EXPORT_DIR = "raw"
RAW_PID_TYPES = {
    "address",
    "argv",
    "block",
    "char",
    "device",
    "directory",
    "file",
    "iattr",
    "inode_unknown",
    "link",
    "machine",
    "named pipe",
    "netflow",
    "network",
    "packet",
    "path",
    "pipe",
    "process",
    "process_memory",
    "regular file",
    "socket",
    "super block",
    "symlink",
    "task",
    "unknown",
    "xattr",
}
PROCESS_NODE_TYPES = {"process", "task"}
NETWORK_NODE_TYPES = {"netflow", "address", "socket", "packet", "network"}
FILESYSTEM_NODE_TYPES = {
    "file",
    "regular file",
    "directory",
    "symlink",
    "link",
    "path",
    "block",
    "char",
    "device",
    "super block",
    "inode_unknown",
    "xattr",
    "iattr",
}
PID_SPLITS = ("train", "val", "test")
SPLIT_MANIFEST_FILE = "split_manifest.json"
DEFAULT_WINDOW_SIZE_SECONDS = 60


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_reference_run(reference_run_path: str | Path) -> dict[str, Any]:
    return load_json(reference_run_path)


def load_split_file(split_dir: str | Path, split_name: str) -> dict[str, Any]:
    split_path = Path(split_dir) / f"{split_name}.json"
    if not split_path.exists():
        raise FileNotFoundError(f"Missing split file: {split_path}")
    return load_json(split_path)


def load_split_manifest(reference_dataset_dir: str | Path) -> dict[str, Any]:
    manifest_path = Path(reference_dataset_dir) / "splits" / SPLIT_MANIFEST_FILE
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing split manifest: {manifest_path}")
    return load_json(manifest_path)


def normalize_type_token(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    token = " ".join(value.strip().lower().split())
    if not token or token in {"<missing>", "<unknown>"}:
        return None
    return token


def select_raw_node_type(node: dict[str, Any]) -> str:
    attributes = dict(node.get("attributes") or {})
    node_type = normalize_type_token(attributes.get("object_type"))
    if node_type is None:
        node_type = normalize_type_token(attributes.get("_raw_type_hint"))
    if node_type in RAW_PID_TYPES:
        return str(node_type)
    return "unknown"


def resolve_run_file(reference_dataset_dir: str | Path, run_entry: dict[str, Any]) -> dict[str, Any]:
    resolved_entry = dict(run_entry)
    run_file = Path(str(resolved_entry.get("run_file", "")))
    if not run_file.exists():
        relocated = Path(reference_dataset_dir) / "runs" / run_file.name
        if relocated.exists():
            run_file = relocated
    resolved_entry["run_file"] = str(run_file)
    return resolved_entry


def select_node_type(node: dict[str, Any]) -> str:
    return select_raw_node_type(node)


def normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def join_label_parts(parts: list[str | None]) -> str:
    clean_parts = [part for part in parts if part]
    return " ".join(clean_parts) if clean_parts else "missing_endpoint"


def build_node_label(node: dict[str, Any], node_type: str) -> str:
    features = dict(node.get("features") or {})
    raw_type_hint = normalize_text(node.get("attributes", {}).get("_raw_type_hint"))

    if node.get("is_placeholder"):
        return join_label_parts(["missing_endpoint", raw_type_hint])

    if node_type in PROCESS_NODE_TYPES:
        return join_label_parts(
            [
                node_type,
                normalize_text(features.get("process_name")),
                normalize_text(features.get("command_line")),
                normalize_text(features.get("working_directory")),
            ]
        )

    if node_type in FILESYSTEM_NODE_TYPES or node_type in {
        "machine",
        "argv",
        "process_memory",
        "pipe",
        "named pipe",
        "unknown",
    }:
        return join_label_parts(
            [
                node_type,
                raw_type_hint,
                normalize_text(features.get("path")),
                normalize_text(features.get("display_name")),
                normalize_text(features.get("command_line")),
            ]
        )

    if node_type in NETWORK_NODE_TYPES:
        return join_label_parts(
            [
                node_type,
                raw_type_hint,
                normalize_text(features.get("network_address")),
                normalize_text(features.get("port")),
                normalize_text(features.get("network_family")),
            ]
        )

    raise ValueError(f"Unsupported raw node type: {node_type}")


def build_node_path_entry(node: dict[str, Any], node_type: str) -> dict[str, Any]:
    features = dict(node.get("features") or {})
    display_name = normalize_text(features.get("display_name")) or "missing_endpoint"

    if node_type in PROCESS_NODE_TYPES:
        return {
            "path": display_name,
            "type": node_type,
            "cmd": normalize_text(features.get("command_line")),
        }

    return {"path": display_name, "type": node_type}


def graph_file_name(start_ns: int, end_ns: int) -> str:
    return f"{ns_to_window_component(start_ns)}~{ns_to_window_component(end_ns)}"


def edge_is_attack(timestamp: dict[str, Any], attack_windows: list[dict[str, Any]]) -> bool:
    edge_start_ns, edge_end_ns = timestamp_event_interval_ns(timestamp)
    return any(
        ranges_overlap(edge_start_ns, edge_end_ns, int(window["start_ns"]), int(window["end_ns"]))
        for window in attack_windows
    )


def export_run_order(
    reference_dataset_dir: str | Path,
    tools: list[str] | None = None,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    split_manifest = load_split_manifest(reference_dataset_dir)
    tool_names = [str(tool_name) for tool_name in split_manifest.get("tools", [])]
    if tools is not None:
        requested_tools = set(tools)
        missing_tools = requested_tools - set(tool_names)
        if missing_tools:
            raise ValueError(f"Unknown tool(s) in split manifest: {sorted(missing_tools)}")
        tool_names = [tool_name for tool_name in tool_names if tool_name in requested_tools]
    tool_to_split_to_run_entries: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for tool_name in tool_names:
        split_dir = Path(reference_dataset_dir) / "splits" / tool_name
        split_to_run_entries: dict[str, list[dict[str, Any]]] = {}
        for split_name in PID_SPLITS:
            split_payload = load_split_file(split_dir, split_name)
            split_to_run_entries[split_name] = [
                resolve_run_file(reference_dataset_dir, run_entry)
                for run_entry in split_payload.get("runs", [])
            ]
        tool_to_split_to_run_entries[tool_name] = split_to_run_entries
    return tool_to_split_to_run_entries


def extract_global_node_ids(reference_run_path: str | Path) -> list[str]:
    run = load_reference_run(reference_run_path)
    return [str(node["global_node_id"]) for node in run.get("nodes", [])]


def build_export_node_id_map(
    split_to_run_entries: dict[str, list[dict[str, Any]]],
    workers: int,
) -> dict[str, str]:
    run_paths = [
        str(run_entry["run_file"])
        for run_entries in split_to_run_entries.values()
        for run_entry in run_entries
    ]

    global_node_ids: set[str] = set()
    log_message(
        f"[export_to_pidsmaker] Building global node-id map from {len(run_paths)} reference run(s)"
    )

    if workers == 1:
        for index, run_path in enumerate(run_paths, start=1):
            global_node_ids.update(extract_global_node_ids(run_path))
            log_message(
                f"[export_to_pidsmaker] [{index}/{len(run_paths)}] collected node ids from {Path(run_path).name}"
            )
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_run_path = {
                executor.submit(extract_global_node_ids, run_path): run_path for run_path in run_paths
            }
            for index, future in enumerate(as_completed(future_to_run_path), start=1):
                global_node_ids.update(future.result())
                log_message(
                    f"[export_to_pidsmaker] [{index}/{len(run_paths)}] collected node ids from {Path(future_to_run_path[future]).name}"
                )

    sorted_global_node_ids = sorted(global_node_ids)
    log_message(
        f"[export_to_pidsmaker] Global node-id map completed with {len(sorted_global_node_ids)} unique node(s)"
    )
    return {
        global_node_id: str(index)
        for index, global_node_id in enumerate(sorted_global_node_ids, start=1)
    }


def initialize_export_directories(output_dir: str | Path) -> dict[str, Path]:
    base_dir = Path(output_dir) / RAW_EXPORT_DIR
    if base_dir.exists():
        shutil.rmtree(base_dir)
    construction_graphs_dir = base_dir / "construction" / "nx"
    transformation_graphs_dir = base_dir / "transformation" / "nx"
    dicts_dir = base_dir / "construction" / "indexid2msg"
    node_to_path_dir = base_dir / "construction" / "node_id_to_path"

    for split_name in PID_SPLITS:
        (construction_graphs_dir / f"graph_{split_name}").mkdir(parents=True, exist_ok=True)
        (transformation_graphs_dir / f"graph_{split_name}").mkdir(parents=True, exist_ok=True)

    dicts_dir.mkdir(parents=True, exist_ok=True)
    node_to_path_dir.mkdir(parents=True, exist_ok=True)

    return {
        "base": base_dir,
        "construction_graphs": construction_graphs_dir,
        "transformation_graphs": transformation_graphs_dir,
        "dicts": dicts_dir,
        "node_to_path": node_to_path_dir,
    }


def export_is_complete(
    output_dir: str | Path,
    split_to_run_entries: dict[str, list[dict[str, Any]]],
    window_size_seconds: int,
) -> bool:
    base_dir = Path(output_dir) / RAW_EXPORT_DIR
    export_index_path = base_dir / "export_index.json"
    if not export_index_path.exists():
        return False

    required_files = [
        base_dir / "construction" / "indexid2msg" / "indexid2msg.pkl",
        base_dir / "construction" / "indexid2msg" / "split2nodes.pkl",
        base_dir / "construction" / "node_id_to_path" / "node_to_paths.pkl",
    ]
    if any(not path.exists() for path in required_files):
        return False

    export_index = load_json(export_index_path)
    if int(export_index.get("window_size_seconds", 0)) != window_size_seconds:
        return False
    for split_name in PID_SPLITS:
        expected_run_ids = {str(entry.get("run_id", "")) for entry in split_to_run_entries.get(split_name, [])}
        exported_runs = list(export_index.get("splits", {}).get(split_name, []))
        exported_run_ids = {str(entry.get("run_id", "")) for entry in exported_runs}
        if expected_run_ids != exported_run_ids:
            return False

        for entry in exported_runs:
            windows = list(entry.get("windows", []))
            if not windows:
                return False
            for window in windows:
                file_name = str(window.get("file_name", ""))
                if not file_name:
                    return False
                graph_paths = [
                    base_dir / "construction" / "nx" / f"graph_{split_name}" / file_name,
                    base_dir / "transformation" / "nx" / f"graph_{split_name}" / file_name,
                ]
                if any(not path.exists() for path in graph_paths):
                    return False

    return True


def build_single_exported_run(
    reference_run_path: str | Path,
    split_name: str,
    export_node_id_map: dict[str, str],
    window_size_seconds: int,
) -> dict[str, Any]:
    run = load_reference_run(reference_run_path)
    indexid2msg: dict[str, list[str]] = {}
    node_to_path_type: dict[str, dict[str, Any]] = {}
    split_nodes: set[str] = set()
    edge_labels: set[str] = set()
    node_payloads = {str(node["global_node_id"]): node for node in run.get("nodes", [])}
    attack_windows = list(run.get("metadata", {}).get("attack_windows") or [])
    window_size_ns = window_size_seconds * 1_000_000_000

    def add_node_to_graph(graph: Any, global_node_id: str) -> str:
        node = node_payloads[global_node_id]
        node_type = select_node_type(node)
        export_node_id = export_node_id_map[global_node_id]
        node_label = build_node_label(node, node_type)

        if export_node_id not in graph:
            graph.add_node(export_node_id, node_type=node_type, label=node_label)
        indexid2msg[export_node_id] = [node_type, node_label]
        node_to_path_type[export_node_id] = build_node_path_entry(node, node_type)
        split_nodes.add(export_node_id)
        return export_node_id

    sorted_edges = sorted(
        run.get("edges", []),
        key=lambda item: timestamp_sort_key(item["timestamp"], item["global_edge_id"]),
    )
    if not sorted_edges:
        raise ValueError(f"Run {run['metadata']['run_id']} has no edges to export.")

    windows: list[dict[str, Any]] = []
    current_start_ns: int | None = None
    current_graph: Any = None
    current_edge_count = 0
    current_attack_edge_count = 0
    current_end_ns: int | None = None

    def flush_window() -> None:
        nonlocal current_start_ns, current_graph, current_edge_count, current_attack_edge_count, current_end_ns
        if current_graph is None or current_start_ns is None or current_end_ns is None:
            return
        if current_edge_count == 0:
            return
        windows.append(
            {
                "start_ns": current_start_ns,
                "end_ns": current_end_ns,
                "graph": current_graph,
                "node_count": current_graph.number_of_nodes(),
                "edge_count": current_graph.number_of_edges(),
                "attack_edge_count": current_attack_edge_count,
                "y": int(current_attack_edge_count > 0),
            }
        )
        current_start_ns = None
        current_graph = None
        current_edge_count = 0
        current_attack_edge_count = 0
        current_end_ns = None

    for edge in sorted_edges:
        edge_timestamp = edge["timestamp"]
        edge_wall_time = timestamp_wall_time_ns(edge_timestamp)
        edge_model_time = timestamp_model_time(edge_timestamp)
        if current_graph is None:
            current_start_ns = edge_wall_time
            current_graph = nx.MultiDiGraph()
        elif current_start_ns is not None and edge_wall_time >= current_start_ns + window_size_ns:
            flush_window()
            current_start_ns = edge_wall_time
            current_graph = nx.MultiDiGraph()

        src_id = add_node_to_graph(current_graph, str(edge["src_global_node_id"]))
        dst_id = add_node_to_graph(current_graph, str(edge["dst_global_node_id"]))
        edge_y = int(edge_is_attack(edge_timestamp, attack_windows))
        current_graph.add_edge(
            src_id,
            dst_id,
            event_uuid=str(edge["global_edge_id"]),
            time=edge_model_time,
            time_unit=str(edge_timestamp.get("model_time_unit", "unknown")),
            wall_time=edge_wall_time,
            wall_time_precision_ns=int(edge_timestamp.get("wall_time_precision_ns", 1)),
            label=str(edge["edge_label"]),
            y=edge_y,
        )
        current_edge_count += 1
        current_attack_edge_count += edge_y
        current_end_ns = edge_wall_time
        edge_labels.add(str(edge["edge_label"]))

    flush_window()

    return {
        "split_name": split_name,
        "run_id": run["metadata"]["run_id"],
        "window_size_seconds": window_size_seconds,
        "attack_windows": attack_windows,
        "windows": windows,
        "split_nodes": split_nodes,
        "indexid2msg": indexid2msg,
        "node_to_path_type": node_to_path_type,
        "edge_labels": sorted(edge_labels),
    }


def export_single_run_to_pidsmaker(
    reference_run_path: str | Path,
    split_name: str,
    export_node_id_map: dict[str, str],
    window_size_seconds: int,
    base_dir: str | Path,
) -> dict[str, Any]:
    base_path = Path(base_dir)
    payload = build_single_exported_run(
        reference_run_path,
        split_name,
        export_node_id_map,
        window_size_seconds,
    )

    exported_windows: list[dict[str, Any]] = []
    used_file_names: set[str] = set()
    for index, window in enumerate(payload["windows"], start=1):
        file_name = graph_file_name(window["start_ns"], window["end_ns"])
        if file_name in used_file_names:
            raise ValueError(f"Duplicate window filename inside run {payload['run_id']}: {file_name}")
        used_file_names.add(file_name)

        construction_path = base_path / "construction" / "nx" / f"graph_{split_name}" / file_name
        transformation_path = base_path / "transformation" / "nx" / f"graph_{split_name}" / file_name
        torch.save(window["graph"], construction_path)
        shutil.copy2(construction_path, transformation_path)
        exported_windows.append(
            {
                "window_index": index,
                "file_name": file_name,
                "node_count": window["node_count"],
                "edge_count": window["edge_count"],
                "attack_edge_count": window["attack_edge_count"],
                "y": window["y"],
                "start_ns": window["start_ns"],
                "end_ns": window["end_ns"],
            }
        )

    return {
        "split_name": split_name,
        "run_id": payload["run_id"],
        "window_size_seconds": payload["window_size_seconds"],
        "attack_windows": payload["attack_windows"],
        "windows": exported_windows,
        "window_count": len(exported_windows),
        "node_count": sum(window["node_count"] for window in exported_windows),
        "edge_count": sum(window["edge_count"] for window in exported_windows),
        "attack_window_count": sum(window["y"] for window in exported_windows),
        "attack_edge_count": sum(window["attack_edge_count"] for window in exported_windows),
        "split_nodes": sorted(payload["split_nodes"]),
        "indexid2msg": payload["indexid2msg"],
        "node_to_path_type": payload["node_to_path_type"],
        "edge_labels": payload["edge_labels"],
    }


def save_graphs_and_metadata(
    split_to_run_entries: dict[str, list[dict[str, Any]]],
    output_dir: str | Path,
    workers: int = 1,
    window_size_seconds: int = DEFAULT_WINDOW_SIZE_SECONDS,
) -> Path:
    dirs = initialize_export_directories(output_dir)
    export_node_id_map = build_export_node_id_map(split_to_run_entries, workers)

    log_message("[export_to_pidsmaker] Starting PIDSMaker export")
    log_message(f"[export_to_pidsmaker] output_dir={dirs['base']}")
    log_message(f"[export_to_pidsmaker] window_size_seconds={window_size_seconds}")

    split2nodes: dict[str, set[str]] = {split: set() for split in PID_SPLITS}
    indexid2msg: dict[str, list[str]] = {}
    node_to_path_type: dict[str, dict[str, Any]] = {}
    export_index: dict[str, Any] = {"representation": RAW_EXPORT_DIR, "splits": {}}

    run_jobs = [
        (split_name, run_entry)
        for split_name in PID_SPLITS
        for run_entry in split_to_run_entries.get(split_name, [])
    ]

    log_message(f"[export_to_pidsmaker] workers={workers}")

    run_results: list[dict[str, Any]] = []
    if workers == 1:
        for split_name, run_entry in run_jobs:
            log_message(f"[export_to_pidsmaker] exporting run {run_entry['run_id']}")
            run_results.append(
                export_single_run_to_pidsmaker(
                    run_entry["run_file"],
                    split_name,
                    export_node_id_map,
                    window_size_seconds,
                    dirs["base"],
                )
            )
            log_message(
                f"[export_to_pidsmaker] wrote {run_results[-1]['window_count']} window(s) for {run_entry['run_id']}"
            )
    else:
        log_message(f"[export_to_pidsmaker] Exporting {len(run_jobs)} run(s) with multiprocessing")
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_job = {
                executor.submit(
                    export_single_run_to_pidsmaker,
                    run_entry["run_file"],
                    split_name,
                    export_node_id_map,
                    window_size_seconds,
                    str(dirs["base"]),
                ): (split_name, run_entry["run_id"])
                for split_name, run_entry in run_jobs
            }

            for index, future in enumerate(as_completed(future_to_job), start=1):
                split_name, run_id = future_to_job[future]
                result = future.result()
                run_results.append(result)
                log_message(
                    f"[export_to_pidsmaker] [{index}/{len(run_jobs)}] wrote {result['window_count']} window(s) ({split_name} / {run_id})"
                )

    for split_name in PID_SPLITS:
        export_index["splits"][split_name] = []
        log_message(
            f"[export_to_pidsmaker] split={split_name} runs={len(split_to_run_entries.get(split_name, []))}"
        )

    relation_vocabulary: set[str] = set()
    split_to_file_names: dict[str, set[str]] = {split: set() for split in PID_SPLITS}
    for result in sorted(run_results, key=lambda item: (item["split_name"], item["run_id"])):
        split_name = str(result["split_name"])
        for window in result["windows"]:
            file_name = str(window["file_name"])
            if file_name in split_to_file_names[split_name]:
                raise ValueError(f"Duplicate exported graph filename in split {split_name}: {file_name}")
            split_to_file_names[split_name].add(file_name)
        export_index["splits"][split_name].append(
            {
                "run_id": result["run_id"],
                "window_count": result["window_count"],
                "attack_window_count": result["attack_window_count"],
                "node_count": result["node_count"],
                "edge_count": result["edge_count"],
                "attack_edge_count": result["attack_edge_count"],
                "attack_windows": result["attack_windows"],
                "windows": result["windows"],
            }
        )
        split2nodes[split_name].update(result["split_nodes"])
        indexid2msg.update(result["indexid2msg"])
        node_to_path_type.update(result["node_to_path_type"])
        relation_vocabulary.update(result["edge_labels"])

    torch.save(indexid2msg, dirs["dicts"] / "indexid2msg.pkl")
    torch.save(split2nodes, dirs["dicts"] / "split2nodes.pkl")
    torch.save(node_to_path_type, dirs["node_to_path"] / "node_to_paths.pkl")

    export_index.update(
        {
            "node_count": len(export_node_id_map),
            "node_type_count": len({str(msg[0]) for msg in indexid2msg.values()}),
            "node_types": sorted({str(msg[0]) for msg in indexid2msg.values()}),
            "edge_label_count": len(relation_vocabulary),
            "edge_labels": sorted(relation_vocabulary),
            "window_size_seconds": window_size_seconds,
            "window_count": sum(len(file_names) for file_names in split_to_file_names.values()),
            "date_aliases": {split: [split] for split in PID_SPLITS},
        }
    )

    export_index_path = dirs["base"] / "export_index.json"
    with export_index_path.open("w", encoding="utf-8") as handle:
        json.dump(export_index, handle, indent=2)
        handle.write("\n")

    log_message(f"[export_to_pidsmaker] Wrote export index to {export_index_path}")
    log_message("[export_to_pidsmaker] PIDSMaker export completed")
    return dirs["base"]


def write_export_manifest(
    output_dir: str | Path,
    window_size_seconds: int,
    tool_to_export_dir: dict[str, Path],
) -> Path:
    manifest_path = Path(output_dir) / "export_manifest.json"
    payload = {
        "mode": "per_tool",
        "representation": RAW_EXPORT_DIR,
        "window_size_seconds": window_size_seconds,
        "tool_exports": {
            tool_name: str(export_dir) for tool_name, export_dir in sorted(tool_to_export_dir.items())
        },
    }
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return manifest_path


def export_dataset_to_pidsmaker(
    reference_dataset_dir: str | Path = DEFAULT_REFERENCE_DATASET_DIR,
    output_dir: str | Path = DEFAULT_EXPORT_DIR,
    workers: int = 1,
    window_size_seconds: int = DEFAULT_WINDOW_SIZE_SECONDS,
    tools: list[str] | None = None,
) -> Path:
    log_message("[export_to_pidsmaker] Preparing PIDSMaker export inputs")
    tool_to_split_to_run_entries = export_run_order(reference_dataset_dir, tools=tools)
    tool_to_export_dir: dict[str, Path] = {}

    for tool_name, split_to_run_entries in sorted(tool_to_split_to_run_entries.items()):
        log_message(f"[export_to_pidsmaker] tool={tool_name}")
        for split_name in PID_SPLITS:
            log_message(
                f"[export_to_pidsmaker] split input {tool_name}/{split_name}: {len(split_to_run_entries.get(split_name, []))} run(s)"
            )

        tool_output_dir = Path(output_dir) / tool_name
        if export_is_complete(tool_output_dir, split_to_run_entries, window_size_seconds):
            tool_to_export_dir[tool_name] = tool_output_dir / RAW_EXPORT_DIR
            log_message(f"[export_to_pidsmaker] Skipping complete export for tool={tool_name}")
        else:
            tool_to_export_dir[tool_name] = save_graphs_and_metadata(
                split_to_run_entries,
                tool_output_dir,
                workers=workers,
                window_size_seconds=window_size_seconds,
            )

    manifest_path = write_export_manifest(output_dir, window_size_seconds, tool_to_export_dir)
    log_message(f"[export_to_pidsmaker] Wrote export manifest to {manifest_path}")

    return Path(output_dir)
