#!/usr/bin/env python3
"""Derive larger PIDSMaker Orange graph windows from an existing graph export.

The source export is treated as a lossless event cache: graph edges are loaded
from the existing windows, sorted by their original absolute wall time, and then
written into a new export root with a larger window size. The original export is
never modified.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import tempfile
from typing import Any

import networkx as nx
import torch


NS_PER_SECOND = 1_000_000_000
SPLITS = ("train", "val", "test")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def ns_to_window_component(ns_value: int) -> str:
    seconds = ns_value // NS_PER_SECOND
    nanos = ns_value % NS_PER_SECOND
    dt = datetime.fromtimestamp(seconds, timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{nanos:09d}"


def graph_file_name(start_ns: int, end_ns: int) -> str:
    return f"{ns_to_window_component(start_ns)}~{ns_to_window_component(end_ns)}"


def resolve_tool_export_dir(source_root: Path, tool: str, manifest: dict[str, Any]) -> Path:
    tool_exports = manifest.get("tool_exports") or {}
    if isinstance(tool_exports, dict) and tool in tool_exports:
        candidate = Path(str(tool_exports[tool]))
        if candidate.exists():
            return candidate.resolve()
    return (source_root / tool / "raw").resolve()


def graph_path(tool_dir: Path, split: str, file_name: str, stage: str) -> Path:
    return tool_dir / stage / "nx" / f"graph_{split}" / file_name


def load_graph(path: Path) -> Any:
    try:
        return torch.load(str(path), map_location="cpu", weights_only=False)
    except TypeError:
        return torch.load(str(path), map_location="cpu")


def sorted_windows(run_entry: dict[str, Any]) -> list[dict[str, Any]]:
    windows = list(run_entry.get("windows") or [])
    return sorted(windows, key=lambda item: (int(item.get("window_index", 0)), int(item.get("start_ns", 0))))


def comparable_node_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    return {"label": attrs.get("label"), "node_type": attrs.get("node_type")}


def copy_static_construction_metadata(source_tool_dir: Path, dest_tool_dir: Path) -> None:
    source_construction = source_tool_dir / "construction"
    dest_construction = dest_tool_dir / "construction"
    if not source_construction.exists():
        raise FileNotFoundError(f"Missing construction directory: {source_construction}")

    for child in source_construction.iterdir():
        if child.name == "nx":
            continue
        destination = dest_construction / child.name
        if child.is_dir():
            shutil.copytree(child, destination, symlinks=True)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(child, destination)

    for split in SPLITS:
        (dest_tool_dir / "construction" / "nx" / f"graph_{split}").mkdir(parents=True, exist_ok=True)
        (dest_tool_dir / "transformation" / "nx" / f"graph_{split}").mkdir(parents=True, exist_ok=True)


def load_run_edge_stream(
    source_tool_dir: Path,
    split: str,
    run_entry: dict[str, Any],
) -> tuple[dict[Any, dict[str, Any]], list[tuple[tuple[Any, ...], Any, Any, dict[str, Any]]], dict[str, int]]:
    node_attrs_by_id: dict[Any, dict[str, Any]] = {}
    edges: list[tuple[tuple[Any, ...], Any, Any, dict[str, Any]]] = []
    source_edge_count = 0
    source_attack_edge_count = 0

    for window in sorted_windows(run_entry):
        file_name = str(window.get("file_name") or "")
        if not file_name:
            raise ValueError(f"Window without file_name in run {run_entry.get('run_id')}")

        construction_path = graph_path(source_tool_dir, split, file_name, "construction")
        transformation_path = graph_path(source_tool_dir, split, file_name, "transformation")
        if not construction_path.exists():
            raise FileNotFoundError(f"Missing source construction graph: {construction_path}")
        if not transformation_path.exists():
            raise FileNotFoundError(f"Missing source transformation graph: {transformation_path}")

        graph = load_graph(transformation_path)
        expected_nodes = int(window.get("node_count", -1))
        expected_edges = int(window.get("edge_count", -1))
        if expected_nodes >= 0 and graph.number_of_nodes() != expected_nodes:
            raise ValueError(
                f"Node count mismatch for {transformation_path}: "
                f"graph={graph.number_of_nodes()} index={expected_nodes}"
            )
        if expected_edges >= 0 and graph.number_of_edges() != expected_edges:
            raise ValueError(
                f"Edge count mismatch for {transformation_path}: "
                f"graph={graph.number_of_edges()} index={expected_edges}"
            )

        for node_id, attrs in graph.nodes(data=True):
            attrs_copy = dict(attrs)
            previous = node_attrs_by_id.get(node_id)
            if previous is not None and comparable_node_attrs(previous) != comparable_node_attrs(attrs_copy):
                raise ValueError(
                    f"Node attribute conflict in run {run_entry.get('run_id')} node={node_id!r}: "
                    f"{comparable_node_attrs(previous)} != {comparable_node_attrs(attrs_copy)}"
                )
            node_attrs_by_id[node_id] = attrs_copy

        window_attack_edges = 0
        for src, dst, edge_key, attrs in graph.edges(data=True, keys=True):
            attrs_copy = dict(attrs)
            if "wall_time" not in attrs_copy:
                raise ValueError(f"Missing edge wall_time in {transformation_path}: {src!r}->{dst!r}")
            wall_time = int(attrs_copy["wall_time"])
            model_time = int(attrs_copy.get("time", wall_time))
            event_uuid = str(attrs_copy.get("event_uuid", ""))
            sort_key = (wall_time, model_time, event_uuid, str(src), str(dst), str(edge_key))
            edges.append((sort_key, src, dst, attrs_copy))
            source_edge_count += 1
            window_attack_edges += int(attrs_copy.get("y", 0))
        expected_attack_edges = int(window.get("attack_edge_count", -1))
        if expected_attack_edges >= 0 and window_attack_edges != expected_attack_edges:
            raise ValueError(
                f"Attack edge count mismatch for {transformation_path}: "
                f"graph={window_attack_edges} index={expected_attack_edges}"
            )
        source_attack_edge_count += window_attack_edges

    edges.sort(key=lambda item: item[0])
    return node_attrs_by_id, edges, {
        "source_edge_count": source_edge_count,
        "source_attack_edge_count": source_attack_edge_count,
    }


def rewindow_run(
    source_tool_dir: Path,
    dest_tool_dir: Path,
    split: str,
    run_entry: dict[str, Any],
    target_window_size_seconds: int,
    used_file_names: set[str],
    totals: dict[str, int],
) -> dict[str, Any]:
    node_attrs_by_id, edges, source_counts = load_run_edge_stream(source_tool_dir, split, run_entry)
    if not edges:
        raise ValueError(f"Run {run_entry.get('run_id')} has no edges")

    target_window_ns = target_window_size_seconds * NS_PER_SECOND
    exported_windows: list[dict[str, Any]] = []
    current_graph: Any = None
    current_start_ns: int | None = None
    current_end_ns: int | None = None
    current_attack_edge_count = 0

    construction_dir = dest_tool_dir / "construction" / "nx" / f"graph_{split}"
    transformation_dir = dest_tool_dir / "transformation" / "nx" / f"graph_{split}"

    def flush_window() -> None:
        nonlocal current_graph, current_start_ns, current_end_ns, current_attack_edge_count
        if current_graph is None or current_start_ns is None or current_end_ns is None:
            return
        if current_graph.number_of_edges() == 0:
            return

        file_name = graph_file_name(current_start_ns, current_end_ns)
        if file_name in used_file_names:
            raise ValueError(f"Duplicate derived graph filename in split {split}: {file_name}")
        used_file_names.add(file_name)

        construction_path = construction_dir / file_name
        transformation_path = transformation_dir / file_name
        torch.save(current_graph, str(construction_path))
        shutil.copy2(construction_path, transformation_path)

        edge_count = current_graph.number_of_edges()
        node_count = current_graph.number_of_nodes()
        exported_windows.append(
            {
                "window_index": len(exported_windows) + 1,
                "file_name": file_name,
                "node_count": node_count,
                "edge_count": edge_count,
                "attack_edge_count": current_attack_edge_count,
                "y": int(current_attack_edge_count > 0),
                "start_ns": current_start_ns,
                "end_ns": current_end_ns,
            }
        )
        totals["derived_edge_count"] += edge_count
        totals["derived_attack_edge_count"] += current_attack_edge_count
        current_graph = None
        current_start_ns = None
        current_end_ns = None
        current_attack_edge_count = 0

    for sort_key, src, dst, attrs in edges:
        wall_time = int(sort_key[0])
        if current_graph is None:
            current_graph = nx.MultiDiGraph()
            current_start_ns = wall_time
        elif current_start_ns is not None and wall_time >= current_start_ns + target_window_ns:
            flush_window()
            current_graph = nx.MultiDiGraph()
            current_start_ns = wall_time

        assert current_graph is not None
        if src not in node_attrs_by_id:
            raise ValueError(f"Missing node attrs for src={src!r} in run {run_entry.get('run_id')}")
        if dst not in node_attrs_by_id:
            raise ValueError(f"Missing node attrs for dst={dst!r} in run {run_entry.get('run_id')}")
        if src not in current_graph:
            current_graph.add_node(src, **node_attrs_by_id[src])
        if dst not in current_graph:
            current_graph.add_node(dst, **node_attrs_by_id[dst])
        current_graph.add_edge(src, dst, **attrs)
        current_attack_edge_count += int(attrs.get("y", 0))
        current_end_ns = wall_time

    flush_window()
    derived_edge_count = sum(int(window["edge_count"]) for window in exported_windows)
    derived_attack_edge_count = sum(int(window["attack_edge_count"]) for window in exported_windows)
    if derived_edge_count != source_counts["source_edge_count"]:
        raise ValueError(
            f"Edge conservation failed for {run_entry.get('run_id')}: "
            f"source={source_counts['source_edge_count']} derived={derived_edge_count}"
        )
    if derived_attack_edge_count != source_counts["source_attack_edge_count"]:
        raise ValueError(
            f"Attack edge conservation failed for {run_entry.get('run_id')}: "
            f"source={source_counts['source_attack_edge_count']} derived={derived_attack_edge_count}"
        )

    totals["source_edge_count"] += source_counts["source_edge_count"]
    totals["source_attack_edge_count"] += source_counts["source_attack_edge_count"]
    totals["derived_window_count"] += len(exported_windows)

    return {
        "run_id": run_entry["run_id"],
        "window_count": len(exported_windows),
        "attack_window_count": sum(int(window["y"]) for window in exported_windows),
        "node_count": sum(int(window["node_count"]) for window in exported_windows),
        "edge_count": derived_edge_count,
        "attack_edge_count": derived_attack_edge_count,
        "attack_windows": list(run_entry.get("attack_windows") or []),
        "windows": exported_windows,
    }


def derive_export(
    source_root: Path,
    output_root: Path,
    tool: str,
    target_window_size_seconds: int,
    replace: bool,
) -> Path:
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    if source_root == output_root:
        raise ValueError("Output root must differ from source root")
    if source_root in output_root.parents:
        raise ValueError("Output root must not be inside the source export root")
    if output_root.exists() and not replace:
        raise FileExistsError(f"Output root exists; pass --replace to rebuild: {output_root}")

    source_manifest_path = source_root / "export_manifest.json"
    if not source_manifest_path.exists():
        raise FileNotFoundError(f"Missing source export manifest: {source_manifest_path}")
    source_manifest = load_json(source_manifest_path)
    source_tool_dir = resolve_tool_export_dir(source_root, tool, source_manifest)
    source_index_path = source_tool_dir / "export_index.json"
    if not source_index_path.exists():
        raise FileNotFoundError(f"Missing source export index: {source_index_path}")
    source_index = load_json(source_index_path)
    source_window_size_seconds = int(source_index.get("window_size_seconds", 0))
    if source_window_size_seconds <= 0:
        raise ValueError(f"Invalid source window_size_seconds in {source_index_path}")
    if target_window_size_seconds <= source_window_size_seconds:
        raise ValueError(
            "Target window size must be larger than source window size: "
            f"source={source_window_size_seconds}, target={target_window_size_seconds}"
        )

    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(tempfile.mkdtemp(prefix=f".{output_root.name}.tmp-", dir=str(output_root.parent)))
    dest_tool_dir = temp_root / tool / "raw"
    try:
        copy_static_construction_metadata(source_tool_dir, dest_tool_dir)
        derived_splits: dict[str, list[dict[str, Any]]] = {}
        used_file_names_by_split = {split: set() for split in SPLITS}
        totals = {
            "source_edge_count": 0,
            "source_attack_edge_count": 0,
            "derived_edge_count": 0,
            "derived_attack_edge_count": 0,
            "derived_window_count": 0,
        }

        for split in SPLITS:
            derived_splits[split] = []
            for run_entry in list(source_index.get("splits", {}).get(split, [])):
                print(f"derive split={split} run={run_entry.get('run_id')} target={target_window_size_seconds}s", flush=True)
                derived_splits[split].append(
                    rewindow_run(
                        source_tool_dir,
                        dest_tool_dir,
                        split,
                        run_entry,
                        target_window_size_seconds,
                        used_file_names_by_split[split],
                        totals,
                    )
                )

        if totals["source_edge_count"] != totals["derived_edge_count"]:
            raise ValueError(f"Global edge conservation failed: {totals}")
        if totals["source_attack_edge_count"] != totals["derived_attack_edge_count"]:
            raise ValueError(f"Global attack edge conservation failed: {totals}")

        derived_index = dict(source_index)
        derived_index.update(
            {
                "window_size_seconds": target_window_size_seconds,
                "window_count": totals["derived_window_count"],
                "splits": derived_splits,
                "derived_from": {
                    "source_export_root": str(source_root),
                    "source_tool_export_dir": str(source_tool_dir),
                    "source_window_size_seconds": source_window_size_seconds,
                    "method": "rewindow_existing_graph_edges_by_wall_time",
                },
            }
        )
        write_json(dest_tool_dir / "export_index.json", derived_index)

        derived_manifest = dict(source_manifest)
        tool_exports = dict(derived_manifest.get("tool_exports") or {})
        tool_exports[tool] = str(output_root / tool / "raw")
        derived_manifest.update(
            {
                "window_size_seconds": target_window_size_seconds,
                "tool_exports": tool_exports,
                "derived_from": {
                    "source_export_root": str(source_root),
                    "source_window_size_seconds": source_window_size_seconds,
                    "method": "rewindow_existing_graph_edges_by_wall_time",
                },
            }
        )
        write_json(temp_root / "export_manifest.json", derived_manifest)
        write_json(
            temp_root / "derive_validation.json",
            {
                "tool": tool,
                "source_export_root": str(source_root),
                "output_root": str(output_root),
                "source_window_size_seconds": source_window_size_seconds,
                "target_window_size_seconds": target_window_size_seconds,
                "totals": totals,
            },
        )

        if output_root.exists():
            shutil.rmtree(output_root)
        temp_root.rename(output_root)
        print(f"Wrote derived export: {output_root}", flush=True)
        return output_root
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--tool", default="recap")
    parser.add_argument("--window-size-seconds", type=int, required=True)
    parser.add_argument("--replace", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    derive_export(
        source_root=args.source_root,
        output_root=args.output_root,
        tool=args.tool,
        target_window_size_seconds=args.window_size_seconds,
        replace=args.replace,
    )


if __name__ == "__main__":
    main()
