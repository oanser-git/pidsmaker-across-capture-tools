"""Assign stable node and edge identifiers inside each run."""

from __future__ import annotations

import hashlib

from capture_export.pipeline.models import ReferenceRun


def stable_hex(*parts: str) -> str:
    payload = "||".join(parts).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def build_global_node_id(run_id: str, raw_node_id: str) -> str:
    return f"node_{stable_hex(run_id, raw_node_id)}"


def build_global_edge_id(
    run_id: str,
    raw_edge_id: str | None,
    src_raw_node_id: str,
    dst_raw_node_id: str,
    timestamp: str | int | None,
    line_number: int,
) -> str:
    return (
        f"edge_{stable_hex(run_id, str(raw_edge_id), src_raw_node_id, dst_raw_node_id, str(timestamp), str(line_number))}"
    )


def assign_stable_ids(reference_run: ReferenceRun) -> ReferenceRun:
    node_id_map: dict[str, str] = {}

    for node in reference_run.nodes:
        node.global_node_id = build_global_node_id(reference_run.metadata.run_id, node.raw_node_id)
        node_id_map[node.raw_node_id] = node.global_node_id

    for edge in reference_run.edges:
        if edge.src_raw_node_id not in node_id_map or edge.dst_raw_node_id not in node_id_map:
            raise ValueError("Cannot assign edge IDs before all edge endpoints exist as nodes.")

        edge.src_global_node_id = node_id_map[edge.src_raw_node_id]
        edge.dst_global_node_id = node_id_map[edge.dst_raw_node_id]
        edge.global_edge_id = build_global_edge_id(
            run_id=reference_run.metadata.run_id,
            raw_edge_id=edge.raw_edge_id,
            src_raw_node_id=edge.src_raw_node_id,
            dst_raw_node_id=edge.dst_raw_node_id,
            timestamp=edge.timestamp,
            line_number=edge.line_number,
        )

    return reference_run
