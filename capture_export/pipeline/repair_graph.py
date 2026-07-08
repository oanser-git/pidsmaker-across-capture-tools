"""Repair missing endpoints and duplicate nodes without dropping edges."""

from __future__ import annotations

from copy import deepcopy

from capture_export.pipeline.models import EdgeRecord, JsonDict, NodeRecord, RunMetadata


UNKNOWN_VALUE = "<unknown>"


def raw_record_copy(raw: JsonDict) -> JsonDict:
    clean_raw = dict(raw)
    clean_raw.pop("_repair", None)
    return deepcopy(clean_raw)


def ensure_duplicate_metadata(node: NodeRecord) -> JsonDict:
    repair = node.raw.get("_repair")
    if isinstance(repair, dict):
        return repair

    repair = {
        "duplicate_count": 1,
        "duplicate_exact_count": 0,
        "duplicate_conflict_count": 0,
        "conflicting_fields": [],
        "source_line_numbers": [node.line_number],
        "source_records": [raw_record_copy(node.raw)],
    }
    node.raw["_repair"] = repair
    return repair


def append_source_record(node: NodeRecord, incoming: NodeRecord) -> JsonDict:
    repair = ensure_duplicate_metadata(node)
    repair["duplicate_count"] += 1
    repair["source_line_numbers"].append(incoming.line_number)
    repair["source_records"].append(raw_record_copy(incoming.raw))
    return repair


def add_conflicting_field(repair: JsonDict, field_name: str) -> None:
    conflicting_fields = repair["conflicting_fields"]
    if field_name not in conflicting_fields:
        conflicting_fields.append(field_name)


def is_exact_duplicate(existing: NodeRecord, incoming: NodeRecord) -> bool:
    return (
        existing.prov_type == incoming.prov_type
        and raw_record_copy(existing.raw) == raw_record_copy(incoming.raw)
    )


def is_missing_value(value: object) -> bool:
    return value is None or value == ""


def merge_attribute_value(existing_value: object, incoming_value: object) -> object:
    if existing_value == incoming_value:
        return existing_value
    if is_missing_value(existing_value):
        return incoming_value
    if is_missing_value(incoming_value):
        return existing_value
    return UNKNOWN_VALUE


def merge_node_records(existing: NodeRecord, incoming: NodeRecord) -> NodeRecord:
    repair = append_source_record(existing, incoming)

    if is_exact_duplicate(existing, incoming):
        repair["duplicate_exact_count"] += 1
        return existing

    had_conflict = False

    if existing.prov_type != incoming.prov_type:
        existing.prov_type = None
        add_conflicting_field(repair, "prov_type")
        had_conflict = True

    for key, incoming_value in incoming.attributes.items():
        if key not in existing.attributes:
            existing.attributes[key] = incoming_value
            continue

        merged_value = merge_attribute_value(existing.attributes[key], incoming_value)
        if merged_value == UNKNOWN_VALUE and existing.attributes[key] != incoming_value:
            add_conflicting_field(repair, key)
            had_conflict = True
        existing.attributes[key] = merged_value

    if had_conflict:
        repair["duplicate_conflict_count"] += 1

    return existing


def deduplicate_nodes(nodes: list[NodeRecord]) -> list[NodeRecord]:
    deduped: dict[str, NodeRecord] = {}

    for node in nodes:
        if node.raw_node_id not in deduped:
            deduped[node.raw_node_id] = node
            continue

        existing = deduped[node.raw_node_id]
        deduped[node.raw_node_id] = merge_node_records(existing, node)

    return list(deduped.values())


def normalize_type_hint(value: object) -> str | None:
    if not isinstance(value, str):
        return None

    hint = " ".join(value.strip().lower().split())
    return hint or None


def get_missing_endpoint_type_hint(edge: EdgeRecord, side: str) -> str | None:
    if side == "src":
        return normalize_type_hint(edge.attributes.get("from_type"))
    return normalize_type_hint(edge.attributes.get("to_type"))


def make_placeholder_node(raw_node_id: str, metadata: RunMetadata, raw_type_hint: str | None) -> NodeRecord:
    repair_metadata: JsonDict = {
        "missing_endpoint": True,
        "original_id": raw_node_id,
        "run_id": metadata.run_id,
        "tool": metadata.tool,
        "scenario": metadata.scenario,
        "raw_type_hints": [raw_type_hint] if raw_type_hint is not None else [],
    }

    attributes: JsonDict = {
        "object_type": "<missing>",
        "_missing_endpoint": True,
        "_placeholder_reason": "missing_edge_endpoint",
    }
    if raw_type_hint is not None:
        attributes["_raw_type_hint"] = raw_type_hint

    return NodeRecord(
        raw_node_id=raw_node_id,
        prov_type=None,
        line_number=0,
        is_placeholder=True,
        attributes=attributes,
        raw={"id": raw_node_id, "placeholder": True, "_repair": repair_metadata},
    )


def update_placeholder_type_hint(node: NodeRecord, raw_type_hint: str | None) -> None:
    if raw_type_hint is None:
        return

    repair = node.raw.get("_repair")
    if not isinstance(repair, dict):
        return

    raw_type_hints = repair.setdefault("raw_type_hints", [])
    if raw_type_hint not in raw_type_hints:
        raw_type_hints.append(raw_type_hint)

    if len(raw_type_hints) == 1:
        node.attributes["_raw_type_hint"] = raw_type_hints[0]
    else:
        node.attributes["_raw_type_hint"] = UNKNOWN_VALUE


def add_placeholder_nodes_for_missing_endpoints(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    metadata: RunMetadata,
) -> list[NodeRecord]:
    by_id = {node.raw_node_id: node for node in nodes}

    for edge in edges:
        if edge.src_raw_node_id not in by_id:
            by_id[edge.src_raw_node_id] = make_placeholder_node(
                edge.src_raw_node_id,
                metadata,
                get_missing_endpoint_type_hint(edge, "src"),
            )
        elif by_id[edge.src_raw_node_id].is_placeholder:
            update_placeholder_type_hint(
                by_id[edge.src_raw_node_id],
                get_missing_endpoint_type_hint(edge, "src"),
            )

        if edge.dst_raw_node_id not in by_id:
            by_id[edge.dst_raw_node_id] = make_placeholder_node(
                edge.dst_raw_node_id,
                metadata,
                get_missing_endpoint_type_hint(edge, "dst"),
            )
        elif by_id[edge.dst_raw_node_id].is_placeholder:
            update_placeholder_type_hint(
                by_id[edge.dst_raw_node_id],
                get_missing_endpoint_type_hint(edge, "dst"),
            )

    return list(by_id.values())


# Policy summary:
# - Never drop dangling edges. We keep them because missing endpoints are part of
#   the tool output quality we want to benchmark.
# - Create one placeholder node per missing raw endpoint ID per run. This keeps
#   the graph connected without creating fake paths, commands, or IPs.
# - Build one logical node per raw node ID per run. Duplicate node records are
#   exporter noise unless proven otherwise, so they must not become fake graph structure.
# - Exact duplicates are collapsed. Complementary duplicates are merged.
#   Conflicting fields are neutralized to <unknown> and kept in side metadata.
def repair_graph(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    metadata: RunMetadata,
) -> tuple[list[NodeRecord], list[EdgeRecord]]:
    repaired_nodes = deduplicate_nodes(nodes)
    repaired_nodes = add_placeholder_nodes_for_missing_endpoints(repaired_nodes, edges, metadata)
    return repaired_nodes, edges
