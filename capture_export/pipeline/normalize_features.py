"""Normalize shared node and edge features without doing model-specific tokenization or embeddings."""

from __future__ import annotations

from capture_export.pipeline.models import EdgeRecord, NodeRecord, ReferenceRun


NODE_TEXT_KEYS = {
    "path": ("path", "pathname", "filename"),
    "process_name": ("comm", "processName"),
    "command_line": ("cmdLine", "cmd_line", "cmd"),
    "working_directory": ("cwd",),
    "network_address": ("address", "remote_ip", "local_ip", "src_addr", "dst_addr", "sender", "receiver"),
    "port": ("port", "remote_port", "local_port", "src_port", "dst_port"),
    "network_family": ("family",),
}


def normalize_text(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text in {"<missing>", "<unknown>"}:
        return None
    return text


def first_normalized_value(attributes: dict[str, object], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = normalize_text(attributes.get(key))
        if value is not None:
            return value
    return None


def build_node_display_name(features: dict[str, str | None]) -> str | None:
    if features["process_name"] is not None:
        return features["process_name"]
    if features["path"] is not None:
        return features["path"]
    if features["network_address"] is not None and features["port"] is not None:
        return f"{features['network_address']}:{features['port']}"
    if features["network_address"] is not None:
        return features["network_address"]
    return None


def normalize_node_features(nodes: list[NodeRecord]) -> None:
    for node in nodes:
        features = {
            feature_name: first_normalized_value(node.attributes, keys)
            for feature_name, keys in NODE_TEXT_KEYS.items()
        }
        features["display_name"] = build_node_display_name(features)
        node.features = features


def normalize_edge_features(edges: list[EdgeRecord]) -> None:
    for edge in edges:
        timestamp_source = None
        if "cf:date" in edge.attributes:
            timestamp_source = "cf:date"
        elif "ts" in edge.attributes:
            timestamp_source = "ts"

        edge.features = {
            "prov_type": edge.prov_type,
            "relation": normalize_text(edge.attributes.get("relation_value")),
        }
        edge.attributes["timestamp_source"] = timestamp_source


def normalize_run_features(reference_run: ReferenceRun) -> ReferenceRun:
    normalize_node_features(reference_run.nodes)
    normalize_edge_features(reference_run.edges)
    return reference_run
