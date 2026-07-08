"""Build common edge labels while preserving the raw edge metadata."""

from __future__ import annotations

from typing import Any

from capture_export.pipeline.models import EdgeRecord


RELATION_KEYS = ("relation_type", "relation", "cf:relation", "prov:relation")


def relation_from(attributes: dict[str, Any]) -> tuple[str, Any]:
    for key in RELATION_KEYS:
        if key in attributes:
            return key, attributes.get(key)
    return "<missing>", None


def normalize_relation_value(value: Any) -> str:
    if value is None:
        return "<missing>"

    normalized = "_".join(str(value).strip().lower().split())
    return normalized or "<missing>"


def normalize_prov_type(prov_type: str | None) -> str:
    return prov_type.strip() if isinstance(prov_type, str) and prov_type.strip() else "<missing>"


def build_edge_label(prov_type: str | None, relation: str) -> str:
    left = normalize_prov_type(prov_type)
    right = relation
    return f"{left}:{right}"


def normalize_edges(edges: list[EdgeRecord]) -> None:
    for edge in edges:
        relation_field, raw_relation_value = relation_from(edge.attributes)
        normalized_relation = normalize_relation_value(raw_relation_value)

        edge.attributes["relation_field"] = relation_field
        edge.attributes["relation_value"] = normalized_relation
        edge.attributes["_raw_relation_value"] = (
            str(raw_relation_value) if raw_relation_value is not None else "<missing>"
        )
        edge.edge_label = build_edge_label(edge.prov_type, normalized_relation)
