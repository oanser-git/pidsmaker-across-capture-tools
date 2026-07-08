"""Data structures shared by the Orange pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


JsonDict = dict[str, Any]


@dataclass(slots=True)
class RunMetadata:
    run_id: str
    tool: str
    scenario: str
    kind: str
    source_file: str
    file_name: str
    attack_windows: list[JsonDict] = field(default_factory=list)


@dataclass(slots=True)
class NodeRecord:
    raw_node_id: str
    prov_type: str | None
    line_number: int
    global_node_id: str | None = None
    is_placeholder: bool = False
    attributes: JsonDict = field(default_factory=dict)
    features: JsonDict = field(default_factory=dict)
    raw: JsonDict = field(default_factory=dict)


@dataclass(slots=True)
class EdgeRecord:
    raw_edge_id: str | None
    src_raw_node_id: str
    dst_raw_node_id: str
    prov_type: str | None
    line_number: int
    timestamp: JsonDict
    global_edge_id: str | None = None
    src_global_node_id: str | None = None
    dst_global_node_id: str | None = None
    edge_label: str | None = None
    attributes: JsonDict = field(default_factory=dict)
    features: JsonDict = field(default_factory=dict)
    raw: JsonDict = field(default_factory=dict)

@dataclass(slots=True)
class ReferenceRun:
    metadata: RunMetadata
    nodes: list[NodeRecord]
    edges: list[EdgeRecord]

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    @property
    def record_count(self) -> int:
        return self.node_count + self.edge_count
