#!/usr/bin/env python3
"""Print schema vocabulary by tool: PROV types, object types, relations, and keys."""

from collections import Counter, defaultdict
from pathlib import Path
import importlib
import importlib.util
import json
import sys
from typing import Any

orjson = None
if importlib.util.find_spec("orjson") is not None:
    orjson = importlib.import_module("orjson")


DEFAULT_DATA_RAW = (
    Path(__file__).resolve().parents[1]
    / "houssel.paul-provenance-graphs-dataset"
    / "data-raw"
)


def pct(part, total):
    return f"{(part / total * 100):.2f}%" if total else "0.00%"


def parse_tool(path):
    return path.stem.rsplit("_", 1)[-1]


def relation_from(annotations):
    for key in ("relation_type", "relation", "cf:relation", "prov:relation"):
        if key in annotations:
            return key, annotations.get(key)
    return "<missing>", "<missing>"


def load_json_line(line):
    if orjson is not None:
        return orjson.loads(line)
    return json.loads(line)


def main():
    data_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_RAW
    files = sorted(data_raw.glob("*.jsonl"))

    if not files:
        raise SystemExit(f"No JSONL files found in {data_raw}")

    vocab: defaultdict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "records": 0,
            "nodes": 0,
            "edges": 0,
            "parse_errors": 0,
            "prov_types": Counter(),
            "object_types": Counter(),
            "relations": Counter(),
            "relation_fields": Counter(),
            "node_keys": Counter(),
            "edge_keys": Counter(),
        }
    )

    for index, path in enumerate(files, 1):
        tool = parse_tool(path)
        with path.open("rb") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = load_json_line(line)
                except Exception:
                    vocab[tool]["parse_errors"] += 1
                    continue

                annotations = obj.get("annotations") or {}
                if not isinstance(annotations, dict):
                    annotations = {}

                vocab[tool]["records"] += 1
                vocab[tool]["prov_types"][obj.get("type", "<missing>")] += 1

                is_edge = "from" in obj and "to" in obj
                is_node = "id" in obj and not is_edge

                if is_edge:
                    relation_field, relation_value = relation_from(annotations)
                    vocab[tool]["edges"] += 1
                    vocab[tool]["relation_fields"][relation_field] += 1
                    vocab[tool]["relations"][relation_value] += 1
                    for key in annotations:
                        vocab[tool]["edge_keys"][key] += 1
                elif is_node:
                    vocab[tool]["nodes"] += 1
                    vocab[tool]["object_types"][annotations.get("object_type", "<missing>")] += 1
                    for key in annotations:
                        vocab[tool]["node_keys"][key] += 1

        print(f"processed {index}/{len(files)} {path.name}", flush=True)

    print("\nSCHEMA VOCABULARY BY TOOL")
    print(f"data_raw={data_raw}")

    for tool in sorted(vocab):
        data = vocab[tool]
        print(f"\nTOOL {tool}")
        print(
            f"  records={data['records']} nodes={data['nodes']} "
            f"edges={data['edges']} parse_errors={data['parse_errors']}"
        )
        print(
            f"  prov_types_count={len(data['prov_types'])} "
            f"values={', '.join(sorted(str(k) for k in data['prov_types']))}"
        )
        print(
            f"  object_types_count={len(data['object_types'])} "
            f"values={', '.join(sorted(str(k) for k in data['object_types']))}"
        )
        print(
            f"  relations_count={len(data['relations'])} "
            f"relation_fields={', '.join(f'{k}:{v}' for k, v in data['relation_fields'].most_common())}"
        )
        print("  top_relations")
        for relation, count in data["relations"].most_common(20):
            print(f"    {relation}: {count} ({pct(count, data['edges'])})")
        print(
            f"  node_annotation_keys_count={len(data['node_keys'])} "
            f"values={', '.join(sorted(data['node_keys']))}"
        )
        print(
            f"  edge_annotation_keys_count={len(data['edge_keys'])} "
            f"values={', '.join(sorted(data['edge_keys']))}"
        )


if __name__ == "__main__":
    main()
