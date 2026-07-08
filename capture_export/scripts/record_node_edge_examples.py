#!/usr/bin/env python3
"""Print simple examples explaining records, nodes, edges, duplicates, and dangling edges."""

from pathlib import Path
import importlib
import importlib.util
import json
import sys

orjson = None
if importlib.util.find_spec("orjson") is not None:
    orjson = importlib.import_module("orjson")


DEFAULT_DATA_RAW = (
    Path(__file__).resolve().parents[1]
    / "houssel.paul-provenance-graphs-dataset"
    / "data-raw"
)
DEFAULT_SCENARIO = "openssl_CVE-2022-1292"


def parse_tool(path):
    return path.stem.rsplit("_", 1)[-1]


def load_json_line(line):
    if orjson is not None:
        return orjson.loads(line)
    return json.loads(line)


def one_line_json(obj, limit=700):
    text = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def find_first_node_and_edge(path):
    first_node = None
    first_edge = None
    with path.open("rb") as handle:
        for line in handle:
            try:
                obj = load_json_line(line)
            except Exception:
                continue
            is_edge = "from" in obj and "to" in obj
            is_node = "id" in obj and not is_edge
            if first_node is None and is_node:
                first_node = obj
            if first_edge is None and is_edge:
                first_edge = obj
            if first_node is not None and first_edge is not None:
                break
    return first_node, first_edge


def find_duplicate_node_example(path):
    seen = {}
    with path.open("rb") as handle:
        for line_no, line in enumerate(handle, 1):
            try:
                obj = load_json_line(line)
            except Exception:
                continue
            is_edge = "from" in obj and "to" in obj
            is_node = "id" in obj and not is_edge
            if not is_node:
                continue
            node_id = str(obj.get("id"))
            if node_id in seen:
                return node_id, seen[node_id], (line_no, obj)
            seen[node_id] = (line_no, obj)
    return None


def find_dangling_edge_example(path):
    node_ids = set()
    with path.open("rb") as handle:
        for line in handle:
            try:
                obj = load_json_line(line)
            except Exception:
                continue
            if "id" in obj and "from" not in obj and "to" not in obj:
                node_ids.add(str(obj.get("id")))

    with path.open("rb") as handle:
        for line_no, line in enumerate(handle, 1):
            try:
                obj = load_json_line(line)
            except Exception:
                continue
            if "from" in obj and "to" in obj:
                src = str(obj.get("from"))
                dst = str(obj.get("to"))
                src_missing = src not in node_ids
                dst_missing = dst not in node_ids
                if src_missing or dst_missing:
                    return line_no, obj, src_missing, dst_missing
    return None


def main():
    data_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_RAW
    scenario = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SCENARIO
    files = sorted(data_raw.glob(f"{scenario}_*.jsonl"))

    if not files:
        raise SystemExit(f"No files found for scenario {scenario!r} in {data_raw}")

    print("RECORD / NODE / EDGE EXAMPLES")
    print(f"data_raw={data_raw}")
    print(f"scenario={scenario}")
    print("\nMeaning:")
    print("  record = one JSON object / one line in a JSONL file")
    print("  node record = has id and no from/to")
    print("  edge record = has from and to")
    print("  dangling edge = source or destination ID is not defined as a node in the same file")
    print("  duplicate node record = same node id appears more than once as a node record")

    for path in files:
        tool = parse_tool(path)
        print(f"\nTOOL {tool}")
        print(f"file={path.name}")

        first_node, first_edge = find_first_node_and_edge(path)
        print("  first_node_record:")
        print("    " + (one_line_json(first_node) if first_node else "none"))
        print("  first_edge_record:")
        print("    " + (one_line_json(first_edge) if first_edge else "none"))

        duplicate = find_duplicate_node_example(path)
        print("  duplicate_node_example:")
        if duplicate is None:
            print("    none")
        else:
            node_id, first, second = duplicate
            print(f"    node_id={node_id}")
            print(f"    first_line={first[0]} first_record={one_line_json(first[1])}")
            print(f"    second_line={second[0]} second_record={one_line_json(second[1])}")

        dangling = find_dangling_edge_example(path)
        print("  dangling_edge_example:")
        if dangling is None:
            print("    none")
        else:
            line_no, edge, src_missing, dst_missing = dangling
            print(
                f"    line={line_no} src_missing={src_missing} "
                f"dst_missing={dst_missing} edge={one_line_json(edge)}"
            )


if __name__ == "__main__":
    main()
