#!/usr/bin/env python3
"""Print dangling edge and duplicate node summaries by tool and file."""

from collections import defaultdict
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
    return f"{(part / total * 100):.3f}%" if total else "0.000%"


def parse_name(path):
    stem = path.stem
    tool = stem.rsplit("_", 1)[-1]
    scenario = stem[: -(len(tool) + 1)]
    kind = "benign" if "_benign_" in scenario else "cve"
    return scenario, tool, kind


def load_json_line(line):
    if orjson is not None:
        return orjson.loads(line)
    return json.loads(line)


def main():
    data_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_RAW
    files = sorted(data_raw.glob("*.jsonl"))

    if not files:
        raise SystemExit(f"No JSONL files found in {data_raw}")

    stats: defaultdict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "files": 0,
            "nodes": 0,
            "edges": 0,
            "parse_errors": 0,
            "duplicate_node_records": 0,
            "edges_missing_when_seen": 0,
            "true_dangling_edges": 0,
            "missing_src_edges": 0,
            "missing_dst_edges": 0,
        }
    )
    file_rows = []

    for index, path in enumerate(files, 1):
        scenario, tool, kind = parse_name(path)
        node_ids = set()
        seen_nodes = set()
        row: dict[str, Any] = {
            "file": path.name,
            "scenario": scenario,
            "tool": tool,
            "kind": kind,
            "nodes": 0,
            "edges": 0,
            "parse_errors": 0,
            "duplicate_node_records": 0,
            "edges_missing_when_seen": 0,
            "true_dangling_edges": 0,
            "missing_src_edges": 0,
            "missing_dst_edges": 0,
        }

        with path.open("rb") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = load_json_line(line)
                except Exception:
                    row["parse_errors"] += 1
                    continue

                is_edge = "from" in obj and "to" in obj
                is_node = "id" in obj and not is_edge

                if is_node:
                    row["nodes"] += 1
                    node_id = str(obj.get("id"))
                    if node_id in node_ids:
                        row["duplicate_node_records"] += 1
                    node_ids.add(node_id)
                    seen_nodes.add(node_id)
                elif is_edge:
                    row["edges"] += 1
                    src = str(obj.get("from"))
                    dst = str(obj.get("to"))
                    if src not in seen_nodes or dst not in seen_nodes:
                        row["edges_missing_when_seen"] += 1

        with path.open("rb") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = load_json_line(line)
                except Exception:
                    continue

                if "from" in obj and "to" in obj:
                    src_missing = str(obj.get("from")) not in node_ids
                    dst_missing = str(obj.get("to")) not in node_ids
                    if src_missing:
                        row["missing_src_edges"] += 1
                    if dst_missing:
                        row["missing_dst_edges"] += 1
                    if src_missing or dst_missing:
                        row["true_dangling_edges"] += 1

        st: dict[str, Any] = stats[tool]
        st["files"] += 1
        for key in (
            "nodes",
            "edges",
            "parse_errors",
            "duplicate_node_records",
            "edges_missing_when_seen",
            "true_dangling_edges",
            "missing_src_edges",
            "missing_dst_edges",
        ):
            st[key] += row[key]
        file_rows.append(row)

        print(f"checked {index}/{len(files)} {path.name}", flush=True)

    print("\nGRAPH INTEGRITY BY TOOL")
    print(f"data_raw={data_raw}")

    for tool in sorted(stats):
        st = stats[tool]
        print(f"\nTOOL {tool}")
        print(
            f"  files={st['files']} nodes={st['nodes']} edges={st['edges']} "
            f"parse_errors={st['parse_errors']}"
        )
        print(
            f"  duplicate_node_records={st['duplicate_node_records']} "
            f"({pct(st['duplicate_node_records'], st['nodes'])} of nodes)"
        )
        print(
            f"  edges_missing_when_seen={st['edges_missing_when_seen']} "
            f"({pct(st['edges_missing_when_seen'], st['edges'])} of edges)"
        )
        print(
            f"  true_dangling_edges={st['true_dangling_edges']} "
            f"({pct(st['true_dangling_edges'], st['edges'])} of edges)"
        )
        print(
            f"  missing_src_edges={st['missing_src_edges']} "
            f"({pct(st['missing_src_edges'], st['edges'])} of edges)"
        )
        print(
            f"  missing_dst_edges={st['missing_dst_edges']} "
            f"({pct(st['missing_dst_edges'], st['edges'])} of edges)"
        )

    print("\nPER FILE ANOMALIES")
    for row in file_rows:
        if row["parse_errors"] or row["duplicate_node_records"] or row["true_dangling_edges"]:
            print(
                f"  {row['file']}: tool={row['tool']} kind={row['kind']} "
                f"parse_errors={row['parse_errors']} "
                f"duplicate_node_records={row['duplicate_node_records']} "
                f"({pct(row['duplicate_node_records'], row['nodes'])} of nodes) "
                f"true_dangling_edges={row['true_dangling_edges']} "
                f"({pct(row['true_dangling_edges'], row['edges'])} of edges) "
                f"missing_src_edges={row['missing_src_edges']} "
                f"missing_dst_edges={row['missing_dst_edges']} "
                f"edges_missing_when_seen={row['edges_missing_when_seen']}"
            )


if __name__ == "__main__":
    main()
