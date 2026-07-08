#!/usr/bin/env python3
"""Print record, node, and edge counts by tool and file."""

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
            "bytes": 0,
            "records": 0,
            "nodes": 0,
            "edges": 0,
            "other": 0,
            "parse_errors": 0,
            "types": Counter(),
        }
    )
    file_rows = []

    for index, path in enumerate(files, 1):
        scenario, tool, kind = parse_name(path)
        row: dict[str, Any] = {
            "file": path.name,
            "scenario": scenario,
            "tool": tool,
            "kind": kind,
            "bytes": path.stat().st_size,
            "records": 0,
            "nodes": 0,
            "edges": 0,
            "other": 0,
            "parse_errors": 0,
            "types": Counter(),
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

                row["records"] += 1
                row["types"][obj.get("type", "<missing>")] += 1

                is_edge = "from" in obj and "to" in obj
                is_node = "id" in obj and not is_edge
                if is_edge:
                    row["edges"] += 1
                elif is_node:
                    row["nodes"] += 1
                else:
                    row["other"] += 1

        st: dict[str, Any] = stats[tool]
        st["files"] += 1
        st["bytes"] += row["bytes"]
        st["records"] += row["records"]
        st["nodes"] += row["nodes"]
        st["edges"] += row["edges"]
        st["other"] += row["other"]
        st["parse_errors"] += row["parse_errors"]
        st["types"].update(row["types"])
        file_rows.append(row)

        print(f"processed {index}/{len(files)} {path.name}", flush=True)

    total_records = sum(st["records"] for st in stats.values())
    total_nodes = sum(st["nodes"] for st in stats.values())
    total_edges = sum(st["edges"] for st in stats.values())
    total_bytes = sum(st["bytes"] for st in stats.values())

    print("\nRECORD PROFILE BY TOOL")
    print(f"data_raw={data_raw}")
    print(f"files={len(files)}")
    print(f"records={total_records}")
    print(f"nodes={total_nodes}")
    print(f"edges={total_edges}")

    for tool in sorted(stats):
        st = stats[tool]
        print(f"\nTOOL {tool}")
        print(
            f"  files={st['files']} "
            f"size_gib={st['bytes'] / 1024 / 1024 / 1024:.2f} ({pct(st['bytes'], total_bytes)})"
        )
        print(
            f"  records={st['records']} ({pct(st['records'], total_records)}) "
            f"nodes={st['nodes']} ({pct(st['nodes'], total_nodes)}) "
            f"edges={st['edges']} ({pct(st['edges'], total_edges)}) "
            f"other={st['other']} parse_errors={st['parse_errors']}"
        )
        print("  top_types=" + ", ".join(f"{k}:{v}" for k, v in st["types"].most_common(10)))

    print("\nPER FILE SUMMARY")
    for row in file_rows:
        top_types = ", ".join(f"{k}:{v}" for k, v in row["types"].most_common(3))
        print(
            f"  {row['file']}: tool={row['tool']} kind={row['kind']} "
            f"size_mb={row['bytes'] / 1024 / 1024:.1f} "
            f"records={row['records']} nodes={row['nodes']} edges={row['edges']} "
            f"other={row['other']} parse_errors={row['parse_errors']} "
            f"top_types=[{top_types}]"
        )


if __name__ == "__main__":
    main()
