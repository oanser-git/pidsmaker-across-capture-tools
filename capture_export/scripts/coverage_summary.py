#!/usr/bin/env python3
"""Print file coverage and size summary for the provenance dataset."""

from collections import defaultdict
from pathlib import Path
import sys


DEFAULT_DATA_RAW = (
    Path(__file__).resolve().parents[1]
    / "houssel.paul-provenance-graphs-dataset"
    / "data-raw"
)
EXPECTED_TOOLS = ["camflow", "conprov", "provbpf", "recap"]


def pct(part, total):
    return f"{(part / total * 100):.2f}%" if total else "0.00%"


def parse_name(path):
    stem = path.stem
    tool = stem.rsplit("_", 1)[-1]
    scenario = stem[: -(len(tool) + 1)]
    kind = "benign" if "_benign_" in scenario else "cve"
    return scenario, tool, kind


def main():
    data_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_RAW
    files = sorted(data_raw.glob("*.jsonl"))

    if not files:
        raise SystemExit(f"No JSONL files found in {data_raw}")

    by_tool = defaultdict(list)
    by_kind = defaultdict(list)
    coverage = defaultdict(dict)

    for path in files:
        scenario, tool, kind = parse_name(path)
        size = path.stat().st_size
        by_tool[tool].append(path)
        by_kind[kind].append(path)
        coverage[scenario][tool] = size

    total_size = sum(path.stat().st_size for path in files)

    print("COVERAGE SUMMARY")
    print(f"data_raw={data_raw}")
    print(f"files={len(files)}")
    print(f"scenarios={len(coverage)}")
    print(f"total_size_gib={total_size / 1024 / 1024 / 1024:.2f}")

    print("\nFILES BY TOOL")
    for tool in sorted(by_tool):
        paths = by_tool[tool]
        size = sum(path.stat().st_size for path in paths)
        print(
            f"  {tool}: files={len(paths)} ({pct(len(paths), len(files))}) "
            f"size_gib={size / 1024 / 1024 / 1024:.2f} ({pct(size, total_size)})"
        )

    print("\nFILES BY KIND")
    for kind in sorted(by_kind):
        paths = by_kind[kind]
        size = sum(path.stat().st_size for path in paths)
        print(
            f"  {kind}: files={len(paths)} ({pct(len(paths), len(files))}) "
            f"size_gib={size / 1024 / 1024 / 1024:.2f} ({pct(size, total_size)})"
        )

    print("\nCOVERAGE ANOMALIES")
    found_anomaly = False
    for scenario in sorted(coverage):
        present = sorted(coverage[scenario])
        missing = [tool for tool in EXPECTED_TOOLS if tool not in coverage[scenario]]
        extra = [tool for tool in present if tool not in EXPECTED_TOOLS]
        if missing or extra:
            found_anomaly = True
            print(
                f"  {scenario}: present={present} "
                f"missing={missing or []} extra={extra or []}"
            )
    if not found_anomaly:
        print("  none")

    print("\nSCENARIO TABLE")
    for scenario in sorted(coverage):
        parts = []
        for tool in sorted(coverage[scenario]):
            size_mb = coverage[scenario][tool] / 1024 / 1024
            parts.append(f"{tool}:{size_mb:.1f}MB")
        print(f"  {scenario}: " + ", ".join(parts))


if __name__ == "__main__":
    main()
