#!/usr/bin/env python3
"""Audit the raw Orange JSONL files and report malformed lines."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import importlib
import importlib.util
import json
import sys
from typing import Any


DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_RAW = Path(__file__).resolve().parents[1] / "data-raw"
DEFAULT_SUMMARY_OUT = DEFAULT_REPO_ROOT / "artifacts" / "raw_audit_summary.json"
DEFAULT_DETAILS_OUT = DEFAULT_REPO_ROOT / "artifacts" / "raw_audit_details.json"

orjson = None
if importlib.util.find_spec("orjson") is not None:
    orjson = importlib.import_module("orjson")


def parse_name(path: Path) -> tuple[str, str, str]:
    stem = path.stem
    tool = stem.rsplit("_", 1)[-1]
    scenario = stem[: -(len(tool) + 1)]
    kind = "benign" if "_benign_" in scenario else "cve"
    return scenario, tool, kind


def strict_parse(line: bytes) -> dict[str, Any]:
    if orjson is not None:
        record = orjson.loads(line)
    else:
        record = json.loads(line)

    if not isinstance(record, dict):
        raise ValueError("JSON line did not decode to an object")
    return record


def latin1_parse(line: bytes) -> dict[str, Any]:
    record = json.loads(line.decode("latin-1"))
    if not isinstance(record, dict):
        raise ValueError("JSON line did not decode to an object")
    return record


def repair_by_escaping_error_quotes(text: str, max_repairs: int = 8) -> tuple[dict[str, Any] | None, list[int]]:
    repaired = text
    repaired_positions: list[int] = []

    for _ in range(max_repairs):
        try:
            record = json.loads(repaired)
            if isinstance(record, dict):
                return record, repaired_positions
            return None, repaired_positions
        except json.JSONDecodeError as error:
            if "Expecting" not in error.msg or "delimiter" not in error.msg:
                return None, repaired_positions

            candidate_pos = find_quote_to_escape(repaired, error.pos)
            if candidate_pos is None:
                return None, repaired_positions

            repaired = repaired[:candidate_pos] + '\\"' + repaired[candidate_pos + 1 :]
            repaired_positions.append(candidate_pos)

    return None, repaired_positions


def find_quote_to_escape(text: str, error_pos: int) -> int | None:
    scan = error_pos - 1
    while scan >= 0:
        if text[scan] == '"' and (scan == 0 or text[scan - 1] != "\\"):
            return scan
        scan -= 1
    return None


def classify_line(line: bytes) -> tuple[str, dict[str, Any] | None, dict[str, Any]]:
    try:
        return "strict_ok", strict_parse(line), {}
    except Exception as strict_error:
        strict_message = f"{type(strict_error).__name__}: {strict_error}"

    try:
        record = latin1_parse(line)
        return "repairable_latin1", record, {"strict_error": strict_message}
    except Exception as latin1_error:
        latin1_message = f"{type(latin1_error).__name__}: {latin1_error}"

    repaired_record, repaired_positions = repair_by_escaping_error_quotes(line.decode("latin-1"))
    if repaired_record is not None:
        return (
            "repairable_escape_quotes",
            repaired_record,
            {
                "strict_error": strict_message,
                "latin1_error": latin1_message,
                "repaired_positions": repaired_positions,
            },
        )

    return (
        "unrepaired",
        None,
        {
            "strict_error": strict_message,
            "latin1_error": latin1_message,
        },
    )


def audit_file(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    scenario, tool, kind = parse_name(path)
    counts = Counter()
    details: list[dict[str, Any]] = []

    with path.open("rb") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.rstrip(b"\n")
            if not line:
                continue

            status, _, extra = classify_line(line)
            counts[status] += 1

            if status != "strict_ok":
                details.append(
                    {
                        "file": path.name,
                        "scenario": scenario,
                        "tool": tool,
                        "kind": kind,
                        "line_number": line_number,
                        "status": status,
                        "excerpt_latin1": line.decode("latin-1", errors="replace")[:400],
                        **extra,
                    }
                )

    summary = {
        "file": path.name,
        "scenario": scenario,
        "tool": tool,
        "kind": kind,
        "strict_ok": counts["strict_ok"],
        "repairable_latin1": counts["repairable_latin1"],
        "repairable_escape_quotes": counts["repairable_escape_quotes"],
        "unrepaired": counts["unrepaired"],
        "total_problem_lines": counts["repairable_latin1"]
        + counts["repairable_escape_quotes"]
        + counts["unrepaired"],
    }
    return summary, details


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def main() -> None:
    data_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_RAW
    summary_out = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_SUMMARY_OUT
    details_out = Path(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_DETAILS_OUT

    files = sorted(data_raw.glob("*.jsonl"))
    if not files:
        raise SystemExit(f"No JSONL files found in {data_raw}")

    summaries: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    totals = Counter()

    for index, path in enumerate(files, start=1):
        file_summary, file_details = audit_file(path)
        summaries.append(file_summary)
        details.extend(file_details)

        totals["strict_ok"] += file_summary["strict_ok"]
        totals["repairable_latin1"] += file_summary["repairable_latin1"]
        totals["repairable_escape_quotes"] += file_summary["repairable_escape_quotes"]
        totals["unrepaired"] += file_summary["unrepaired"]

        print(
            f"audited {index}/{len(files)} {path.name} "
            f"problems={file_summary['total_problem_lines']}",
            flush=True,
        )

    summary_payload = {
        "data_raw": str(data_raw),
        "file_count": len(files),
        "strict_ok": totals["strict_ok"],
        "repairable_latin1": totals["repairable_latin1"],
        "repairable_escape_quotes": totals["repairable_escape_quotes"],
        "unrepaired": totals["unrepaired"],
        "files": summaries,
    }

    write_json(summary_out, summary_payload)
    write_json(details_out, details)
    print(f"summary_out={summary_out}")
    print(f"details_out={details_out}")


if __name__ == "__main__":
    main()
