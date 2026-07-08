"""Read one raw Orange JSONL file and split it into metadata, nodes, and edges."""

from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterator

from capture_export.pipeline.labels import load_label_rows, select_attack_windows
from capture_export.pipeline.logging_utils import log_message
from capture_export.pipeline.models import EdgeRecord, JsonDict, NodeRecord, ReferenceRun, RunMetadata
from capture_export.pipeline.time_utils import build_event_timestamp


KNOWN_TOOLS = ("camflow", "conprov", "provbpf", "recap")

orjson = None
if importlib.util.find_spec("orjson") is not None:
    orjson = importlib.import_module("orjson")


def parse_name(path: Path) -> tuple[str, str, str]:
    stem = path.stem
    base_stem = stem
    if "_run_" in stem:
        possible_base, possible_run = stem.rsplit("_run_", 1)
        if possible_run.isdigit():
            base_stem = possible_base

    for tool in KNOWN_TOOLS:
        tool_suffix = f"_{tool}"
        if base_stem.endswith(tool_suffix):
            scenario = base_stem[: -len(tool_suffix)]
            break
    else:
        raise ValueError(f"Could not infer known Orange tool from file name: {path.name}")

    kind = "benign" if "_benign_" in scenario else "cve"
    return scenario, tool, kind


def extract_run_metadata(jsonl_path: str | Path) -> RunMetadata:
    path = Path(jsonl_path)
    scenario, tool, kind = parse_name(path)
    return RunMetadata(
        run_id=path.stem,
        tool=tool,
        scenario=scenario,
        kind=kind,
        source_file=str(path.resolve()),
        file_name=path.name,
    )


def load_json_line(line: bytes) -> JsonDict:
    try:
        if orjson is not None:
            record = orjson.loads(line)
        else:
            record = json.loads(line)
    except Exception:
        # Some raw files contain rare non-UTF-8 bytes inside string fields.
        # We first try a byte-preserving latin-1 decode, then a targeted quote repair
        # for rare payload strings that break JSON quoting inside path-like fields.
        text = line.decode("latin-1")
        try:
            record = json.loads(text)
        except json.JSONDecodeError:
            record = repair_json_by_escaping_premature_quotes(text)

    if not isinstance(record, dict):
        raise ValueError("Expected each JSONL line to decode to a JSON object.")

    return record


def repair_json_by_escaping_premature_quotes(text: str, max_repairs: int = 8) -> JsonDict:
    repaired = text

    for _ in range(max_repairs):
        try:
            record = json.loads(repaired)
            if isinstance(record, dict):
                return record
            break
        except json.JSONDecodeError as error:
            if "Expecting" not in error.msg or "delimiter" not in error.msg:
                raise

            candidate_pos = find_quote_to_escape(repaired, error.pos)
            if candidate_pos is None:
                raise

            repaired = repaired[:candidate_pos] + '\\"' + repaired[candidate_pos + 1 :]

    raise ValueError("Failed to repair malformed JSON string.")


def find_quote_to_escape(text: str, error_pos: int) -> int | None:
    scan = error_pos - 1
    while scan >= 0:
        if text[scan] == '"' and (scan == 0 or text[scan - 1] != "\\"):
            return scan
        scan -= 1
    return None


def normalize_annotations(record: JsonDict) -> JsonDict:
    annotations = record.get("annotations")
    if isinstance(annotations, dict):
        return dict(annotations)
    return {}


def get_timestamp(tool: str, annotations: JsonDict) -> JsonDict:
    return build_event_timestamp(tool, annotations)


def iter_jsonl(jsonl_path: str | Path) -> Iterator[tuple[int, JsonDict]]:
    path = Path(jsonl_path)
    fallback_count = 0
    skipped_count = 0

    with path.open("rb") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = load_json_line(line)
            except Exception as error:
                skipped_count += 1
                log_message(
                    f"[parse_raw_run] Skipping unreadable line {line_number} in {path.name}: {error}"
                )
                continue

            if any(byte_value >= 128 for byte_value in line):
                fallback_count += 1

            yield line_number, record

    if fallback_count:
        log_message(f"[parse_raw_run] {path.name}: used latin-1 fallback on {fallback_count} line(s)")
    if skipped_count:
        log_message(f"[parse_raw_run] {path.name}: skipped {skipped_count} unreadable line(s)")


def is_edge_record(record: JsonDict) -> bool:
    return "from" in record and "to" in record


def is_node_record(record: JsonDict) -> bool:
    return "id" in record and not is_edge_record(record)


def parse_node_record(record: JsonDict, line_number: int) -> NodeRecord:
    annotations = normalize_annotations(record)
    return NodeRecord(
        raw_node_id=str(record.get("id")),
        prov_type=str(record.get("type")) if record.get("type") is not None else None,
        line_number=line_number,
        attributes=annotations,
        raw=record,
    )


def parse_edge_record(record: JsonDict, line_number: int, tool: str) -> EdgeRecord:
    annotations = normalize_annotations(record)
    return EdgeRecord(
        raw_edge_id=str(annotations.get("id")) if annotations.get("id") is not None else None,
        src_raw_node_id=str(record.get("from")),
        dst_raw_node_id=str(record.get("to")),
        prov_type=str(record.get("type")) if record.get("type") is not None else None,
        line_number=line_number,
        timestamp=get_timestamp(tool, annotations),
        attributes=annotations,
        raw=record,
    )


def parse_raw_run(jsonl_path: str | Path) -> ReferenceRun:
    metadata = extract_run_metadata(jsonl_path)
    nodes: list[NodeRecord] = []
    edges: list[EdgeRecord] = []

    for line_number, record in iter_jsonl(jsonl_path):
        if is_node_record(record):
            nodes.append(parse_node_record(record, line_number))
        elif is_edge_record(record):
            edges.append(parse_edge_record(record, line_number, metadata.tool))
        else:
            raise ValueError(
                f"Unsupported record shape at line {line_number} in {metadata.file_name}."
            )

    label_rows = load_label_rows(Path(jsonl_path).parent)
    metadata.attack_windows = select_attack_windows(metadata, edges, label_rows)

    return ReferenceRun(metadata=metadata, nodes=nodes, edges=edges)
