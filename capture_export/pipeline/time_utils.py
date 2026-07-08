"""Small timestamp helpers for Orange provenance records."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


NS_PER_SECOND = 1_000_000_000


def decimal_seconds_to_ns(value: object) -> int:
    return int(Decimal(str(value)) * Decimal(NS_PER_SECOND))


def split_fractional_timestamp(timestamp_text: str) -> tuple[str, int]:
    if "." not in timestamp_text:
        return timestamp_text, 0

    base_text, fractional_text = timestamp_text.split(".", 1)
    fractional_digits = ""
    for char in fractional_text:
        if not char.isdigit():
            break
        fractional_digits += char

    if not fractional_digits:
        return base_text, 0

    return base_text, int(fractional_digits[:9].ljust(9, "0"))


def timestamp_precision_ns(timestamp: object) -> int:
    timestamp_text = str(timestamp).strip().rstrip("Z")
    if "." not in timestamp_text:
        return NS_PER_SECOND

    fractional_text = timestamp_text.split(".", 1)[1]
    fractional_digits = ""
    for char in fractional_text:
        if not char.isdigit():
            break
        fractional_digits += char

    if not fractional_digits:
        return NS_PER_SECOND
    return 10 ** max(0, 9 - min(len(fractional_digits), 9))


def datetime_text_to_ns(timestamp: object) -> int:
    timestamp_text = str(timestamp).strip().rstrip("Z")
    base_text, fractional_ns = split_fractional_timestamp(timestamp_text)

    for timestamp_format in ("%Y:%m:%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(base_text, timestamp_format)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Unsupported timestamp format: {timestamp}")

    return int(dt.replace(tzinfo=timezone.utc).timestamp()) * NS_PER_SECOND + fractional_ns


def parse_int(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def build_event_timestamp(tool: str, annotations: dict[str, Any]) -> dict[str, Any]:
    if tool == "recap":
        raw_timestamp = annotations.get("ts")
        if raw_timestamp is None:
            raw_timestamp = annotations.get("cf:date")
        wall_time_ns = datetime_text_to_ns(raw_timestamp)
        return {
            "wall_time_ns": wall_time_ns,
            "wall_time_precision_ns": timestamp_precision_ns(raw_timestamp),
            "model_time": wall_time_ns,
            "model_time_unit": "ns",
            "order": 0,
            "source": "ts",
            "raw": raw_timestamp,
        }

    raw_timestamp = annotations.get("cf:date")
    if raw_timestamp is None:
        raw_timestamp = annotations.get("ts")

    wall_time_ns = datetime_text_to_ns(raw_timestamp)
    timestamp = {
        "wall_time_ns": wall_time_ns,
        "wall_time_precision_ns": timestamp_precision_ns(raw_timestamp),
        "model_time": wall_time_ns // NS_PER_SECOND,
        "model_time_unit": "seconds",
        "order": 0,
        "source": "cf:date",
        "raw": raw_timestamp,
    }

    if tool == "camflow":
        jiffies = parse_int(annotations.get("cf:jiffies") or annotations.get("jiffies"))
        if jiffies and jiffies > 0:
            timestamp["model_time"] = jiffies
            timestamp["model_time_unit"] = "jiffies"
            timestamp["order"] = jiffies
            timestamp["source"] = "cf:date+cf:jiffies"
            timestamp["jiffies"] = jiffies

    return timestamp


def timestamp_wall_time_ns(timestamp: dict[str, Any]) -> int:
    return int(timestamp["wall_time_ns"])


def timestamp_model_time(timestamp: dict[str, Any]) -> int:
    return int(timestamp["model_time"])


def timestamp_event_interval_ns(timestamp: dict[str, Any]) -> tuple[int, int]:
    start_ns = timestamp_wall_time_ns(timestamp)
    precision_ns = max(1, int(timestamp.get("wall_time_precision_ns", 1)))
    return start_ns, start_ns + precision_ns - 1


def timestamp_sort_key(timestamp: dict[str, Any], fallback: object = "") -> tuple[int, int, str]:
    return timestamp_wall_time_ns(timestamp), int(timestamp.get("order", 0)), str(fallback)


def ns_to_window_component(ns_value: int) -> str:
    seconds = ns_value // NS_PER_SECOND
    nanos = ns_value % NS_PER_SECOND
    dt = datetime.fromtimestamp(seconds, timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{nanos:09d}"


def ranges_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    return start_a <= end_b and end_a >= start_b
