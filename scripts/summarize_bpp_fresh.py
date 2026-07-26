#!/usr/bin/env python3
"""Summarize machine-readable workload results from fresh benchmark processes."""

from __future__ import annotations

import argparse
import math
from pathlib import Path


RESULT_KINDS = {
    "RUN_RESULT",
    "NEW_SERIES_RESULT",
    "QUERY_SATURATION_RESULT",
}
NON_NUMERIC_RESULT_KEYS = {"run", "resource_profile", "storage_path_kind"}
CONFIGURATION_KIND = "BPP_PRESET_CONFIGURATION"


def numeric_value(raw: str) -> int | float | None:
    if raw == "unavailable":
        return None
    try:
        return int(raw)
    except ValueError:
        try:
            value = float(raw)
            return value if math.isfinite(value) else None
        except ValueError:
            return None


def nearest_rank(values: list[int | float], percentile: float) -> int | float:
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def display(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return f"{value:.6f}"


def parse_fields(fields: list[str], record_kind: str) -> dict[str, str]:
    record = {}
    for field in fields:
        key, separator, value = field.partition("=")
        if not separator or not key or not value:
            raise SystemExit(f"malformed {record_kind} field: {field!r}")
        if key in record:
            raise SystemExit(f"duplicate {record_kind} field: {key!r}")
        record[key] = value
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_output", type=Path)
    parser.add_argument("--expected-runs", type=int, required=True)
    parser.add_argument("--expected-kind", choices=sorted(RESULT_KINDS))
    parser.add_argument(
        "--expected-profile", choices=["test", "edge", "embedded", "server"]
    )
    parser.add_argument("--require-configuration", action="store_true")
    parser.add_argument("--require-storage-path-kind", action="store_true")
    args = parser.parse_args()
    if args.expected_runs <= 0:
        parser.error("--expected-runs must be a positive integer")

    records: list[dict[str, str]] = []
    configurations: list[dict[str, str]] = []
    result_kind: str | None = None
    for line in args.raw_output.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if not fields:
            continue
        if fields[0] == CONFIGURATION_KIND:
            configurations.append(parse_fields(fields[1:], CONFIGURATION_KIND))
            continue
        if fields[0] not in RESULT_KINDS:
            continue
        if "ERROR" in fields:
            raise SystemExit(f"failed result row found: {line}")
        if result_kind is None:
            result_kind = fields[0]
        elif result_kind != fields[0]:
            raise SystemExit(
                f"mixed result kinds in {args.raw_output}: {result_kind} and {fields[0]}"
            )
        records.append(parse_fields(fields[1:], fields[0]))

    if len(records) != args.expected_runs:
        raise SystemExit(
            f"expected {args.expected_runs} successful result rows, found {len(records)}"
        )
    if result_kind != args.expected_kind and args.expected_kind is not None:
        raise SystemExit(
            f"expected result kind {args.expected_kind}, found {result_kind}"
        )
    run_ids = [record.get("run") for record in records]
    expected_run_ids = [str(run) for run in range(1, args.expected_runs + 1)]
    if run_ids != expected_run_ids:
        raise SystemExit(
            f"expected ordered result run identifiers {expected_run_ids!r}, found {run_ids!r}"
        )

    result_schema = set(records[0])
    for run_id, record in zip(run_ids, records, strict=True):
        if set(record) != result_schema:
            raise SystemExit(
                f"result schema changed in run {run_id}: "
                f"expected {sorted(result_schema)!r}, found {sorted(record)!r}"
            )

    if configurations and len(configurations) != args.expected_runs:
        raise SystemExit(
            f"expected {args.expected_runs} preset configuration rows, "
            f"found {len(configurations)}"
        )
    if args.require_configuration and not configurations:
        raise SystemExit("preset configuration rows are required")
    if configurations:
        configuration_schema = set(configurations[0])
        for index, configuration in enumerate(configurations, start=1):
            if set(configuration) != configuration_schema:
                raise SystemExit(
                    f"preset configuration schema changed in run {index}: "
                    f"expected {sorted(configuration_schema)!r}, "
                    f"found {sorted(configuration)!r}"
                )
            if configuration != configurations[0]:
                raise SystemExit(
                    f"preset configuration changed between runs 1 and {index}"
                )

    resource_profiles = {record.get("resource_profile") for record in records}
    if len(resource_profiles) != 1:
        raise SystemExit(f"result resource profiles are inconsistent: {resource_profiles!r}")
    resource_profile = next(iter(resource_profiles))
    if resource_profile is None:
        if args.expected_profile is not None:
            raise SystemExit("result resource profile is missing")
        resource_profile = "unreported"
    if (
        args.expected_profile is not None
        and resource_profile != args.expected_profile
    ):
        raise SystemExit(
            f"expected resource profile {args.expected_profile}, found {resource_profile}"
        )

    storage_path_kinds = {record.get("storage_path_kind") for record in records}
    if len(storage_path_kinds) != 1:
        raise SystemExit(
            f"result storage path kinds are inconsistent: {storage_path_kinds!r}"
        )
    storage_path_kind = next(iter(storage_path_kinds))
    if storage_path_kind is None:
        if args.require_storage_path_kind:
            raise SystemExit("result storage_path_kind is missing")
    elif storage_path_kind not in {"temporary", "configured"}:
        raise SystemExit(f"unknown storage_path_kind: {storage_path_kind!r}")

    non_numeric_result_keys = set(NON_NUMERIC_RESULT_KEYS)
    if storage_path_kind is None:
        # Pre-provenance logs emitted an absolute `path=` field instead of the stable
        # storage_path_kind classification. Keep those legacy rows summarizable without treating
        # a path as a numeric metric; current rows must never reintroduce it.
        non_numeric_result_keys.add("path")
    elif "path" in result_schema:
        raise SystemExit("current result rows must not contain a raw storage path")

    numeric_metrics: dict[str, list[int | float]] = {}
    unavailable_metrics: dict[str, int] = {}
    for key in sorted(result_schema - non_numeric_result_keys):
        values = [numeric_value(record[key]) for record in records]
        if all(value is not None for value in values):
            numeric_metrics[key] = [value for value in values if value is not None]
        elif all(
            value is not None or raw == "unavailable"
            for value, raw in zip(
                values, (record[key] for record in records), strict=True
            )
        ):
            unavailable_metrics[key] = sum(value is None for value in values)
        else:
            raw_values = [record[key] for record in records]
            raise SystemExit(f"metric {key!r} has non-numeric values: {raw_values!r}")

    print(
        f"FRESH_PROCESS_SUITE_RESULT kind={result_kind} "
        f"resource_profile={resource_profile} runs={len(records)} failures=0 "
        f"raw_output_name={args.raw_output.name}"
    )
    for key, values in numeric_metrics.items():
        print(
            f"FRESH_PROCESS_METRIC name={key} "
            f"p50={display(nearest_rank(values, 0.50))} "
            f"p95={display(nearest_rank(values, 0.95))} "
            f"min={display(min(values))} max={display(max(values))}"
        )
    for key, unavailable_runs in unavailable_metrics.items():
        print(
            f"FRESH_PROCESS_METRIC name={key} status=unavailable "
            f"available_runs={len(records) - unavailable_runs} "
            f"unavailable_runs={unavailable_runs}"
        )


if __name__ == "__main__":
    main()
