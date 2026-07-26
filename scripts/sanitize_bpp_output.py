#!/usr/bin/env python3
"""Redact local filesystem roots from streamed benchmark output."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable


def replacement_variants(path: str) -> set[str]:
    if not path:
        return set()
    return {
        candidate
        for candidate in (path, os.path.abspath(path), os.path.realpath(path))
        if candidate and candidate != os.path.sep
    }


def replacements(
    checkout_root: str, temporary_root: str, keep_roots: Iterable[str]
) -> list[tuple[str, str]]:
    by_path: dict[str, str] = {}
    for path in replacement_variants(checkout_root):
        by_path[path.rstrip(os.path.sep)] = "<checkout>"
    for path in replacement_variants(temporary_root):
        by_path.setdefault(path.rstrip(os.path.sep), "<temporary-root>")
    for keep_root in keep_roots:
        for path in replacement_variants(keep_root):
            by_path[path.rstrip(os.path.sep)] = "<configured-storage-root>"
    return sorted(by_path.items(), key=lambda item: len(item[0]), reverse=True)


def sanitize(text: str, configured_replacements: list[tuple[str, str]]) -> str:
    for raw_path, placeholder in configured_replacements:
        text = text.replace(raw_path, placeholder)
    return text


def self_test() -> None:
    configured = replacements(
        "/private/work/tsink",
        "/private/tmp/",
        ["/private/work/kept data"],
    )
    raw = (
        "Compiling tsink (/private/work/tsink)\n"
        "failed at /private/tmp/.tmp123/run-01/file\n"
        "kept at /private/work/kept data/fresh-test/run-02\n"
        "RUN_RESULT run=1 storage_path_kind=temporary\n"
    )
    expected = (
        "Compiling tsink (<checkout>)\n"
        "failed at <temporary-root>/.tmp123/run-01/file\n"
        "kept at <configured-storage-root>/fresh-test/run-02\n"
        "RUN_RESULT run=1 storage_path_kind=temporary\n"
    )
    actual = sanitize(raw, configured)
    if actual != expected:
        raise SystemExit(f"sanitizer self-test failed:\n{actual!r}\n!=\n{expected!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root")
    parser.add_argument("--temp-root")
    parser.add_argument("--keep-root", action="append", default=[])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.root or not args.temp_root:
        parser.error("--root and --temp-root are required unless --self-test is used")

    configured = replacements(args.root, args.temp_root, args.keep_root)
    for line in sys.stdin:
        sys.stdout.write(sanitize(line, configured))
        sys.stdout.flush()


if __name__ == "__main__":
    main()
