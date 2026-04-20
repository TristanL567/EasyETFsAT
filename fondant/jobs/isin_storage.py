from __future__ import annotations

import csv
import re
from pathlib import Path

ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
DEFAULT_STORAGE_PATH = Path("Documentation") / "isin_storage.csv"


def normalize_isin(raw: str) -> str:
    return raw.strip().upper()


def is_valid_isin(value: str) -> bool:
    return bool(ISIN_PATTERN.match(value))


def ensure_storage_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow(["ISIN"])


def load_storage_isins(path: Path = DEFAULT_STORAGE_PATH) -> list[str]:
    ensure_storage_file(path)

    with path.open("r", newline="", encoding="utf-8") as file_handle:
        reader = csv.DictReader(file_handle)
        if not reader.fieldnames:
            return []
        if "ISIN" not in reader.fieldnames:
            first_field = reader.fieldnames[0]
        else:
            first_field = "ISIN"

        deduped: set[str] = set()
        for row in reader:
            raw_value = row.get(first_field, "")
            normalized = normalize_isin(raw_value)
            if normalized and is_valid_isin(normalized):
                deduped.add(normalized)

    return sorted(deduped)


def add_storage_isins(isins: list[str], path: Path = DEFAULT_STORAGE_PATH) -> tuple[list[str], list[str]]:
    existing = set(load_storage_isins(path))
    added: set[str] = set()
    invalid: list[str] = []

    for raw in isins:
        normalized = normalize_isin(raw)
        if not normalized:
            continue
        if not is_valid_isin(normalized):
            invalid.append(raw)
            continue
        if normalized not in existing:
            existing.add(normalized)
            added.add(normalized)

    ensure_storage_file(path)
    with path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.writer(file_handle)
        writer.writerow(["ISIN"])
        for isin in sorted(existing):
            writer.writerow([isin])

    return sorted(added), invalid

