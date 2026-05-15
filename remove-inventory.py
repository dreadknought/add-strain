#!/usr/bin/env python3
# path: remove_inventory_columns.py

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Dict


# Exact column names known to update inventory values in Lightspeed.
EXACT_INVENTORY_COLUMNS = {
    "track_inventory",
}


# Column prefixes that are outlet-specific in Lightspeed exports/imports.
# Examples:
# - inventory_Main_Outlet
# - reorder_point_Main_Outlet
# - restock_level_Main_Outlet
INVENTORY_COLUMN_PREFIXES = (
    "inventory_",
    "reorder_point_",
    "restock_level_",
)


def is_inventory_column(column_name: str) -> bool:
    """
    Return True if the column appears to control Lightspeed inventory behavior.

    This handles both exact column names like:
        track_inventory

    And outlet-specific columns like:
        inventory_Main_Outlet
        reorder_point_Main_Outlet
        restock_level_Main_Outlet
    """
    normalized = column_name.strip()

    if normalized in EXACT_INVENTORY_COLUMNS:
        return True

    return normalized.startswith(INVENTORY_COLUMN_PREFIXES)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove Lightspeed inventory-related columns from a CSV."
    )

    parser.add_argument(
        "input_csv",
        help="Path to the source CSV file.",
    )

    parser.add_argument(
        "output_csv",
        help="Path where the cleaned CSV should be written.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which columns would be removed without writing an output file.",
    )

    return parser.parse_args()


def read_csv(path: Path) -> tuple[List[Dict[str, str]], List[str]]:
    """
    Read a CSV into rows and preserve the original header order.
    """
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")

        fieldnames = list(reader.fieldnames)
        rows: List[Dict[str, str]] = []

        for row in reader:
            normalized_row: Dict[str, str] = {}

            for field in fieldnames:
                value = row.get(field, "")
                normalized_row[field] = value if value is not None else ""

            rows.append(normalized_row)

    return rows, fieldnames


def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    """
    Write rows using only the cleaned fieldnames.

    extrasaction='ignore' is intentional. The row dictionaries may still contain
    removed inventory fields, but they will not be written to the output CSV.
    """
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()

    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)

    rows, original_fieldnames = read_csv(input_csv)

    removed_columns = [
        field for field in original_fieldnames if is_inventory_column(field)
    ]

    cleaned_fieldnames = [
        field for field in original_fieldnames if not is_inventory_column(field)
    ]

    print("Inventory-related columns found:")
    if removed_columns:
        for column in removed_columns:
            print(f"  - {column}")
    else:
        print("  None")

    print()
    print(f"Original column count: {len(original_fieldnames)}")
    print(f"Cleaned column count:  {len(cleaned_fieldnames)}")

    if args.dry_run:
        print()
        print("Dry run only. No output file written.")
        return

    write_csv(output_csv, rows, cleaned_fieldnames)

    print()
    print(f"Cleaned CSV written to: {output_csv}")


if __name__ == "__main__":
    main()