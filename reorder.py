#!/usr/bin/env python3

import argparse
import csv
import math
import sys


LOCATIONS = [
    {
        "label": "Main Outlet",
        "inventory_col": "inventory_Main_Outlet",
        "reorder_point_col": "reorder_point_Main_Outlet",
        "reorder_qty_col": "restock_level_Main_Outlet",
    },
    {
        "label": "Market Square",
        "inventory_col": "inventory_Market_Square",
        "reorder_point_col": "reorder_point_Market_Square",
        "reorder_qty_col": "restock_level_Market_Square",
    },
]


def parse_number(value, default=0.0):
    if value is None:
        return default

    value = str(value).strip()
    if value == "":
        return default

    try:
        return float(value)
    except ValueError:
        return default


def format_qty(value):
    if value == int(value):
        return str(int(value))

    return str(round(value, 2))


def is_active(row):
    return str(row.get("active", "")).strip().lower() in {
        "1",
        "1.0",
        "true",
        "yes",
    }


def tracks_inventory(row):
    return str(row.get("track_inventory", "")).strip().lower() in {
        "1",
        "1.0",
        "true",
        "yes",
    }


def supplier_matches(row, supplier_filter):
    if not supplier_filter:
        return True

    supplier_name = row.get("supplier_name", "") or ""
    supplier_code = row.get("supplier_code", "") or ""
    brand_name = row.get("brand_name", "") or ""

    needle = supplier_filter.lower().strip()

    return (
        needle in supplier_name.lower()
        or needle in supplier_code.lower()
        or needle in brand_name.lower()
    )


def make_dot_leader(left_text, right_text, total_width):
    left_text = str(left_text)
    right_text = str(right_text)

    dot_count = total_width - len(left_text) - len(right_text)

    if dot_count < 2:
        dot_count = 2

    return f"{left_text}{'.' * dot_count}{right_text}"


def load_reorder_rows(csv_path, supplier_filter=None, include_inactive=False):
    aggregated = {}

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        required_cols = {
            "sku",
            "name",
            "supplier_name",
            "supplier_code",
            "brand_name",
            "active",
            "track_inventory",
        }

        for loc in LOCATIONS:
            required_cols.add(loc["inventory_col"])
            required_cols.add(loc["reorder_point_col"])
            required_cols.add(loc["reorder_qty_col"])

        missing_cols = sorted(col for col in required_cols if col not in reader.fieldnames)
        if missing_cols:
            raise ValueError(
                "CSV is missing required columns: " + ", ".join(missing_cols)
            )

        for row in reader:
            sku = (row.get("sku") or "").strip()
            name = (row.get("name") or "").strip()

            if not sku or not name:
                continue

            if not include_inactive and not is_active(row):
                continue

            if not tracks_inventory(row):
                continue

            if not supplier_matches(row, supplier_filter):
                continue

            total_reorder_qty = 0

            for loc in LOCATIONS:
                inventory = parse_number(row.get(loc["inventory_col"]))
                reorder_point = parse_number(row.get(loc["reorder_point_col"]))
                reorder_qty = parse_number(row.get(loc["reorder_qty_col"]))

                if reorder_point <= 0:
                    continue

                if reorder_qty <= 0:
                    continue

                if inventory < reorder_point:
                    total_reorder_qty += math.ceil(reorder_qty)

            if total_reorder_qty <= 0:
                continue

            if sku not in aggregated:
                aggregated[sku] = {
                    "sku": sku,
                    "name": name,
                    "supplier_name": (row.get("supplier_name") or "").strip(),
                    "reorder_qty": 0,
                }

            aggregated[sku]["reorder_qty"] += total_reorder_qty

    return list(aggregated.values())


def print_results(results):
    if not results:
        print("No products are below reorder point.")
        return

    sku_width = max(len(item["sku"]) for item in results)
    name_width = max(len(item["name"]) for item in results)
    qty_width = max(len(format_qty(item["reorder_qty"])) for item in results)

    sku_to_name_width = sku_width + 6
    name_to_qty_width = name_width + qty_width + 8

    for item in results:
        sku = item["sku"]
        name = item["name"]
        qty = format_qty(item["reorder_qty"])

        left_section = make_dot_leader(
            left_text=sku,
            right_text=name,
            total_width=sku_to_name_width + name_width,
        )

        full_line = make_dot_leader(
            left_text=left_section,
            right_text=qty,
            total_width=sku_to_name_width + name_to_qty_width + name_width,
        )

        print(full_line)


def main():
    parser = argparse.ArgumentParser(
        description="Show aggregate reorder quantities across Main Outlet and Market Square."
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="Path to Lightspeed product CSV export.",
    )

    parser.add_argument(
        "--supplier",
        help="Optional supplier/brand filter. Partial match is okay. Case-insensitive.",
    )

    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive products. By default inactive products are skipped.",
    )

    args = parser.parse_args()

    try:
        results = load_reorder_rows(
            csv_path=args.csv,
            supplier_filter=args.supplier,
            include_inactive=args.include_inactive,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    results.sort(
        key=lambda x: (
            x["supplier_name"].lower(),
            x["name"].lower(),
            x["sku"],
        )
    )

    print_results(results)


if __name__ == "__main__":
    main()