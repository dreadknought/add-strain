#!/usr/bin/env python3

import argparse
import csv
import math
import sys


MAIN = {
    "label": "Main Outlet",
    "inventory_col": "inventory_Main_Outlet",
    "reorder_point_col": "reorder_point_Main_Outlet",
    "reorder_qty_col": "restock_level_Main_Outlet",
}

MARKET = {
    "label": "Market Square",
    "inventory_col": "inventory_Market_Square",
    "reorder_point_col": "reorder_point_Market_Square",
    "reorder_qty_col": "restock_level_Market_Square",
}


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


def required_columns():
    cols = {
        "sku",
        "name",
        "supplier_name",
        "supplier_code",
        "brand_name",
        "active",
        "track_inventory",
        MAIN["inventory_col"],
        MAIN["reorder_point_col"],
        MARKET["inventory_col"],
        MARKET["reorder_point_col"],
        MARKET["reorder_qty_col"],
    }

    return cols


def load_transfer_rows(
    csv_path,
    supplier_filter=None,
    include_inactive=False,
    show_insufficient=False,
):
    results = []

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        missing_cols = sorted(col for col in required_columns() if col not in reader.fieldnames)
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

            main_inventory = parse_number(row.get(MAIN["inventory_col"]))
            main_reorder_point = parse_number(row.get(MAIN["reorder_point_col"]))

            market_inventory = parse_number(row.get(MARKET["inventory_col"]))
            market_reorder_point = parse_number(row.get(MARKET["reorder_point_col"]))
            market_reorder_qty = parse_number(row.get(MARKET["reorder_qty_col"]))

            # If Market Square has no reorder setup, skip it.
            if market_reorder_point <= 0:
                continue

            if market_reorder_qty <= 0:
                continue

            # Only care about products where Market Square is below reorder point.
            if market_inventory >= market_reorder_point:
                continue

            # How much Main can spare without dropping below its own reorder point.
            main_available_to_transfer = main_inventory - main_reorder_point

            if main_available_to_transfer < 0:
                main_available_to_transfer = 0

            transfer_qty = math.ceil(market_reorder_qty)
            can_transfer = main_available_to_transfer >= transfer_qty

            if not can_transfer and not show_insufficient:
                continue

            results.append(
                {
                    "sku": sku,
                    "name": name,
                    "supplier_name": (row.get("supplier_name") or "").strip(),
                    "transfer_qty": transfer_qty,
                    "main_inventory": main_inventory,
                    "main_reorder_point": main_reorder_point,
                    "market_inventory": market_inventory,
                    "market_reorder_point": market_reorder_point,
                    "main_available_to_transfer": math.floor(main_available_to_transfer),
                    "can_transfer": can_transfer,
                }
            )

    return results


def print_results(results, show_insufficient=False):
    if not results:
        print("No eligible Market Square transfers found.")
        return

    name_width = max(len(item["name"]) for item in results)
    qty_width = max(len(format_qty(item["transfer_qty"])) for item in results)

    if show_insufficient:
        status_width = max(len("OK"), len("INSUFFICIENT"))
    else:
        status_width = 0

    name_to_qty_width = name_width + qty_width + 8

    for item in results:
        name = item["name"]
        qty = format_qty(item["transfer_qty"])

        full_line = make_dot_leader(
            left_text=name,
            right_text=qty,
            total_width=name_to_qty_width,
        )

        if show_insufficient:
            status = "OK" if item["can_transfer"] else "INSUFFICIENT"
            full_line = make_dot_leader(
                left_text=full_line,
                right_text=status,
                total_width=len(full_line) + status_width + 6,
            )

        print(full_line)

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Show products that Market Square needs and Main Outlet can transfer "
            "without dropping Main below its reorder point."
        )
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

    parser.add_argument(
        "--show-insufficient",
        action="store_true",
        help=(
            "Also show Market Square items that are below reorder point but cannot "
            "be fully transferred from Main."
        ),
    )

    args = parser.parse_args()

    try:
        results = load_transfer_rows(
            csv_path=args.csv,
            supplier_filter=args.supplier,
            include_inactive=args.include_inactive,
            show_insufficient=args.show_insufficient,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    results.sort(
        key=lambda x: (
            not x["can_transfer"],
            x["supplier_name"].lower(),
            x["name"].lower(),
            x["sku"],
        )
    )

    print_results(
        results=results,
        show_insufficient=args.show_insufficient,
    )


if __name__ == "__main__":
    main()