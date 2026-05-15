#!/usr/bin/env python3
# path: add-bbuds.py

from __future__ import annotations

import argparse
import csv
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Set


REQUIRED_COLUMNS = [
    "id",
    "handle",
    "sku",
    "composite_name",
    "composite_sku",
    "composite_quantity",
    "name",
    "description",
    "product_category",
    "variant_option_one_name",
    "variant_option_one_value",
    "variant_option_two_name",
    "variant_option_two_value",
    "variant_option_three_name",
    "variant_option_three_value",
    "tags",
    "supply_price",
    "retail_price",
    "account_code",
    "account_code_purchase",
    "brand_name",
    "supplier_name",
    "supplier_code",
    "active",
    "track_inventory",
    "outlet_tax_Main_Outlet",
    "inventory_Main_Outlet",
    "reorder_point_Main_Outlet",
    "restock_level_Main_Outlet",
]


# B-buds use 1/2 oz as the base inventory unit.
#
# This intentionally avoids normal flower eighth/quarter rows so your other
# scripts that depend on eighth/quarter product patterns do not accidentally
# pick these up.
BBUD_SIZE_INFO = {
    "half_ounce": {
        "display_name": "1/2 oz",
        "handle_suffix": "half-ounce",
        "sku_suffix": "HO",
        "name_suffix": "(1/2 oz)",
        "category": "Flower / B-Buds / Half Ounce",
        "retail_price": "35",
        "composite_quantity": "1",
    },
    "ounce": {
        "display_name": "1 oz",
        "handle_suffix": "ounce",
        "sku_suffix": "OZ",
        "name_suffix": "(1 oz)",
        "category": "Flower / B-Buds / Ounce",
        "retail_price": "70",
        "composite_quantity": "2",
    },
    "quarter_pound": {
        "display_name": "1/4 lb",
        "handle_suffix": "quarter-pound",
        "sku_suffix": "QP",
        "name_suffix": "(1/4 lb)",
        "category": "Flower / B-Buds / Quarter Pound",
        "retail_price": "250",
        "composite_quantity": "8",
    },
    "pound": {
        "display_name": "1 lb",
        "handle_suffix": "pound",
        "sku_suffix": "LB",
        "name_suffix": "(1 lb)",
        "category": "Flower / B-Buds / Pound",
        "retail_price": "900",
        "composite_quantity": "32",
    },
}


DEFAULT_TAX = "Default Tax"
DEFAULT_BRAND = "Various"
DEFAULT_PRODUCT_LINE = "B-Buds"


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[’']", "", value)
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def encode_spaces(value: str) -> str:
    return value.replace(" ", "%20")


def extract_lot_from_filename(filename: str) -> str:
    match = re.match(r"^([A-Za-z0-9.]+)\s+-\s+", filename)
    return match.group(1) if match else ""


def sanitize_for_sku(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", value).upper()


def format_money(value: Decimal) -> str:
    value = value.quantize(Decimal("0.01"))
    return f"{value:.2f}"


def make_unique_handle(base_handle: str, existing_handles: Set[str]) -> str:
    if base_handle not in existing_handles:
        existing_handles.add(base_handle)
        return base_handle

    suffix = 2
    while True:
        candidate = f"{base_handle}-{suffix}"
        if candidate not in existing_handles:
            existing_handles.add(candidate)
            return candidate
        suffix += 1


def make_unique_sku(base_sku: str, existing_skus: Set[str]) -> str:
    if base_sku not in existing_skus:
        existing_skus.add(base_sku)
        return base_sku

    suffix = 2
    while True:
        candidate = f"{base_sku}{suffix}"
        if candidate not in existing_skus:
            existing_skus.add(candidate)
            return candidate
        suffix += 1


def build_sku_family(product_name: str, existing_skus: Set[str]) -> Dict[str, str]:
    seed = sanitize_for_sku(product_name)
    if not seed:
        raise ValueError("Could not generate a SKU seed from the product name.")

    # Example:
    # Halle Berry -> BB-HALL-B, BB-HALL-HO, BB-HALL-OZ, BB-HALL-QP, BB-HALL-LB
    # Punch Breath -> BB-PUNC-B, BB-PUNC-HO, BB-PUNC-OZ, BB-PUNC-QP, BB-PUNC-LB
    base_seed = (seed + "XXXX")[:4]

    return {
        "base": make_unique_sku(f"BB-{base_seed}-B", existing_skus),
        "half_ounce": make_unique_sku(f"BB-{base_seed}-HO", existing_skus),
        "ounce": make_unique_sku(f"BB-{base_seed}-OZ", existing_skus),
        "quarter_pound": make_unique_sku(f"BB-{base_seed}-QP", existing_skus),
        "pound": make_unique_sku(f"BB-{base_seed}-LB", existing_skus),
    }


def build_tags(thc: str, coa_filename: str, coa_lot: str = "") -> str:
    tags = [
        "sellable_composite=1",
        "usecoa=1",
        "netwt=14g",
    ]

    normalized_thc = thc.strip().removesuffix("%").strip()

    if coa_filename.strip():
        encoded_file = encode_spaces(coa_filename.strip())
        tags.append(f"coa_ref_0_file={encoded_file}")
        tags.append(f"coa_ref_0_url=/coas/flower/{encoded_file}")
        if normalized_thc:
            tags.append(f"coa_ref_0_thc={normalized_thc}")

        lot = coa_lot.strip() if coa_lot.strip() else extract_lot_from_filename(coa_filename.strip())
        if lot:
            tags.append(f"coa_ref_0_lot={lot}")
    elif normalized_thc:
        # Keep a fallback for data entry without a COA file. Normal COA-backed
        # rows should prefer coa_ref_N_thc so the THC maps to a specific COA.
        tags.append(f"thc={normalized_thc}")

    return ";".join(tags)


def read_csv(path: Path) -> tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")

        fieldnames = list(reader.fieldnames)
        rows: List[Dict[str, str]] = []

        for row in reader:
            normalized = {}
            for field in fieldnames:
                value = row.get(field, "")
                normalized[field] = value if value is not None else ""
            rows.append(normalized)

    return rows, fieldnames


def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def ensure_required_columns(fieldnames: List[str]) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
    if missing:
        raise ValueError(f"Source CSV is missing required columns: {', '.join(missing)}")


def collect_existing(rows: List[Dict[str, str]], column: str) -> Set[str]:
    return {
        (row.get(column, "") or "").strip()
        for row in rows
        if (row.get(column, "") or "").strip()
    }


def blank_row(fieldnames: List[str]) -> Dict[str, str]:
    return {field: "" for field in fieldnames}


def parse_optional_decimal(value: str, field_name: str) -> Decimal | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        raise argparse.ArgumentTypeError(
            f"Invalid {field_name} '{value}'. Expected something like 10.50"
        )


def build_product_rows(
    fieldnames: List[str],
    product_name: str,
    inventory_half_ounces: str,
    thc: str,
    coa_filename: str,
    coa_lot: str,
    supply_price_half_ounce: Decimal | None,
    existing_handles: Set[str],
    existing_skus: Set[str],
) -> List[Dict[str, str]]:
    slug = slugify(product_name)
    if not slug:
        raise ValueError("Could not generate a valid handle from the product name.")

    skus = build_sku_family(product_name, existing_skus)

    base_handle = make_unique_handle(f"base-{slug}-bbuds", existing_handles)
    base_name = f"BASE – BASE – {product_name} B-Buds (1/2 oz)"
    description = f"<p>{product_name} B-Buds Flower</p>"

    tags = build_tags(
        thc=thc,
        coa_filename=coa_filename,
        coa_lot=coa_lot,
    )

    rows: List[Dict[str, str]] = []

    # 1) Base inventory row.
    # This is the only row that should carry the source inventory count.
    #
    # Inventory is counted in half-ounce units:
    #   1 lb   = 32 half-ounce units
    #   1/2 lb = 16 half-ounce units
    #   1/4 lb = 8 half-ounce units
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": base_handle,
        "sku": skus["base"],
        "composite_name": "",
        "composite_sku": "",
        "composite_quantity": "",
        "name": base_name,
        "description": description,
        "product_category": "Flower / B-Buds / Base",
        "tags": "",
        "supply_price": format_money(supply_price_half_ounce) if supply_price_half_ounce is not None else "",
        "retail_price": BBUD_SIZE_INFO["half_ounce"]["retail_price"],
        "brand_name": DEFAULT_BRAND,
        "supplier_name": "",
        "supplier_code": "",
        "active": "1",
        "track_inventory": "1",
        "outlet_tax_Main_Outlet": DEFAULT_TAX,
        "inventory_Main_Outlet": inventory_half_ounces,
        "reorder_point_Main_Outlet": "",
        "restock_level_Main_Outlet": "",
    })
    rows.append(row)

    # 2 through 9) Sellable rows plus matching component rows.
    for size_key in ["half_ounce", "ounce", "quarter_pound", "pound"]:
        size = BBUD_SIZE_INFO[size_key]

        sellable_name = f"{product_name} B-Buds {size['name_suffix']}"
        sellable_handle = make_unique_handle(
            f"{slug}-bbuds-{size['handle_suffix']}",
            existing_handles,
        )

        composite_quantity = Decimal(size["composite_quantity"])
        supply_price = ""
        if supply_price_half_ounce is not None:
            supply_price = format_money(supply_price_half_ounce * composite_quantity)

        # Sellable product row.
        row = blank_row(fieldnames)
        row.update({
            "id": "",
            "handle": sellable_handle,
            "sku": skus[size_key],
            "composite_name": "",
            "composite_sku": "",
            "composite_quantity": "",
            "name": sellable_name,
            "description": description,
            "product_category": size["category"],
            "tags": tags,
            "supply_price": supply_price,
            "retail_price": size["retail_price"],
            "brand_name": DEFAULT_BRAND,
            "supplier_name": "",
            "supplier_code": "",
            "active": "1",
            "track_inventory": "0",
            "outlet_tax_Main_Outlet": DEFAULT_TAX,
            "inventory_Main_Outlet": "",
            "reorder_point_Main_Outlet": "",
            "restock_level_Main_Outlet": "",
        })
        rows.append(row)

        # Component row.
        # This makes the sellable product consume from the base B-buds inventory item.
        row = blank_row(fieldnames)
        row.update({
            "id": "",
            "handle": "",
            "sku": skus[size_key],
            "composite_name": base_name,
            "composite_sku": skus["base"],
            "composite_quantity": size["composite_quantity"],
            "name": sellable_name,
        })
        rows.append(row)

    return rows


def validate_inventory_half_ounces(value: str) -> str:
    cleaned = value.strip()
    if not re.fullmatch(r"\d+", cleaned):
        raise argparse.ArgumentTypeError(
            f"Invalid inventory '{value}'. Expected a whole number of half-ounce units."
        )

    if int(cleaned) < 0:
        raise argparse.ArgumentTypeError("Inventory cannot be negative.")

    return cleaned


def validate_thc(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""

    if not re.fullmatch(r"\d+(\.\d+)?", cleaned):
        raise argparse.ArgumentTypeError(
            f"Invalid THC content '{value}'. Expected something like 22.430"
        )

    return cleaned


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add a B-buds product family to a Lightspeed CSV export."
    )

    parser.add_argument(
        "--name",
        required=True,
        help='Strain name, e.g. "Halle Berry"',
    )
    parser.add_argument(
        "--inventory-half-ounces",
        required=True,
        default="32",
        type=validate_inventory_half_ounces,
        help="Inventory count in half-ounce units. Example: 32 for one pound.",
    )
    parser.add_argument(
        "--thc",
        default="",
        type=validate_thc,
        help='Optional THC content, e.g. "24.500"',
    )
    parser.add_argument(
        "--coa-file",
        default="",
        help="Optional exact COA file name.",
    )
    parser.add_argument(
        "--lot",
        default="",
        help="Optional explicit COA lot. If omitted, the script tries to extract it from the COA filename.",
    )
    parser.add_argument(
        "--supply-price-half-ounce",
        default="",
        help="Optional supply cost per half-ounce unit. Example: 20.00",
    )
    parser.add_argument(
        "--source-csv",
        required=True,
        help="Path to source CSV.",
    )
    parser.add_argument(
        "--output-csv",
        help="Optional output CSV path. If omitted, source CSV is overwritten.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_csv = Path(args.source_csv)
    output_csv = Path(args.output_csv) if args.output_csv else source_csv

    rows, fieldnames = read_csv(source_csv)
    ensure_required_columns(fieldnames)

    product_name = args.name.strip()
    inventory_half_ounces = args.inventory_half_ounces.strip()
    thc = args.thc.strip()
    coa_filename = args.coa_file.strip()
    coa_lot = args.lot.strip()
    supply_price_half_ounce = parse_optional_decimal(
        args.supply_price_half_ounce,
        "supply price per half ounce",
    )

    existing_names = collect_existing(rows, "name")
    canonical_half_ounce_name = f"{product_name} B-Buds (1/2 oz)"
    if canonical_half_ounce_name in existing_names:
        raise ValueError(f'Product already exists in CSV as "{canonical_half_ounce_name}".')

    existing_handles = collect_existing(rows, "handle")
    existing_skus = collect_existing(rows, "sku")

    new_rows = build_product_rows(
        fieldnames=fieldnames,
        product_name=product_name,
        inventory_half_ounces=inventory_half_ounces,
        thc=thc,
        coa_filename=coa_filename,
        coa_lot=coa_lot,
        supply_price_half_ounce=supply_price_half_ounce,
        existing_handles=existing_handles,
        existing_skus=existing_skus,
    )

    rows.extend(new_rows)
    write_csv(output_csv, rows, fieldnames)

    print("Added B-buds product rows successfully.")
    print(f"Product: {product_name} B-Buds")
    print(f"Inventory half-ounce units: {inventory_half_ounces}")
    print(f"Equivalent pounds: {Decimal(inventory_half_ounces) / Decimal('32')}")
    print(f"THC: {thc if thc else '(not set)'}")
    print(f"COA file: {coa_filename if coa_filename else '(not set)'}")
    print(f"COA lot: {coa_lot if coa_lot else '(auto from filename if possible)'}")
    print(f"Supply price per half ounce: {format_money(supply_price_half_ounce) if supply_price_half_ounce is not None else '(not set)'}")
    print(f"Rows added: {len(new_rows)}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()