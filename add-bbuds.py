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
    "id", "handle", "sku", "composite_name", "composite_sku",
    "composite_quantity", "name", "description", "product_category",
    "variant_option_one_name", "variant_option_one_value",
    "variant_option_two_name", "variant_option_two_value",
    "variant_option_three_name", "variant_option_three_value", "tags",
    "supply_price", "retail_price", "account_code",
    "account_code_purchase", "brand_name", "supplier_name",
    "supplier_code", "active", "outlet_tax_Main_Outlet",
]

OPTIONAL_COLUMNS = [
    "track_inventory", "inventory_Main_Outlet", "reorder_point_Main_Outlet",
    "restock_level_Main_Outlet", "inventory_Market_Square",
    "reorder_point_Market_Square", "restock_level_Market_Square",
]

BBUD_SIZE_INFO = {
    "half_ounce": {
        "display_name": "1/2 oz", "handle_suffix": "half-ounce",
        "sku_suffix": "HO", "name_suffix": "(1/2 oz)",
        "category": "Flower / B-Buds / Half Ounce", "retail_price": "40",
        "composite_quantity": "1",
    },
    "ounce": {
        "display_name": "1 oz", "handle_suffix": "ounce",
        "sku_suffix": "OZ", "name_suffix": "(1 oz)",
        "category": "Flower / B-Buds / Ounce", "retail_price": "80",
        "composite_quantity": "2",
    },
    "quarter_pound": {
        "display_name": "1/4 lb", "handle_suffix": "quarter-pound",
        "sku_suffix": "QP", "name_suffix": "(1/4 lb)",
        "category": "Flower / B-Buds / Quarter Pound", "retail_price": "250",
        "composite_quantity": "8",
    },
    "pound": {
        "display_name": "1 lb", "handle_suffix": "pound",
        "sku_suffix": "LB", "name_suffix": "(1 lb)",
        "category": "Flower / B-Buds / Pound", "retail_price": "900",
        "composite_quantity": "32",
    },
}

DEFAULT_TAX = "Default Tax"
DEFAULT_BRAND = "Various"


def slugify(value: str) -> str:
    value = value.strip().lower().replace("&", " and ")
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
    base_seed = (seed + "XXXX")[:4]
    return {
        "base": make_unique_sku(f"BB-{base_seed}-B", existing_skus),
        "half_ounce": make_unique_sku(f"BB-{base_seed}-HO", existing_skus),
        "ounce": make_unique_sku(f"BB-{base_seed}-OZ", existing_skus),
        "quarter_pound": make_unique_sku(f"BB-{base_seed}-QP", existing_skus),
        "pound": make_unique_sku(f"BB-{base_seed}-LB", existing_skus),
    }


def parse_tags(tags: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for item in (tags or "").split(";"):
        item = item.strip()
        if not item:
            continue
        key, separator, value = item.partition("=")
        if separator:
            parsed[key.strip()] = value.strip()
    return parsed


def normalize_strain_name(name: str) -> str:
    cleaned = (name or "").strip()
    cleaned = re.sub(r"^BASE\s*[–—-]\s*BASE\s*[–—-]\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+B-Buds\s*\([^)]*\)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+-\s+(?:Ounce|Quarter)\s*\([^)]*\)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\((?:1/8|1/4|1/2|1)\s*oz\)\s*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def find_source_strain(rows: List[Dict[str, str]], requested_name: str) -> Dict[str, str]:
    target = requested_name.strip().casefold()
    candidates: List[Dict[str, str]] = []

    for row in rows:
        row_name = normalize_strain_name(row.get("name", ""))
        if row_name.casefold() != target:
            continue
        category = (row.get("product_category", "") or "").strip().casefold()
        tags = (row.get("tags", "") or "").strip()
        if "b-buds" in category:
            continue
        if not category.startswith("flower") or not tags:
            continue
        candidates.append(row)

    if not candidates:
        raise ValueError(
            f'Could not find a sellable flower row named "{requested_name}" with COA tags.'
        )

    def score(row: Dict[str, str]) -> tuple[int, int, int]:
        tags = parse_tags(row.get("tags", ""))
        indexes = [
            int(match.group(1))
            for key in tags
            if (match := re.fullmatch(r"coa_ref_(\d+)_file", key))
        ]
        highest = max(indexes, default=-1)
        complete = int(any(
            tags.get(f"coa_ref_{index}_file") and
            tags.get(f"coa_ref_{index}_thc") and
            tags.get(f"coa_ref_{index}_lot")
            for index in indexes
        ))
        eighth = int("eighth" in (row.get("product_category", "") or "").casefold())
        return complete, highest, eighth

    return max(candidates, key=score)


def source_values_from_row(row: Dict[str, str]) -> tuple[str, str, str, str, int]:
    product_name = normalize_strain_name(row.get("name", ""))
    tags = parse_tags(row.get("tags", ""))

    indexes = sorted({
        int(match.group(1))
        for key in tags
        if (match := re.fullmatch(r"coa_ref_(\d+)_(?:file|thc|lot)", key))
    }, reverse=True)

    selected_index: int | None = None
    for index in indexes:
        if tags.get(f"coa_ref_{index}_file") and tags.get(f"coa_ref_{index}_thc"):
            selected_index = index
            break
    if selected_index is None:
        for index in indexes:
            if tags.get(f"coa_ref_{index}_file"):
                selected_index = index
                break
    if selected_index is None:
        raise ValueError(f'Found "{product_name}", but it has no coa_ref_N_file tag.')

    coa_file = tags.get(f"coa_ref_{selected_index}_file", "")
    thc = tags.get(f"coa_ref_{selected_index}_thc", tags.get("thc", ""))
    lot = tags.get(f"coa_ref_{selected_index}_lot", "")

    if not thc:
        raise ValueError(
            f'Found "{product_name}", but COA reference {selected_index} has no THC value.'
        )
    if not lot:
        lot = extract_lot_from_filename(coa_file.replace("%20", " "))

    return product_name, thc, coa_file, lot, selected_index


def build_tags(thc: str, coa_filename: str, coa_lot: str = "") -> str:
    tags = ["sellable_composite=1", "usecoa=1", "netwt=14g"]
    normalized_thc = thc.strip().removesuffix("%").strip()
    if coa_filename.strip():
        encoded_file = encode_spaces(coa_filename.strip())
        tags.append(f"coa_ref_0_file={encoded_file}")
        tags.append(f"coa_ref_0_url=/coas/flower/{encoded_file}")
        if normalized_thc:
            tags.append(f"coa_ref_0_thc={normalized_thc}")
        lot = coa_lot.strip() or extract_lot_from_filename(coa_filename.strip())
        if lot:
            tags.append(f"coa_ref_0_lot={lot}")
    elif normalized_thc:
        tags.append(f"thc={normalized_thc}")
    return ";".join(tags)


def read_csv(path: Path) -> tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")
        fieldnames = list(reader.fieldnames)
        rows = []
        for row in reader:
            rows.append({field: (row.get(field, "") or "") for field in fieldnames})
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
    return {(row.get(column, "") or "").strip() for row in rows if (row.get(column, "") or "").strip()}


def blank_row(fieldnames: List[str]) -> Dict[str, str]:
    return {field: "" for field in fieldnames}


def update_existing_columns(row: Dict[str, str], values: Dict[str, str]) -> None:
    for column, value in values.items():
        if column in row:
            row[column] = value


def parse_optional_decimal(value: str, field_name: str) -> Decimal | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid {field_name} '{value}'. Expected something like 10.50"
        ) from exc


def build_product_rows(
    fieldnames: List[str], product_name: str, inventory_half_ounces: str,
    thc: str, coa_filename: str, coa_lot: str,
    supply_price_half_ounce: Decimal | None, existing_handles: Set[str],
    existing_skus: Set[str],
) -> List[Dict[str, str]]:
    slug = slugify(product_name)
    if not slug:
        raise ValueError("Could not generate a valid handle from the product name.")
    skus = build_sku_family(product_name, existing_skus)
    base_handle = make_unique_handle(f"base-{slug}-bbuds", existing_handles)
    base_name = f"BASE – BASE – {product_name} B-Buds (1/2 oz)"
    description = f"<p>{product_name} B-Buds Flower</p>"
    tags = build_tags(thc, coa_filename, coa_lot)
    rows: List[Dict[str, str]] = []

    row = blank_row(fieldnames)
    update_existing_columns(row, {
        "id": "", "handle": base_handle, "sku": skus["base"],
        "composite_name": "", "composite_sku": "", "composite_quantity": "",
        "name": base_name, "description": description,
        "product_category": "Flower / B-Buds / Base", "tags": "",
        "supply_price": format_money(supply_price_half_ounce) if supply_price_half_ounce is not None else "",
        "retail_price": BBUD_SIZE_INFO["half_ounce"]["retail_price"],
        "brand_name": DEFAULT_BRAND, "supplier_name": "", "supplier_code": "",
        "active": "1", "track_inventory": "1",
        "outlet_tax_Main_Outlet": DEFAULT_TAX,
        "inventory_Main_Outlet": inventory_half_ounces,
        "reorder_point_Main_Outlet": "", "restock_level_Main_Outlet": "",
    })
    rows.append(row)

    for size_key in ["half_ounce", "ounce", "quarter_pound", "pound"]:
        size = BBUD_SIZE_INFO[size_key]
        sellable_name = f"{product_name} B-Buds {size['name_suffix']}"
        sellable_handle = make_unique_handle(f"{slug}-bbuds-{size['handle_suffix']}", existing_handles)
        quantity = Decimal(size["composite_quantity"])
        supply_price = format_money(supply_price_half_ounce * quantity) if supply_price_half_ounce is not None else ""

        row = blank_row(fieldnames)
        update_existing_columns(row, {
            "id": "", "handle": sellable_handle, "sku": skus[size_key],
            "composite_name": "", "composite_sku": "", "composite_quantity": "",
            "name": sellable_name, "description": description,
            "product_category": size["category"], "tags": tags,
            "supply_price": supply_price, "retail_price": size["retail_price"],
            "brand_name": DEFAULT_BRAND, "supplier_name": "", "supplier_code": "",
            "active": "1", "track_inventory": "0",
            "outlet_tax_Main_Outlet": DEFAULT_TAX, "inventory_Main_Outlet": "",
            "reorder_point_Main_Outlet": "", "restock_level_Main_Outlet": "",
        })
        rows.append(row)

        row = blank_row(fieldnames)
        update_existing_columns(row, {
            "id": "", "handle": "", "sku": skus[size_key],
            "composite_name": base_name, "composite_sku": skus["base"],
            "composite_quantity": size["composite_quantity"], "name": sellable_name,
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
    cleaned = value.strip().removesuffix("%").strip()
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
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--name", help='New strain name, e.g. "Halle Berry"')
    source_group.add_argument(
        "--from-strain",
        help="Use an existing flower strain in the source CSV and copy its name and newest complete COA reference.",
    )
    parser.add_argument(
        "--inventory-half-ounces", required=True, type=validate_inventory_half_ounces,
        help="Inventory count in half-ounce units. Example: 32 for one pound.",
    )
    parser.add_argument("--thc", default="", type=validate_thc, help="Optional THC content.")
    parser.add_argument("--coa-file", default="", help="Optional exact COA file name.")
    parser.add_argument("--lot", default="", help="Optional explicit COA lot.")
    parser.add_argument("--supply-price-half-ounce", default="", help="Optional supply cost per half-ounce unit.")
    parser.add_argument("--source-csv", required=True, help="Path to source CSV.")
    parser.add_argument("--output-csv", help="Optional output CSV path. If omitted, source CSV is overwritten.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_csv = Path(args.source_csv)
    output_csv = Path(args.output_csv) if args.output_csv else source_csv
    rows, fieldnames = read_csv(source_csv)
    ensure_required_columns(fieldnames)

    source_coa_index: int | None = None
    if args.from_strain:
        if args.thc or args.coa_file or args.lot:
            raise ValueError("Do not use --thc, --coa-file, or --lot with --from-strain; those values come from the CSV.")
        source_row = find_source_strain(rows, args.from_strain)
        product_name, thc, coa_filename, coa_lot, source_coa_index = source_values_from_row(source_row)
    else:
        product_name = args.name.strip()
        thc = args.thc.strip()
        coa_filename = args.coa_file.strip()
        coa_lot = args.lot.strip()

    inventory_half_ounces = args.inventory_half_ounces.strip()
    supply_price_half_ounce = parse_optional_decimal(
        args.supply_price_half_ounce, "supply price per half ounce"
    )

    existing_names = collect_existing(rows, "name")
    canonical_half_ounce_name = f"{product_name} B-Buds (1/2 oz)"
    if canonical_half_ounce_name in existing_names:
        raise ValueError(f'Product already exists in CSV as "{canonical_half_ounce_name}".')

    new_rows = build_product_rows(
        fieldnames, product_name, inventory_half_ounces, thc, coa_filename,
        coa_lot, supply_price_half_ounce, collect_existing(rows, "handle"),
        collect_existing(rows, "sku"),
    )
    rows.extend(new_rows)
    write_csv(output_csv, rows, fieldnames)

    print("Added B-buds product rows successfully.")
    print(f"Product: {product_name} B-Buds")
    if args.from_strain:
        print(f"Source strain: {args.from_strain}")
        print(f"Source COA reference: coa_ref_{source_coa_index}")
    print(f"Inventory half-ounce units: {inventory_half_ounces}")
    print(f"Equivalent pounds: {Decimal(inventory_half_ounces) / Decimal('32')}")
    print(f"THC: {thc if thc else '(not set)'}")
    print(f"COA file: {coa_filename if coa_filename else '(not set)'}")
    print(f"COA lot: {coa_lot if coa_lot else '(not set)'}")
    print(f"Supply price per half ounce: {format_money(supply_price_half_ounce) if supply_price_half_ounce is not None else '(not set)'}")
    print(f"Rows added: {len(new_rows)}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
