#!/usr/bin/env python3
# path: add-strain.py

from __future__ import annotations

import argparse
import csv
import re
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

TIER_INFO = {
    "budget": {
        "display_name": "Budget",
        "supply_price": "4.69",
        "eighth_price": "15",
        "quarter_price": "25",
        "ounce_price": "90",
    },
    "inhouse": {
        "display_name": "In-House",
        "supply_price": "7.81",
        "eighth_price": "20",
        "quarter_price": "35",
        "ounce_price": "130",
    },
    "organic": {
        "display_name": "Organic",
        "supply_price": "8.59",
        "eighth_price": "25",
        "quarter_price": "45",
        "ounce_price": "150",
    },
    "topshelf": {
        "display_name": "Top Shelf",
        "supply_price": "9.77",
        "eighth_price": "25",
        "quarter_price": "45",
        "ounce_price": "150",
    },
    "premium": {
        "display_name": "Premium",
        "supply_price": "9.77",
        "eighth_price": "30",
        "quarter_price": "50",
        "ounce_price": "170",
    },
}

BASE_INVENTORY_EIGHTHS = "128"
DEFAULT_TAX = "Default Tax"


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


def sanitize_for_sku(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name).upper()


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
    sell_seed = (seed + "XXXXXX")[:6]

    return {
        "base": make_unique_sku(f"FL-{base_seed}", existing_skus),
        "eighth": make_unique_sku(f"FL{sell_seed}E", existing_skus),
        "quarter": make_unique_sku(f"FL{sell_seed}Q", existing_skus),
        "ounce": make_unique_sku(f"FL{sell_seed}OZ", existing_skus),
    }


def build_tags(product_name: str, thc: str, coa_filename: str, coa_lot: str = "") -> str:
    encoded_file = encode_spaces(coa_filename)
    tags = [
        f"thc={thc}",
        f"coa_ref_0_file={encoded_file}",
        f"coa_ref_0_url=/coas/flower/{encoded_file}",
        "usecoa=1",
        "sellable_composite=1",
        "netwt=3.5g",
    ]

    lot = coa_lot.strip() if coa_lot.strip() else extract_lot_from_filename(coa_filename)
    if lot:
        tags.append(f"coa_ref_0_lot={lot}")

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
    missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
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


def build_product_rows(
    fieldnames: List[str],
    product_name: str,
    thc: str,
    coa_filename: str,
    coa_lot: str,
    tier_key: str,
    existing_handles: Set[str],
    existing_skus: Set[str],
) -> List[Dict[str, str]]:
    tier = TIER_INFO[tier_key]
    tier_display = tier["display_name"]
    supply_price = tier["supply_price"]
    eighth_price = tier["eighth_price"]
    quarter_price = tier["quarter_price"]
    ounce_price = tier["ounce_price"]

    slug = slugify(product_name)
    if not slug:
        raise ValueError("Could not generate a valid handle from the product name.")

    handles = {
        "base": make_unique_handle(f"base-{slug}", existing_handles),
        "eighth": make_unique_handle(slug, existing_handles),
        "quarter": make_unique_handle(f"{slug}-quarter", existing_handles),
        "ounce": make_unique_handle(f"{slug}-ounce", existing_handles),
    }

    skus = build_sku_family(product_name, existing_skus)

    base_name = f"BASE – BASE – {product_name} (1/8)"
    eighth_name = f"{product_name} (1/8 oz)"
    quarter_name = f"{product_name} (1/4 oz)"
    ounce_name = f"{product_name} (1 oz)"

    description = f"<p>{product_name} Flower</p>"
    eighth_category = f"Flower / Eighth / {tier_display}"
    quarter_category = f"Flower / Quarter / {tier_display}"
    ounce_category = f"Flower / Ounce / {tier_display}"
    tags = build_tags(
        product_name=product_name,
        thc=thc,
        coa_filename=coa_filename,
        coa_lot=coa_lot,
    )

    rows: List[Dict[str, str]] = []

    # 1) Base source row.
    # This is the inventory-tracked source item.
    # Sellable composite rows below consume this item in 1, 2, or 8 eighth-unit quantities.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": handles["base"],
        "sku": skus["base"],
        "composite_name": "",
        "composite_sku": "",
        "composite_quantity": "",
        "name": base_name,
        "description": description,
        "product_category": eighth_category,
        "tags": "",
        "supply_price": supply_price,
        "retail_price": eighth_price,
        "brand_name": "Various",
        "supplier_name": "",
        "supplier_code": "",
        "active": "1",
        "track_inventory": "1",
        "outlet_tax_Main_Outlet": DEFAULT_TAX,
        "inventory_Main_Outlet": BASE_INVENTORY_EIGHTHS,
        "reorder_point_Main_Outlet": "",
        "restock_level_Main_Outlet": "",
    })
    rows.append(row)

    # 2) Sellable 1/8 row.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": handles["eighth"],
        "sku": skus["eighth"],
        "composite_name": "",
        "composite_sku": "",
        "composite_quantity": "",
        "name": eighth_name,
        "description": description,
        "product_category": eighth_category,
        "tags": tags,
        "supply_price": supply_price,
        "retail_price": eighth_price,
        "brand_name": "Various",
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

    # 3) 1/8 component row.
    # One sellable 1/8 consumes one base 1/8 inventory unit.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": "",
        "sku": skus["eighth"],
        "composite_name": base_name,
        "composite_sku": skus["base"],
        "composite_quantity": "1",
        "name": eighth_name,
    })
    rows.append(row)

    # 4) Sellable quarter row.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": handles["quarter"],
        "sku": skus["quarter"],
        "composite_name": "",
        "composite_sku": "",
        "composite_quantity": "",
        "name": quarter_name,
        "description": description,
        "product_category": quarter_category,
        "tags": "",
        "supply_price": supply_price,
        "retail_price": quarter_price,
        "brand_name": "Various",
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

    # 5) Quarter component row.
    # One sellable 1/4 consumes two base 1/8 inventory units.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": "",
        "sku": skus["quarter"],
        "composite_name": base_name,
        "composite_sku": skus["base"],
        "composite_quantity": "2",
        "name": quarter_name,
    })
    rows.append(row)

    # 6) Sellable ounce row.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": handles["ounce"],
        "sku": skus["ounce"],
        "composite_name": "",
        "composite_sku": "",
        "composite_quantity": "",
        "name": ounce_name,
        "description": description,
        "product_category": ounce_category,
        "tags": "",
        "supply_price": supply_price,
        "retail_price": ounce_price,
        "brand_name": "Various",
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

    # 7) Ounce component row.
    # One sellable ounce consumes eight base 1/8 inventory units.
    row = blank_row(fieldnames)
    row.update({
        "id": "",
        "handle": "",
        "sku": skus["ounce"],
        "composite_name": base_name,
        "composite_sku": skus["base"],
        "composite_quantity": "8",
        "name": ounce_name,
    })
    rows.append(row)

    return rows


def validate_tier(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in TIER_INFO:
        raise argparse.ArgumentTypeError(
            f"Invalid tier '{value}'. Must be one of: {', '.join(TIER_INFO.keys())}"
        )
    return normalized


def validate_thc(value: str) -> str:
    cleaned = value.strip()
    if not re.fullmatch(r"\d+(\.\d+)?", cleaned):
        raise argparse.ArgumentTypeError(
            f"Invalid THC content '{value}'. Expected something like 22.430"
        )
    return cleaned


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add a flower product family to a Lightspeed CSV export."
    )
    parser.add_argument(
        "--name",
        required=True,
        help='Product name, e.g. "Permanent Chimera"',
    )
    parser.add_argument(
        "--thc",
        required=True,
        type=validate_thc,
        help='THC content, e.g. "34.215"',
    )
    parser.add_argument(
        "--coa-file",
        required=True,
        help="Exact COA file name",
    )
    parser.add_argument(
        "--lot",
        default="",
        help="Optional explicit COA lot. If omitted, the script tries to extract it from the COA filename.",
    )
    parser.add_argument(
        "--tier",
        required=True,
        type=validate_tier,
        help="budget, inhouse, organic, topshelf, or premium",
    )
    parser.add_argument(
        "--source-csv",
        required=True,
        help="Path to source CSV",
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
    thc = args.thc.strip()
    coa_filename = args.coa_file.strip()
    coa_lot = args.lot.strip()
    tier_key = args.tier.strip()

    existing_names = collect_existing(rows, "name")
    canonical_eighth_name = f"{product_name} (1/8 oz)"
    if canonical_eighth_name in existing_names:
        raise ValueError(f'Product already exists in CSV as "{canonical_eighth_name}".')

    existing_handles = collect_existing(rows, "handle")
    existing_skus = collect_existing(rows, "sku")

    new_rows = build_product_rows(
        fieldnames=fieldnames,
        product_name=product_name,
        thc=thc,
        coa_filename=coa_filename,
        coa_lot=coa_lot,
        tier_key=tier_key,
        existing_handles=existing_handles,
        existing_skus=existing_skus,
    )

    rows.extend(new_rows)
    write_csv(output_csv, rows, fieldnames)

    print("Added product rows successfully.")
    print(f"Product: {product_name}")
    print(f"Tier: {tier_key}")
    print(f"THC: {thc}")
    print(f"COA file: {coa_filename}")
    print(f"COA lot: {coa_lot if coa_lot else '(auto from filename if possible)'}")
    print(f"Rows added: {len(new_rows)}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()