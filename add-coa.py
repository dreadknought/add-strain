#!/usr/bin/env python3
# path: add_coa_to_sku.py

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple


# Columns that can update Lightspeed stock tracking or on-hand/reorder values.
# Keep these out of generated import files unless you intentionally want to
# change inventory in Lightspeed.
INVENTORY_COLUMNS = {
    "track_inventory",
    "inventory_Main_Outlet",
    "reorder_point_Main_Outlet",
    "restock_level_Main_Outlet",
}


def encode_spaces(value: str) -> str:
    return value.replace(" ", "%20")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append a COA reference to the tags field for a product row identified by SKU."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to the CSV file to update.",
    )
    parser.add_argument(
        "--sku",
        required=True,
        help="SKU of the row to update.",
    )
    parser.add_argument(
        "--lot",
        required=True,
        help="COA lot value to add.",
    )
    parser.add_argument(
        "--coa-file",
        required=True,
        help="COA filename to add.",
    )
    parser.add_argument(
        "--thc",
        default="",
        help='Optional THC content to store with this COA index, e.g. "34.215" or "31.13%".',
    )
    parser.add_argument(
        "--output-csv",
        help="Optional output CSV path. If omitted, the source CSV is overwritten.",
    )
    return parser.parse_args()


def read_csv(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
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


def remove_inventory_columns(fieldnames: List[str]) -> List[str]:
    """Return CSV headers safe for a Lightspeed product import.

    Lightspeed can update stock values when inventory columns are present in an
    import file. This strips those columns from every output CSV written by this
    script, while leaving non-inventory fields like outlet tax intact.
    """
    return [field for field in fieldnames if field not in INVENTORY_COLUMNS]


def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    output_fieldnames = remove_inventory_columns(fieldnames)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def split_tags(tags_value: str) -> List[str]:
    if not tags_value.strip():
        return []
    return [part for part in tags_value.split(";") if part != ""]


def parse_tag_pairs(tags_value: str) -> List[Tuple[str, str | None]]:
    """
    Preserve original order and allow bare tags without '='.
    """
    pairs: List[Tuple[str, str | None]] = []
    for raw in split_tags(tags_value):
        if "=" in raw:
            key, value = raw.split("=", 1)
            pairs.append((key, value))
        else:
            pairs.append((raw, None))
    return pairs


def build_tags_string(pairs: List[Tuple[str, str | None]]) -> str:
    parts: List[str] = []
    for key, value in pairs:
        if value is None:
            parts.append(key)
        else:
            parts.append(f"{key}={value}")
    return ";".join(parts)


def detect_coa_base_path(product_category: str, sku: str = "") -> str:
    category = (product_category or "").strip().lower()
    sku_clean = (sku or "").strip().upper()

    if category.startswith("flower") or re.fullmatch(r"BB-[A-Z]+-(B|HO|OZ|QP|LB)", sku_clean):
        return "/coas/flower"
    if category.startswith("edibles"):
        return "/coas/edibles"
    if category.startswith("beverages"):
        return "/coas/beverages"
    if category.startswith("vapes"):
        return "/coas/vapes"
    if category.startswith("concentrates"):
        return "/coas/edibles"

    raise ValueError(
        f"Could not determine COA path from product_category '{product_category}' and sku '{sku}'. "
        "Expected a category starting with Flower, Edibles, Beverages, Vapes, or Concentrates, "
        "or a B-buds SKU like BB-HALL-HO."
    )


def get_existing_coa_ref_indexes(tag_pairs: List[Tuple[str, str | None]]) -> List[int]:
    indexes = set()

    for key, _value in tag_pairs:
        match = re.fullmatch(r"coa_ref_(\d+)_(file|url|lot|thc)", key)
        if match:
            indexes.add(int(match.group(1)))

    return sorted(indexes)


def next_coa_ref_index(tag_pairs: List[Tuple[str, str | None]]) -> int:
    existing = get_existing_coa_ref_indexes(tag_pairs)
    if not existing:
        return 0
    return max(existing) + 1


def find_exact_same_coa_index(
    tag_pairs: List[Tuple[str, str | None]],
    lot: str,
    coa_file_encoded: str,
    coa_url: str,
) -> int | None:
    """
    Return the index of an existing coa_ref_N triplet matching lot/file/url.
    """
    refs: Dict[int, Dict[str, str]] = {}

    for key, value in tag_pairs:
        match = re.fullmatch(r"coa_ref_(\d+)_(file|url|lot|thc)", key)
        if match and value is not None:
            idx = int(match.group(1))
            kind = match.group(2)
            refs.setdefault(idx, {})[kind] = value

    for idx, idx_data in refs.items():
        if (
            idx_data.get("file") == coa_file_encoded
            and idx_data.get("url") == coa_url
            and idx_data.get("lot") == lot
        ):
            return idx

    return None


def upsert_tag_pair(
    tag_pairs: List[Tuple[str, str | None]],
    target_key: str,
    target_value: str,
) -> None:
    for i, (key, _value) in enumerate(tag_pairs):
        if key.lower() == target_key.lower():
            tag_pairs[i] = (key, target_value)
            return
    tag_pairs.append((target_key, target_value))


def normalize_thc_value(thc_raw: str) -> str:
    return thc_raw.strip().removesuffix("%").strip()


def append_coa_tags(
    existing_tags: str,
    lot: str,
    coa_filename: str,
    product_category: str,
    thc: str = "",
    sku: str = "",
) -> str:
    tag_pairs = parse_tag_pairs(existing_tags)

    encoded_file = encode_spaces(coa_filename)
    coa_base = detect_coa_base_path(product_category, sku=sku)
    coa_url = f"{coa_base}/{encoded_file}"

    existing_ref_index = find_exact_same_coa_index(
        tag_pairs=tag_pairs,
        lot=lot,
        coa_file_encoded=encoded_file,
        coa_url=coa_url,
    )

    ref_index = existing_ref_index if existing_ref_index is not None else next_coa_ref_index(tag_pairs)

    if existing_ref_index is None:
        tag_pairs.append((f"coa_ref_{ref_index}_file", encoded_file))
        tag_pairs.append((f"coa_ref_{ref_index}_url", coa_url))
        tag_pairs.append((f"coa_ref_{ref_index}_lot", lot))

    normalized_thc = normalize_thc_value(thc)
    if normalized_thc:
        upsert_tag_pair(tag_pairs, f"coa_ref_{ref_index}_thc", normalized_thc)

    if existing_ref_index is not None and not normalized_thc:
        return existing_tags

    return build_tags_string(tag_pairs)


def find_matching_rows(rows: List[Dict[str, str]], sku: str) -> List[Dict[str, str]]:
    target = sku.strip()
    return [row for row in rows if (row.get("sku", "") or "").strip() == target]


def is_sellable_product_row(row: Dict[str, str]) -> bool:
    """
    Prefer the real product row over composite wiring rows.

    In this CSV shape, the sellable row usually has product data such as a handle,
    product category, description, tags, prices, and inventory flags. The composite
    component row often shares the same SKU but has blank product fields and instead
    carries composite_name/composite_sku/composite_quantity.
    """
    meaningful_product_fields = [
        "handle",
        "product_category",
        "description",
        "tags",
        "supply_price",
        "retail_price",
        "active",
        "track_inventory",
    ]
    return any((row.get(field, "") or "").strip() for field in meaningful_product_fields)


def choose_target_row(matches: List[Dict[str, str]], sku: str) -> Dict[str, str]:
    if not matches:
        raise ValueError(f'No row found with sku "{sku}".')

    if len(matches) == 1:
        return matches[0]

    sellable_matches = [row for row in matches if is_sellable_product_row(row)]

    if len(sellable_matches) == 1:
        return sellable_matches[0]

    if len(sellable_matches) > 1:
        raise ValueError(
            f'Found multiple sellable-looking rows with sku "{sku}". '
            "Refusing to guess which row to update."
        )

    raise ValueError(
        f'Found multiple rows with sku "{sku}", but none looked like a sellable product row. '
        "Refusing to guess which row to update."
    )


def main() -> None:
    args = parse_args()

    source_csv = Path(args.csv)
    output_csv = Path(args.output_csv) if args.output_csv else source_csv

    sku = args.sku.strip()
    lot = args.lot.strip()
    coa_file = args.coa_file.strip()
    thc = args.thc.strip()

    rows, fieldnames = read_csv(source_csv)

    if "sku" not in fieldnames:
        raise ValueError("CSV is missing required column: sku")
    if "tags" not in fieldnames:
        raise ValueError("CSV is missing required column: tags")
    if "product_category" not in fieldnames:
        raise ValueError("CSV is missing required column: product_category")

    matches = find_matching_rows(rows, sku)
    row = choose_target_row(matches, sku)

    existing_tags = row.get("tags", "") or ""
    product_category = row.get("product_category", "") or ""

    updated_tags = append_coa_tags(
        existing_tags=existing_tags,
        lot=lot,
        coa_filename=coa_file,
        product_category=product_category,
        thc=thc,
        sku=sku,
    )

    row["tags"] = updated_tags

    write_csv(output_csv, rows, fieldnames)

    print("Updated tags successfully.")
    print(f"SKU: {sku}")
    print(f"Lot added: {lot}")
    print(f"COA file added: {coa_file}")
    print(f"THC added: {thc if thc else '(not set)'}")
    print(f"Inventory columns removed from output: {', '.join(sorted(INVENTORY_COLUMNS))}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()