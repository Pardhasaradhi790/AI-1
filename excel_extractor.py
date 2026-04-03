"""
Structured extraction for Michigan MDHHS fee schedule workbooks.

The downloaded files are not uniform. This module handles the common cases we
have observed in the sample workbooks:
  - standard single-table sheets after a boilerplate preamble
  - Telemedicine sheets with multiple header blocks in a single sheet
  - older workbooks with junk/empty secondary tabs
  - rate-only sheets like ABA where custom rate columns must be preserved

Each extracted row keeps a normalized shape plus the original raw row as JSON.
"""

from __future__ import annotations

import json
import os
import re

import pandas as pd


_WS_RE = re.compile(r"\s+")
_CODE_SECTION_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]{1,14}$")


def norm_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return _WS_RE.sub(" ", str(value)).strip()


def norm_lower(value) -> str:
    return norm_text(value).lower()


def row_tokens(raw_row) -> list[str]:
    values = []
    for value in raw_row:
        token = norm_lower(value)
        if token:
            values.append(token)
    return values


def is_header_row(tokens: list[str]) -> bool:
    if not tokens:
        return False

    has_code = any(
        token in {
            "revenue code",
            "rev code",
            "hcpcs code",
            "cpt code",
            "procedure code",
            "service code",
            "code",
        } or token.endswith(" code")
        for token in tokens
    )
    has_desc = any(
        token in {"short description", "description", "service description", "short desc", "desc"}
        or "description" in token
        for token in tokens
    )
    return has_code and has_desc


def detect_code_type_from_header(header_cells) -> str:
    text = " ".join(norm_lower(cell) for cell in header_cells if norm_lower(cell))
    if "revenue" in text and "code" in text:
        return "REV"
    if "hcpcs" in text:
        return "HCPCS"
    if "cpt" in text:
        return "CPT"
    if "service code" in text:
        return "SERVICE"
    return "CODE"


def make_unique_columns(columns) -> list[str]:
    seen: dict[str, int] = {}
    output: list[str] = []
    for column in columns:
        base = norm_text(column) or "Unnamed"
        if base not in seen:
            seen[base] = 1
            output.append(base)
        else:
            seen[base] += 1
            output.append(f"{base}_{seen[base]}")
    return output


def normalize_headers_for_block(columns) -> list[str]:
    mapping = {
        "revenue code": "Revenue Code",
        "rev code": "Revenue Code",
        "hcpcs code": "HCPCS Code",
        "cpt code": "CPT Code",
        "procedure code": "Code",
        "service code": "Code",
        "code": "Code",
        "short description": "Short Description",
        "service description": "Short Description",
        "description": "Short Description",
        "short desc": "Short Description",
        "desc": "Short Description",
        "modifier": "Modifier",
        "mod": "Modifier",
        "age range": "Age Range",
        "age": "Age Range",
        "non fac fee": "Non Fac Fee",
        "non-fac fee": "Non Fac Fee",
        "non facility fee": "Non Fac Fee",
        "rate": "Rate",
        "fee": "Rate",
        "facility fee": "Fac Fee",
        "fac fee": "Fac Fee",
        "facility rate": "Fac Fee",
        "comments": "Comments",
        "comment": "Comments",
        "remarks": "Comments",
        "notes": "Comments",
        "note": "Comments",
        "effective date": "Effective Date",
        "effective date**": "Effective Date",
    }

    output = []
    for column in columns:
        key = norm_lower(column)
        output.append(mapping.get(key, norm_text(column)))
    return output


def normalize_code_value(value):
    if value is None:
        return None
    try:
        if isinstance(value, float) and pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        float_value = float(value)
        if float_value.is_integer():
            return str(int(float_value))
        return str(value).strip().upper()

    text = norm_text(value)
    if not text or text.lower() == "nan":
        return None
    return text.replace(" ", "").upper()


def parse_decimal(value):
    text = norm_text(value)
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"nan", "n/a", "na", "m"}:
        return None
    text = text.replace("$", "").replace(",", "")
    try:
        number = float(text)
    except Exception:
        return None
    if not pd.notna(number):
        return None
    return round(number, 4)


def is_data_code(code_type: str, code_value: str | None) -> bool:
    if not code_value:
        return False
    lowered = code_value.lower()
    if lowered in {"code", "hcpcs", "cpt", "revenue", "revenuecode"}:
        return False

    if code_type == "REV":
        return code_value.isdigit() and 3 <= len(code_value) <= 4

    if not _CODE_SECTION_RE.match(code_value):
        return False
    return any(char.isdigit() for char in code_value)


def extract_tables_from_sheet(file_path: str, sheet_name: str) -> list[dict]:
    raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    if raw.empty:
        return []

    header_indexes: list[int] = []
    scan_rows = min(2000, len(raw))
    for index in range(scan_rows):
        if is_header_row(row_tokens(raw.iloc[index].tolist())):
            header_indexes.append(index)

    if not header_indexes:
        return []

    tables = []
    for position, header_index in enumerate(header_indexes):
        next_header_index = header_indexes[position + 1] if position + 1 < len(header_indexes) else len(raw)
        header_cells = raw.iloc[header_index].tolist()
        code_type = detect_code_type_from_header(header_cells)

        block = raw.iloc[header_index + 1:next_header_index].copy()
        if block.empty:
            continue

        block.columns = make_unique_columns(header_cells)
        block = block.dropna(axis=1, how="all")
        block.columns = normalize_headers_for_block(block.columns)

        valid_columns = [column for column in block.columns if not norm_lower(column).startswith("unnamed")]
        if not valid_columns:
            continue
        block = block[valid_columns]
        tables.append(
            {
                "header_row_index": header_index,
                "table_index": len(tables) + 1,
                "code_type": code_type,
                "data": block,
            }
        )

    return tables


def extract_workbook_rows(
    file_path: str,
    fee_id: str,
    primary_fs: str,
    fs_segment: str,
    file_url: str,
) -> list[dict]:
    file_name = os.path.basename(file_path)
    workbook = pd.ExcelFile(file_path, engine="openpyxl")
    extracted_rows: list[dict] = []

    for sheet_name in workbook.sheet_names:
        tables = extract_tables_from_sheet(file_path, sheet_name)
        if not tables:
            continue

        for table in tables:
            data = table["data"]
            code_type = table["code_type"]
            table_index = table["table_index"]
            header_row_index = table["header_row_index"]

            preferred_code_columns = [
                "Revenue Code",
                "HCPCS Code",
                "CPT Code",
                "Code",
            ]
            if code_type == "REV":
                preferred_code_columns = ["Revenue Code", "Code", "HCPCS Code", "CPT Code"]

            code_column = next((column for column in preferred_code_columns if column in data.columns), None)
            if code_column is None:
                continue

            known_columns = {
                "Revenue Code",
                "HCPCS Code",
                "CPT Code",
                "Code",
                "Short Description",
                "Modifier",
                "Age Range",
                "Non Fac Fee",
                "Fac Fee",
                "Rate",
                "Effective Date",
                "Comments",
            }
            extra_columns = [column for column in data.columns if column not in known_columns]

            for row_index, row in data.iterrows():
                raw_code = row.get(code_column)
                code_value = normalize_code_value(raw_code)

                if not is_data_code(code_type, code_value):
                    # Edge case: row has no valid code but contains text
                    # (e.g. a "Note:" row after a Revenue Code data row).
                    # Attach it as a comment to the previous data row.
                    if extracted_rows:
                        all_text = " ".join(
                            norm_text(row.get(col))
                            for col in data.columns
                            if norm_text(row.get(col))
                        )
                        if all_text:
                            prev = extracted_rows[-1]
                            if prev["comments"]:
                                prev["comments"] += " | " + all_text
                            else:
                                prev["comments"] = all_text
                    continue

                extra_fields = {}
                for column in extra_columns:
                    value = norm_text(row.get(column))
                    if value:
                        extra_fields[column] = value

                short_description = norm_text(row.get("Short Description")) or None
                modifier = norm_text(row.get("Modifier")) or None
                age_range = norm_text(row.get("Age Range")) or None
                comments = norm_text(row.get("Comments")) or None
                effective_date_text = norm_text(row.get("Effective Date")) or None

                raw_row = {}
                for column in data.columns:
                    value = row.get(column)
                    text = norm_text(value)
                    raw_row[column] = text if text else None

                extracted_rows.append(
                    {
                        "fee_id": fee_id,
                        "primary_fs": primary_fs,
                        "fs_segment": fs_segment,
                        "file_name": file_name,
                        "file_url": file_url,
                        "sheet_name": sheet_name,
                        "table_index": table_index,
                        "row_number": int(row_index) + 2,
                        "source_row_number": int(row_index) + header_row_index + 2,
                        "code_type": code_type,
                        "code_value": code_value,
                        "short_description": short_description,
                        "modifier": modifier,
                        "age_range": age_range,
                        "non_fac_fee": parse_decimal(row.get("Non Fac Fee")),
                        "fac_fee": parse_decimal(row.get("Fac Fee")),
                        "rate": parse_decimal(row.get("Rate")),
                        "effective_date_text": effective_date_text,
                        "comments": comments,
                        "extra_fields": extra_fields,
                        "row_data": raw_row,
                    }
                )

    if extracted_rows:
        return extracted_rows

    fallback_rows: list[dict] = []
    for sheet_name in workbook.sheet_names:
        raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")
        if raw.empty:
            continue
        for row_index, row in raw.fillna("").iterrows():
            values = [norm_text(value) for value in row.tolist()]
            if not any(values):
                continue
            fallback_rows.append(
                {
                    "fee_id": fee_id,
                    "primary_fs": primary_fs,
                    "fs_segment": fs_segment,
                    "file_name": file_name,
                    "file_url": file_url,
                    "sheet_name": sheet_name,
                    "table_index": None,
                    "row_number": int(row_index) + 1,
                    "source_row_number": int(row_index) + 1,
                    "code_type": None,
                    "code_value": None,
                    "short_description": None,
                    "modifier": None,
                    "age_range": None,
                    "non_fac_fee": None,
                    "fac_fee": None,
                    "rate": None,
                    "effective_date_text": None,
                    "comments": None,
                    "extra_fields": {},
                    "row_data": {f"column_{index + 1}": value or None for index, value in enumerate(values)},
                }
            )
    return fallback_rows
