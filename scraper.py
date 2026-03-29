"""
scraper.py
----------
Excel-driven Playwright automation that:
  1. Reads CSRA_FeeID_Master.xlsx to learn which pages to visit and which
     FS Segments to download from each page.
  2. For each unique page: navigates michigan.gov → searches "mdhhs" →
     searches the Primary FS → clicks top result → lands on the fee-schedule page.
  3. On each page, only downloads files for the FS Segments listed in the
     master Excel (ignores other dropdowns on the page).
  4. For each segment: downloads ALL quarter files (Excel preferred over PDF,
     PDF fallback), checks DB to skip duplicates, uploads Excel data to SQL
     Server, and records metadata (fee_id, primary_fs, fs_segment, direct_link).

Usage:
    python scraper.py
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import pandas as pd
import pyodbc
from playwright.sync_api import sync_playwright, Page, Locator
from excel_extractor import extract_workbook_rows

from config import (
    ROOT_URL,
    SEARCH_TERM_1,
    MASTER_EXCEL,
    DOWNLOAD_ROOT,
    HEADLESS,
    SQL_DATABASE,
    SQL_DRIVER,
    SQL_SERVER,
    SQL_TRUSTED,
    SQL_USERNAME,
    SQL_PASSWORD,
    TIMEOUT,
)


# ────────────────────────── helpers ──────────────────────────


def _conn_str() -> str:
    base = f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
    if SQL_TRUSTED:
        base += "Trusted_Connection=yes;"
    else:
        base += f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
    return base


def get_db_connection():
    return pyodbc.connect(_conn_str())


def is_already_downloaded(conn, fee_id: str, primary_fs: str,
                          fs_segment: str, file_url: str) -> bool:
    """Return True when this fee_id + primary_fs + fs_segment + file_url combo exists."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM dbo.downloaded_files "
        "WHERE fee_id = ? AND primary_fs = ? AND fs_segment = ? AND file_url = ?",
        fee_id, primary_fs, fs_segment, file_url,
    )
    return cursor.fetchone() is not None


def record_download(conn, section_name: str, file_label: str, file_url: str,
                    file_name: str, local_path: str,
                    fee_id: str = "", primary_fs: str = "",
                    fs_segment: str = "", direct_link: str = ""):
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO dbo.downloaded_files
           (section_name, file_label, file_url, file_name, local_path,
            fee_id, primary_fs, fs_segment, direct_link)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        section_name, file_label, file_url, file_name, local_path,
        fee_id, primary_fs, fs_segment, direct_link,
    )
    conn.commit()


def record_extraction_metadata(conn, fee_id: str, primary_fs: str,
                               fs_segment: str, file_url: str,
                               file_name: str, sheet_count: int,
                               table_count: int, row_count: int,
                               status: str = "success",
                               error_message: str = None,
                               file_size_bytes: int = None):
    """Insert or update a row in extraction_metadata for daily-check tracking."""
    cursor = conn.cursor()
    # Upsert: delete old row if exists, then insert fresh
    cursor.execute(
        "DELETE FROM dbo.extraction_metadata "
        "WHERE fee_id = ? AND primary_fs = ? AND fs_segment = ? AND file_url = ?",
        fee_id, primary_fs, fs_segment, file_url,
    )
    cursor.execute(
        """INSERT INTO dbo.extraction_metadata
           (fee_id, primary_fs, fs_segment, file_url, file_name,
            sheet_count, table_count, row_count,
            extraction_status, error_message, file_size_bytes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        fee_id, primary_fs, fs_segment, file_url, file_name,
        sheet_count, table_count, row_count,
        status, error_message, file_size_bytes,
    )
    conn.commit()


def upload_excel_to_db(conn, section_name: str, file_name: str, local_path: str,
                       fee_id: str = "", primary_fs: str = "",
                       fs_segment: str = "", file_url: str = ""):
    """Extract structured rows from the workbook and insert them into fee_schedule_data."""
    file_size = os.path.getsize(local_path) if os.path.exists(local_path) else None
    try:
        extracted_rows = extract_workbook_rows(
            local_path,
            fee_id=fee_id,
            primary_fs=primary_fs,
            fs_segment=fs_segment or section_name,
            file_url=file_url,
        )
    except Exception as exc:
        # Record failed extraction in metadata
        record_extraction_metadata(
            conn, fee_id, primary_fs, fs_segment or section_name, file_url,
            file_name, sheet_count=0, table_count=0, row_count=0,
            status="error", error_message=str(exc)[:4000],
            file_size_bytes=file_size,
        )
        raise

    if not extracted_rows:
        record_extraction_metadata(
            conn, fee_id, primary_fs, fs_segment or section_name, file_url,
            file_name, sheet_count=0, table_count=0, row_count=0,
            status="empty", file_size_bytes=file_size,
        )
        print(f"    [WARN] No extractable rows found in {file_name}.")
        return

    # Count unique sheets and tables for metadata
    sheet_names = {r["sheet_name"] for r in extracted_rows if r.get("sheet_name")}
    table_indexes = {(r["sheet_name"], r["table_index"]) for r in extracted_rows
                     if r.get("table_index") is not None}

    cursor = conn.cursor()
    records = []
    for row in extracted_rows:
        records.append(
            (
                section_name,
                file_name,
                row["sheet_name"],
                row["row_number"],
                json.dumps(row["row_data"], ensure_ascii=False),
                row["fee_id"],
                row["primary_fs"],
                row["fs_segment"],
                row["file_url"],
                row["table_index"],
                row["source_row_number"],
                row["code_type"],
                row["code_value"],
                row["short_description"],
                row["modifier"],
                row["age_range"],
                row["non_fac_fee"],
                row["fac_fee"],
                row["rate"],
                row["effective_date_text"],
                row["comments"],
                json.dumps(row["extra_fields"], ensure_ascii=False),
            )
        )

    cursor.executemany(
        """INSERT INTO dbo.fee_schedule_data
           (section_name, file_name, sheet_name, row_number, row_data,
            fee_id, primary_fs, fs_segment, file_url,
            table_index, source_row_number, code_type, code_value,
            short_description, modifier, age_range,
            non_fac_fee, fac_fee, rate, effective_date_text,
            comments, extra_fields)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    conn.commit()
    print(f"    [DB] Uploaded {file_name} ({len(records)} extracted row(s)) to fee_schedule_data.")

    # Record extraction metadata for fast daily checks
    record_extraction_metadata(
        conn, fee_id, primary_fs, fs_segment or section_name, file_url,
        file_name, sheet_count=len(sheet_names),
        table_count=len(table_indexes), row_count=len(records),
        status="success", file_size_bytes=file_size,
    )


# ──── date parsing to determine "latest" ────

# Map labels like "Jan 2026 XLSX" → datetime(2026, 1, 1)
MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_file_date(label: str):
    """Try to extract a (year, month) from text like 'Jan 2026 XLSX'."""
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|"
                  r"january|february|march|april|june|july|august|"
                  r"september|october|november|december)\s+(\d{4})",
                  label, re.IGNORECASE)
    if m:
        month = MONTH_MAP[m.group(1).lower()]
        year = int(m.group(2))
        return datetime(year, month, 1)
    return None


def is_excel_link(url: str) -> bool:
    path_lower = urlparse(url).path.lower()
    return path_lower.endswith((".xlsx", ".xls"))


def is_pdf_link(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def classify_entries(entries: list[dict]) -> list[dict]:
    """
    Given a list of {'label', 'url'} dicts, group by quarter date.
    For each quarter:
      - If both PDF and Excel exist, keep only the Excel.
      - If only PDF exists (no Excel), keep the PDF.
    Skip non-date entries like "Database Instructions".
    Returns a list of {'label', 'url', 'date'} dicts sorted newest-first.
    """
    by_date: dict[datetime, list[dict]] = {}
    for e in entries:
        dt = parse_file_date(e["label"])
        if dt is None:
            continue  # "Database Instructions" etc.
        by_date.setdefault(dt, []).append(e)

    result = []
    for dt in sorted(by_date, reverse=True):
        items = by_date[dt]
        excels = [i for i in items if is_excel_link(i["url"])]
        if excels:
            result.append({**excels[0], "date": dt})
        else:
            # No Excel for this quarter – fall back to PDF
            pdfs = [i for i in items if is_pdf_link(i["url"])]
            if pdfs:
                result.append({**pdfs[0], "date": dt})
    return result


def heading_to_folder(heading: str) -> str:
    """Convert a heading like 'Oral/Maxillofacial Surgeon' → 'oral_maxillofacial_surgeon'."""
    name = heading.strip().lower()
    name = re.sub(r"[/\\]+", "_", name)       # slashes → underscore
    name = re.sub(r"[^a-z0-9]+", "_", name)   # non-alphanum → underscore
    return name.strip("_")


def filename_from_url(url: str) -> str:
    """Extract a clean filename from the URL path."""
    path = urlparse(url).path
    return os.path.basename(path).split("?")[0]


# ────────────────────────── Master Excel loader ──────────────────────────


def load_master_excel(path: str) -> list[dict]:
    """
    Read CSRA_FeeID_Master.xlsx and return a list of page-group dicts:
    [
        {
            'fee_id': 'DZ00006245',
            'primary_fs': 'Physicians/Practitioners/Medical Clinics',
            'segments': ['Anesthesia', 'Certified Nurse Midwife', ...],
        },
        ...
    ]
    Forward-fills Fee ID so blank rows inherit from the row above.
    Groups rows by (fee_id, primary_fs) — each unique Primary FS
    becomes a separate page to navigate to via search.
    """
    df = pd.read_excel(path, sheet_name="MI", dtype=str).fillna("")

    current_fee_id = ""

    groups: dict[tuple, dict] = {}  # keyed by (fee_id, primary_fs)

    for _, row in df.iterrows():
        fee_id = row.get("Fee ID", "").strip()
        primary_fs = row.get("Primary FS", "").strip()
        segment = row.get("FS Segments", "").strip()

        if fee_id:
            current_fee_id = fee_id

        if not segment or not primary_fs:
            continue

        key = (current_fee_id, primary_fs)
        if key not in groups:
            groups[key] = {
                "fee_id": current_fee_id,
                "primary_fs": primary_fs,
                "segments": [],
            }
        groups[key]["segments"].append(segment)

    result = list(groups.values())
    print(f"[+] Loaded {len(result)} page group(s) from master Excel:")
    for g in result:
        print(f"    Fee ID={g['fee_id']}  Primary FS={g['primary_fs']}")
        print(f"      Segments ({len(g['segments'])}): {g['segments']}")
    return result


# ────────────────────────── Search-based navigation ──────────────────────────


def navigate_to_mdhhs(page: Page):
    """
    Go to michigan.gov, search for 'mdhhs', click the first result
    to reach the MDHHS home page.
    """
    print(f"[+] Navigating to {ROOT_URL} ...")
    page.goto(ROOT_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    print(f'[+] Searching for "{SEARCH_TERM_1}" ...')
    search_button = page.locator(
        "button.header-search-button, button[aria-label*='Search'], "
        ".search-toggle, header button:has(svg)"
    )
    if search_button.count() > 0:
        search_button.first.click()
        page.wait_for_timeout(500)

    search_input = page.locator(
        "input[type='search'], input[name='q'], input.search-input, "
        "input[placeholder*='Search'], #search-input"
    ).first
    search_input.fill(SEARCH_TERM_1)
    search_input.press("Enter")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(3000)

    print("[+] Clicking first search result ...")
    first_result = page.locator(
        ".CoveoResultLink, .search-results a, .result-link a, "
        "a[href*='mdhhs']"
    ).first
    first_result.click()
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(3000)
    print(f"[+] Landed on MDHHS: {page.url}")


def navigate_to_page_via_search(page: Page, search_term: str):
    """
    From the MDHHS home page, search for *search_term* and click
    the top result to reach the target fee-schedule page.
    """
    print(f'[+] Searching MDHHS site for "{search_term}" ...')
    search_button = page.locator(
        "button.header-search-button, button[aria-label*='Search'], "
        ".search-toggle, header button:has(svg)"
    )
    if search_button.count() > 0:
        search_button.first.click()
        page.wait_for_timeout(500)

    search_input = page.locator(
        "input[type='search'], input[name='q'], input.search-input, "
        "input[placeholder*='Search'], #search-input"
    ).first
    search_input.fill(search_term)
    search_input.press("Enter")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(5000)

    print(f"[DEBUG] Search results URL: {page.url}")
    print(f"[DEBUG] Page title: {page.title()}")

    # Find result link whose text matches the search term
    all_links = page.locator("#content a[href]")
    link_count = all_links.count()
    print(f"[DEBUG] Found {link_count} links in #content")

    matched_link = None
    for i in range(link_count):
        txt = all_links.nth(i).inner_text(timeout=3000).strip()
        href = all_links.nth(i).get_attribute("href") or ""
        if txt:
            print(f"  [{i}] {txt!r}  →  {href}")
        if (txt and txt.lower() == search_term.lower()
                and href.startswith("http")
                and "billingreimbursement" in href):
            matched_link = all_links.nth(i)
            print(f"[+] Matched result link: {txt!r} → {href}")
            break

    # Fallback 1: exact text match (any href)
    if matched_link is None:
        for i in range(link_count):
            txt = all_links.nth(i).inner_text(timeout=3000).strip()
            href = all_links.nth(i).get_attribute("href") or ""
            if txt and txt.lower() == search_term.lower() and href.startswith("http"):
                matched_link = all_links.nth(i)
                print(f"[+] Matched result link (exact text): {txt!r} → {href}")
                break

    if matched_link is None:
        # Fallback: find a link that contains the search term (or vice versa)
        for i in range(link_count):
            txt = all_links.nth(i).inner_text(timeout=3000).strip()
            href = all_links.nth(i).get_attribute("href") or ""
            if not txt or not href.startswith("http"):
                continue
            st_lower = search_term.lower()
            txt_lower = txt.lower()
            if st_lower in txt_lower or txt_lower in st_lower:
                matched_link = all_links.nth(i)
                print(f"[+] Partial-match result link: {txt!r} → {href}")
                break

    if matched_link is None:
        # Last resort: pick the first michigan.gov/mdhhs link that isn't a PDF/doc
        for i in range(link_count):
            txt = all_links.nth(i).inner_text(timeout=3000).strip()
            href = all_links.nth(i).get_attribute("href") or ""
            if (txt and "michigan.gov/mdhhs" in href
                    and not href.endswith((".pdf", ".dotx", ".docx"))):
                matched_link = all_links.nth(i)
                print(f"[+] Best-guess result link: {txt!r} → {href}")
                break

    if matched_link is None:
        raise RuntimeError(
            f"Could not find a search result matching '{search_term}' on {page.url}"
        )

    print("[+] Clicking matched search result ...")
    matched_link.click()
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(3000)
    print(f"[+] Landed on: {page.url}")


# ────────────────────────── Section discovery ──────────────────────────


def discover_sections(page: Page) -> list[dict]:
    """
    Auto-discover all fee-schedule dropdown sections on the current page.
    Returns a list of {'heading': str, 'folder': str} dicts.
    """
    headings = page.locator("div.link-list-dropdown h3")
    count = headings.count()
    sections = []
    for i in range(count):
        text = headings.nth(i).inner_text().strip()
        if text:
            sections.append({
                "heading": text,
                "folder": heading_to_folder(text),
            })
    print(f"[+] Discovered {len(sections)} section(s): {[s['heading'] for s in sections]}")
    return sections


# ────────────────────────── Playwright scraping ──────────────────────────


def scrape_section(page: Page, section_heading: str, folder_name: str,
                   conn, download_dir: str,
                   fee_id: str = "", primary_fs: str = "",
                   fs_segment: str = "", direct_link: str = ""):
    """
    Process one <h3> section: open dropdown, collect links, download ALL
    quarter files that are not already in the database.
    For each quarter: prefer Excel over PDF; if no Excel, download PDF.
    Preserves original filenames. Stores metadata alongside each record.
    """
    print(f"\n{'='*60}")
    print(f"  Section: {section_heading}")
    print(f"{'='*60}")

    # Derive the base domain from the current page URL
    parsed = urlparse(page.url)
    domain = f"{parsed.scheme}://{parsed.netloc}"

    # Find the <h3> that contains the section heading text
    # Escape single quotes in heading to avoid CSS selector breakage
    escaped_heading = section_heading.replace("'", "\\'")
    h3_locator = page.locator(
        f"div.link-list-dropdown h3:text-is('{escaped_heading}')"
    )
    if h3_locator.count() == 0:
        print(f"  [!] Could not find heading '{section_heading}' – skipping.")
        return

    # The dropdown wrapper is a sibling after h3 inside the same parent
    wrapper = h3_locator.locator("xpath=ancestor::div[contains(@class,'wrapper-quicklist')]")

    # Click the dropdown button to open it
    dd_button = wrapper.locator("button.dd-title-button")
    dd_button.scroll_into_view_if_needed()
    dd_button.click()
    page.wait_for_timeout(500)

    # Collect all <li> items from the dropdown
    items = wrapper.locator("ul.dropdown li.link-list-dropdown-item a")
    count = items.count()
    print(f"  Found {count} dropdown item(s).")

    entries = []
    for i in range(count):
        anchor = items.nth(i)
        label = anchor.inner_text().strip()
        data_link = anchor.get_attribute("data-link") or ""
        if data_link:
            full_url = urljoin(domain, data_link)
            entries.append({"label": label, "url": full_url})

    if not entries:
        print("  [!] No downloadable entries found – skipping.")
        dd_button.click()
        return

    # Get one file per quarter (Excel preferred, PDF fallback)
    to_download = classify_entries(entries)

    # Also collect undated entries (e.g. standalone PDFs with no quarter date)
    dated_urls = {e["url"] for e in to_download}
    undated = [e for e in entries if e["url"] not in dated_urls]
    if undated:
        for u in undated:
            to_download.append({**u, "date": None})
        print(f"  {len(to_download)} file(s) to process ({len(undated)} undated).")
    elif to_download:
        print(f"  {len(to_download)} quarter file(s) available.")
    else:
        print("  [!] No downloadable entries – skipping.")
        dd_button.click()
        return

    # Prepare local folder: fee_id / primary_fs / segment_name
    safe_fee_id = fee_id.strip() or "unknown_fee_id"
    safe_primary = heading_to_folder(primary_fs) if primary_fs else "unknown_primary"
    safe_segment = heading_to_folder(fs_segment) if fs_segment else folder_name
    section_dir = os.path.join(download_dir, safe_fee_id, safe_primary, safe_segment)
    os.makedirs(section_dir, exist_ok=True)

    downloaded_count = 0
    skipped_count = 0

    for entry in to_download:
        file_url = entry["url"]
        file_label = entry["label"]
        file_date = entry["date"]
        fname = filename_from_url(file_url)
        local_path = os.path.join(section_dir, fname)

        # Check if already in DB – skip if so
        if is_already_downloaded(conn, fee_id, primary_fs, fs_segment, file_url):
            print(f"    [SKIP] {file_label} – already in DB.")
            skipped_count += 1
            continue

        # Download the file
        date_str = f" ({file_date:%B %Y})" if file_date else ""
        print(f"    Downloading {file_label}{date_str} → {fname}")
        response = page.request.get(file_url)
        if not response.ok:
            print(f"    [ERR] HTTP {response.status} – skipping.")
            continue

        with open(local_path, "wb") as f:
            f.write(response.body())
        print(f"    [OK] Saved ({os.path.getsize(local_path):,} bytes).")

        # Upload Excel data to SQL Server (skip PDFs for now)
        if is_excel_link(file_url):
            try:
                upload_excel_to_db(
                    conn,
                    section_heading,
                    fname,
                    local_path,
                    fee_id=fee_id,
                    primary_fs=primary_fs,
                    fs_segment=fs_segment,
                    file_url=file_url,
                )
            except Exception as exc:
                print(f"    [WARN] Could not parse/upload Excel: {exc}")
        else:
            # TODO: future PDF parsing – for now just save locally
            print(f"    [INFO] PDF file – saved locally, DB parsing skipped.")

        # Record in tracker
        record_download(conn, section_heading, file_label, file_url,
                        fname, local_path,
                        fee_id=fee_id, primary_fs=primary_fs,
                        fs_segment=fs_segment, direct_link=direct_link)
        downloaded_count += 1

    print(f"  Summary: {downloaded_count} downloaded, {skipped_count} skipped.")

    # Close dropdown
    dd_button.click()


def main():
    print("Michigan MDHHS Fee Schedule Scraper (Excel-driven)")
    print("=" * 60)

    # 1. Load the master Excel to know what to download
    page_groups = load_master_excel(MASTER_EXCEL)
    if not page_groups:
        print("[!] No page groups found in master Excel – nothing to do.")
        return

    # Ensure download root exists
    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

    # Connect to SQL Server
    conn = get_db_connection()
    print("[+] Connected to SQL Server.")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(TIMEOUT)

        # 2. Navigate to MDHHS home (once)
        navigate_to_mdhhs(page)
        mdhhs_url = page.url  # remember so we can return here

        # 3. Process each page group from the master Excel
        for group in page_groups:
            fee_id = group["fee_id"]
            primary_fs = group["primary_fs"]
            allowed_segments = set(group["segments"])

            print(f"\n{'#'*60}")
            print(f"  Page group: {primary_fs}")
            print(f"  Fee ID: {fee_id}")
            print(f"  Segments to download: {sorted(allowed_segments)}")
            print(f"{'#'*60}")

            # Navigate: go back to MDHHS home, then search for the Primary FS
            page.goto(mdhhs_url, wait_until="domcontentloaded")
            page.wait_for_timeout(1000)
            navigate_to_page_via_search(page, primary_fs)

            # Discover all dropdown sections on the page
            all_sections = discover_sections(page)

            # Filter to only the FS Segments from the master Excel
            for sec in all_sections:
                if sec["heading"] not in allowed_segments:
                    print(f"\n  [SKIP] '{sec['heading']}' – not in master Excel segments.")
                    continue

                try:
                    scrape_section(
                        page, sec["heading"], sec["folder"],
                        conn, DOWNLOAD_ROOT,
                        fee_id=fee_id,
                        primary_fs=primary_fs,
                        fs_segment=sec["heading"],
                        direct_link=page.url,
                    )
                except Exception as exc:
                    print(f"  [ERR] Section '{sec['heading']}' failed: {exc}")

        browser.close()

    conn.close()
    print("\n" + "=" * 60)
    print("Done.")


def check_daily_status():
    """
    Quick daily check — queries the lightweight extraction_metadata table
    instead of scanning the large fee_schedule_data table.

    Prints a summary of:
      - Total files processed
      - Breakdown by status (success / empty / error)
      - Files extracted today
      - Total rows across all files
      - Any errors to investigate
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Overall counts
    cursor.execute(
        "SELECT extraction_status, COUNT(*) AS cnt, SUM(row_count) AS total_rows "
        "FROM dbo.extraction_metadata GROUP BY extraction_status"
    )
    rows = cursor.fetchall()
    print("\n" + "=" * 60)
    print("  DAILY STATUS CHECK  (from extraction_metadata)")
    print("=" * 60)
    grand_files = 0
    grand_rows = 0
    for status, cnt, total_rows in rows:
        print(f"  {status:>10}: {cnt} file(s), {total_rows or 0} row(s)")
        grand_files += cnt
        grand_rows += (total_rows or 0)
    print(f"  {'TOTAL':>10}: {grand_files} file(s), {grand_rows} row(s)")

    # Files extracted today
    cursor.execute(
        "SELECT fee_id, fs_segment, file_name, row_count, extraction_status "
        "FROM dbo.extraction_metadata "
        "WHERE CAST(extracted_at AS DATE) = CAST(SYSUTCDATETIME() AS DATE) "
        "ORDER BY extracted_at DESC"
    )
    today_rows = cursor.fetchall()
    if today_rows:
        print(f"\n  Files processed today: {len(today_rows)}")
        for fee_id, seg, fname, rc, st in today_rows:
            print(f"    [{st}] {fee_id} / {seg} / {fname}  ({rc} rows)")
    else:
        print("\n  No new files processed today.")

    # Recent errors
    cursor.execute(
        "SELECT TOP 10 fee_id, fs_segment, file_name, error_message, extracted_at "
        "FROM dbo.extraction_metadata "
        "WHERE extraction_status = 'error' "
        "ORDER BY extracted_at DESC"
    )
    errors = cursor.fetchall()
    if errors:
        print(f"\n  Recent errors ({len(errors)}):")
        for fee_id, seg, fname, err, ts in errors:
            print(f"    {ts} | {fee_id}/{seg}/{fname}")
            print(f"      → {err[:200] if err else 'unknown'}")

    print("=" * 60)
    conn.close()


def run_scheduler(interval_hours: int = 24):
    """Run main() immediately, then repeat every *interval_hours* hours."""
    import time
    from datetime import datetime as _dt

    while True:
        start = _dt.now()
        print(f"\n{'='*60}")
        print(f"  Scheduled run started at {start:%Y-%m-%d %H:%M:%S}")
        print(f"{'='*60}")
        try:
            main()
        except Exception as exc:
            print(f"[FATAL] Scraper failed: {exc}")

        # Print daily status after each run
        try:
            check_daily_status()
        except Exception as exc:
            print(f"[WARN] Status check failed: {exc}")

        elapsed = (_dt.now() - start).total_seconds()
        wait = max(0, interval_hours * 3600 - elapsed)
        next_run = _dt.now().timestamp() + wait
        print(f"\n[SCHEDULER] Next run at {_dt.fromtimestamp(next_run):%Y-%m-%d %H:%M:%S}")
        print(f"[SCHEDULER] Sleeping for {wait/3600:.1f} hours ...")
        time.sleep(wait)


if __name__ == "__main__":
    import sys
    if "--schedule" in sys.argv:
        run_scheduler(interval_hours=24)
    elif "--status" in sys.argv:
        check_daily_status()
    else:
        main()
