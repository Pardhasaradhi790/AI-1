# Michigan MDHHS Fee Schedule Scraper

Automated Playwright scraper that downloads fee-schedule Excel files from the
[Michigan MDHHS Physicians/Practitioners/Medical Clinics](https://www.michigan.gov/mdhhs/doing-business/providers/providers/billingreimbursement/physicians-practitioners-medical-clinics)
page and uploads the extracted data to SQL Server.

---

## Features

| Feature | Detail |
|---------|--------|
| **7 sections scraped** | Anesthesia, Oral/Maxillofacial Surgeon, Physician Primary Care Rate Increase, Telemedicine, Certified Nurse Midwife, Podiatry, Practitioner |
| **Excel preferred** | When both PDF and Excel exist for the same quarter, only the Excel is downloaded |
| **Incremental** | `downloaded_files` table prevents re-downloading files already stored |
| **Auto-folder** | Each section saves to its own subfolder (e.g. `downloads/telemedicine/`) |
| **SQL upload** | Every row from every sheet is inserted as JSON into `fee_schedule_data` |

---

## SQL Server Tables

### `downloaded_files` (tracker)
| Column | Type | Purpose |
|--------|------|---------|
| section_name | NVARCHAR(200) | e.g. "Telemedicine" |
| file_label | NVARCHAR(200) | Dropdown text, e.g. "Jan 2023 XLSX" |
| file_url | NVARCHAR(1000) | Full download URL |
| file_name | NVARCHAR(500) | Local filename |
| local_path | NVARCHAR(1000) | Full local path |
| downloaded_at | DATETIME2 | UTC timestamp |

### `fee_schedule_data` (extracted rows)
| Column | Type | Purpose |
|--------|------|---------|
| section_name | NVARCHAR(200) | Section the file belongs to |
| file_name | NVARCHAR(500) | Source filename |
| sheet_name | NVARCHAR(200) | Excel sheet name |
| row_number | INT | 1-based row index |
| row_data | NVARCHAR(MAX) | JSON-serialised row |
| loaded_at | DATETIME2 | UTC timestamp |

---

## Setup

### 1. Install Python dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure
Edit **config.py**:
- `SQL_SERVER` – your SQL Server instance name
- `SQL_DATABASE` – database name (default `FeeScheduleDB`)
- `SQL_TRUSTED` – set `False` and fill `SQL_USERNAME` / `SQL_PASSWORD` if not using Windows auth
- `DOWNLOAD_ROOT` – where files are saved locally
- `HEADLESS` – set `False` to watch the browser

### 3. Create the database & tables
```bash
python db_setup.py
```

### 4. Run the scraper
```bash
python scraper.py
```

---

## How It Works

1. Opens the page in Chromium via Playwright.
2. For each of the 7 sections, locates the `<h3>` heading and its dropdown.
3. Collects all `<li>` entries with `data-link` attributes.
4. Groups entries by quarter date; prefers `.xlsx`/`.xls` over `.pdf`.
5. Picks the **latest** (most recent) file.
6. Queries `downloaded_files` — **skips** if the URL is already recorded.
7. Downloads the file via `page.request.get()` into `downloads/<section>/`.
8. Reads all sheets with pandas and inserts each row as JSON into `fee_schedule_data`.
9. Records the download in `downloaded_files`.

---

## Folder Structure After Running
```
Fee Schedule/
├── config.py
├── db_setup.py
├── scraper.py
├── requirements.txt
├── README.md
├── docs/
│   └── webpage.html
└── downloads/
    ├── anesthesia/
    │   └── Anesthesia-012026.xlsx
    ├── oral_maxillofacial_surgeon/
    │   └── Oral-Max-January.xlsx
    ├── physician_primary_care_rate_increase/
    │   └── Primary-Care-Incentive---012026.xlsx
    ├── telemedicine/
    │   └── Telemedicine-012023-XLSX.xlsx
    ├── certified_nurse_midwife/
    │   └── CNM-012026.xlsx
    ├── podiatry/
    │   └── Podiatrist-012026.xlsx
    └── practitioner/
        └── Practitioner--012026.xlsx
```
"""
Generate CSRA_FeeID_Master.xlsx from the master tracking data.
Run once:  python create_master_excel.py
"""

import pandas as pd

# (Fee ID, Primary FS, FS Segment)
rows = [
    # ── DZ00006245 ──
    ("DZ00006245", "Physicians/Practitioners/Medical Clinics", "Anesthesia"),
    ("", "Physicians/Practitioners/Medical Clinics", "Certified Nurse Midwife"),
    ("", "Physicians/Practitioners/Medical Clinics", "Oral/Maxillofacial Surgeon"),
    ("", "Physicians/Practitioners/Medical Clinics", "Podiatry"),
    ("", "Physicians/Practitioners/Medical Clinics", "Physician Primary Care Rate Increase"),
    ("", "Physicians/Practitioners/Medical Clinics", "Practitioner"),
    ("", "Telemedicine", "Telemedicine Audio-Only"),
    ("", "Telemedicine", "Telemedicine Audio-Visual"),
    # ── CZ00034402 ──
    ("CZ00034402", "Chiropractor", "Chiropractor Fee Databases"),
    ("", "Clinical Laboratory", "Clinical Laboratory Fee Databases"),
    ("", "Medical Suppliers / Orthotists / Prosthetists / DME Dealers", "DMEPOS Database"),
    ("", "Family Planning", "Title X Family Planning Clinics"),
    ("", "Behavioral Health/Substance Abuse", "PIHP/CMHSP Physician Injectable Drugs Carve-Out"),
    ("", "Behavioral Health/Substance Abuse", ""),
    ("", "Behavioral Health/Substance Abuse", "Serious Emotional Disturbance (SED)"),
    ("", "Behavioral Health/Substance Abuse", "Non-Physician Behavioral Health"),
    ("", "Behavioral Health/Substance Abuse", "Children's Waiver Program"),
    ("", "Behavioral Health/Substance Abuse", "Targeted Case Management - Flint Waiver"),
    ("", "Behavioral Health/Substance Abuse", "Applied Behavior Analysis"),
    ("", "Clinic Institutional Billing", "Federally Qualified Health Center (FQHC)"),
    ("", "Clinic Institutional Billing", "Rural Health Clinic (RHC)"),
    ("", "Clinic Institutional Billing", "Tribal Health Center (THC)"),
    ("", "Urgent Care Centers", "Urgent Care Center Fee Databases"),
    ("", "Vision", "Vision Fee Database"),
    ("", "Maternal Infant Health Program", "Maternal Infant Health"),
    ("", "Local Health Department", "Local Health Department Fee Databases"),
    ("", "Hearing Services and Devices", "Hearing Aid Dealers Database"),
    ("", "Hearing Services and Devices", "Hearing Services Fee Databases"),
    ("", "Therapies", "Physical Therapy Fee Databases"),
    ("", "Therapies", "Occupational Therapy Fee Databases"),
    ("", "Therapies", "Speech Therapy Fee Databases"),
    # ── CZ00046142 ──
    ("CZ00046142", "Physicians/Practitioners/Medical Clinics", "Anesthesia"),
    ("", "Physicians/Practitioners/Medical Clinics", "Certified Nurse Midwife"),
    ("", "Physicians/Practitioners/Medical Clinics", "Oral/Maxillofacial Surgeon"),
    ("", "Physicians/Practitioners/Medical Clinics", "Podiatry"),
    ("", "Physicians/Practitioners/Medical Clinics", "Physician Primary Care Rate Increase"),
    ("", "Physicians/Practitioners/Medical Clinics", "Practitioner"),
    ("", "Telemedicine", "Telemedicine Audio-Only"),
    ("", "Telemedicine", "Telemedicine Audio-Visual"),
]

df = pd.DataFrame(rows, columns=["Fee ID", "Primary FS", "FS Segments"])

output = r"C:\Users\hemas\Downloads\Fee Schedule\CSRA_FeeID_Master.xlsx"

with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    df.to_excel(writer, sheet_name="MI", index=False)
    workbook = writer.book
    worksheet = writer.sheets["MI"]

    header_fmt = workbook.add_format({
        "bold": True, "font_color": "white", "bg_color": "#4472C4",
        "border": 1, "align": "center", "valign": "vcenter",
    })
    cell_fmt = workbook.add_format({"border": 1, "text_wrap": True, "valign": "vcenter"})

    for col_num, col_name in enumerate(df.columns):
        worksheet.write(0, col_num, col_name, header_fmt)
    for r_idx in range(len(df)):
        for c_idx in range(len(df.columns)):
            worksheet.write(r_idx + 1, c_idx, df.iloc[r_idx, c_idx], cell_fmt)

    worksheet.set_column("A:A", 16)
    worksheet.set_column("B:B", 52)
    worksheet.set_column("C:C", 50)

print(f"Created: {output}")

