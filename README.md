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
