"""
Configuration for Michigan MDHHS Fee Schedule Scraper.
Update these settings to match your environment.
"""

# ─── Navigation ───
# Root URL where the scraper begins.
ROOT_URL = "https://www.michigan.gov/"

# First search term on michigan.gov to reach MDHHS home page.
SEARCH_TERM_1 = "mdhhs"

# ─── Master Excel ───
# Path to the CSRA_FeeID_Master.xlsx that drives the scraper.
# Columns: Fee ID | Primary FS | FS Segments
MASTER_EXCEL = r"C:\Users\hemas\Downloads\Fee Schedule\CSRA_FeeID_Master.xlsx"

# ─── Local download root ───
DOWNLOAD_ROOT = r"C:\Users\hemas\Downloads\Fee Schedule\downloads"

# ─── SQL Server connection ───
SQL_SERVER = "LAPTOP-OODE0JO9"         # e.g. "localhost\\SQLEXPRESS" or "myserver.database.windows.net"
SQL_DATABASE = "FeeScheduleDB"
SQL_DRIVER = "{ODBC Driver 17 for SQL Server}"
SQL_TRUSTED = True                 # Windows authentication
SQL_USERNAME = "LAPTOP-OODE0JO9\\hemas"              # only if SQL_TRUSTED=False
SQL_PASSWORD = "Hemasri@16"                  # only if SQL_TRUSTED = False

# ─── Playwright settings ───
HEADLESS = True                    # False if you want to watch the browser
TIMEOUT = 100_000                    # ms – page-level timeout
