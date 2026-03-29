"""
db_setup.py
-----------
Creates the SQL Server database (if needed) and the two tracking / data tables.

Tables
------
1. downloaded_files   – tracks every file ever downloaded (prevents re-download)
2. fee_schedule_data  – stores the parsed Excel rows

Run once:  python db_setup.py
"""

import pyodbc
from config import SQL_SERVER, SQL_DATABASE, SQL_DRIVER, SQL_TRUSTED, SQL_USERNAME, SQL_PASSWORD


def _conn_str(database: str = "master") -> str:
    base = f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={database};"
    if SQL_TRUSTED:
        base += "Trusted_Connection=yes;"
    else:
        base += f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
    return base


def create_database():
    conn = pyodbc.connect(_conn_str("master"), autocommit=True)
    cursor = conn.cursor()
    cursor.execute(
        f"IF DB_ID(?) IS NULL CREATE DATABASE [{SQL_DATABASE}]", SQL_DATABASE
    )
    conn.close()
    print(f"[+] Database '{SQL_DATABASE}' ensured.")


def create_tables():
    conn = pyodbc.connect(_conn_str(SQL_DATABASE), autocommit=True)
    cursor = conn.cursor()

    # Table 1 – Downloaded files tracker (with master Excel metadata)
    cursor.execute("""
        IF OBJECT_ID('dbo.downloaded_files', 'U') IS NULL
        CREATE TABLE dbo.downloaded_files (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            section_name    NVARCHAR(200)   NOT NULL,
            file_label      NVARCHAR(200)   NOT NULL,
            file_url        NVARCHAR(1000)  NOT NULL,
            file_name       NVARCHAR(500)   NOT NULL,
            local_path      NVARCHAR(1000)  NOT NULL,
            fee_id          NVARCHAR(50)    NULL,
            primary_fs      NVARCHAR(500)   NULL,
            fs_segment      NVARCHAR(500)   NULL,
            direct_link     NVARCHAR(1000)  NULL,
            downloaded_at   DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_downloaded_files UNIQUE (fee_id, primary_fs, fs_segment, file_url)
        );
    """)

    # Table 2 – Extraction metadata (lightweight daily-check table)
    cursor.execute("""
        IF OBJECT_ID('dbo.extraction_metadata', 'U') IS NULL
        CREATE TABLE dbo.extraction_metadata (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            fee_id          NVARCHAR(50)    NOT NULL,
            primary_fs      NVARCHAR(500)   NOT NULL,
            fs_segment      NVARCHAR(500)   NOT NULL,
            file_url        NVARCHAR(1000)  NOT NULL,
            file_name       NVARCHAR(500)   NOT NULL,
            sheet_count      INT            NULL,
            table_count      INT            NULL,
            row_count        INT            NOT NULL DEFAULT 0,
            extraction_status NVARCHAR(20)  NOT NULL DEFAULT 'success',
            error_message    NVARCHAR(MAX)  NULL,
            file_size_bytes  BIGINT         NULL,
            extracted_at     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_extraction_meta UNIQUE (fee_id, primary_fs, fs_segment, file_url)
        );
    """)

    # Table 3 – Extracted fee-schedule data
    cursor.execute("""
        IF OBJECT_ID('dbo.fee_schedule_data', 'U') IS NULL
        CREATE TABLE dbo.fee_schedule_data (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            section_name    NVARCHAR(200)   NOT NULL,
            file_name       NVARCHAR(500)   NOT NULL,
            sheet_name      NVARCHAR(200)   NULL,
            row_number      INT             NOT NULL,
            row_data        NVARCHAR(MAX)   NOT NULL,   -- JSON-serialised row
            fee_id          NVARCHAR(50)    NULL,
            primary_fs      NVARCHAR(500)   NULL,
            fs_segment      NVARCHAR(500)   NULL,
            file_url        NVARCHAR(1000)  NULL,
            table_index     INT             NULL,
            source_row_number INT           NULL,
            code_type       NVARCHAR(20)    NULL,
            code_value      NVARCHAR(50)    NULL,
            short_description NVARCHAR(1000) NULL,
            modifier        NVARCHAR(200)   NULL,
            age_range       NVARCHAR(100)   NULL,
            non_fac_fee     DECIMAL(18,4)   NULL,
            fac_fee         DECIMAL(18,4)   NULL,
            rate            DECIMAL(18,4)   NULL,
            effective_date_text NVARCHAR(100) NULL,
            comments        NVARCHAR(MAX)   NULL,
            extra_fields    NVARCHAR(MAX)   NULL,
            loaded_at       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
        );
    """)

    conn.close()
    print("[+] Tables 'downloaded_files', 'extraction_metadata', and 'fee_schedule_data' ensured.")


def migrate_unique_constraint():
    """Drop the old UNIQUE constraint and create one on (fee_id, primary_fs, fs_segment, file_url)."""
    conn = pyodbc.connect(_conn_str(SQL_DATABASE), autocommit=True)
    cursor = conn.cursor()

    # Find and drop the existing constraint named uq_downloaded_files (if any)
    cursor.execute("""
        IF EXISTS (
            SELECT 1 FROM sys.key_constraints
            WHERE name = 'uq_downloaded_files'
              AND parent_object_id = OBJECT_ID('dbo.downloaded_files')
        )
        ALTER TABLE dbo.downloaded_files DROP CONSTRAINT uq_downloaded_files;
    """)

    # Make sure columns exist (in case table was created before metadata columns were added)
    for col, dtype in [("fee_id", "NVARCHAR(50)"),
                       ("primary_fs", "NVARCHAR(500)"),
                       ("fs_segment", "NVARCHAR(500)")]:
        cursor.execute(f"""
            IF COL_LENGTH('dbo.downloaded_files', '{col}') IS NULL
            ALTER TABLE dbo.downloaded_files ADD {col} {dtype} NULL;
        """)

    # Create the new constraint
    cursor.execute("""
        ALTER TABLE dbo.downloaded_files
        ADD CONSTRAINT uq_downloaded_files
        UNIQUE (fee_id, primary_fs, fs_segment, file_url);
    """)

    conn.close()
    print("[+] Migrated unique constraint to (fee_id, primary_fs, fs_segment, file_url).")


def migrate_fee_schedule_data_schema():
    """Add structured extraction columns to fee_schedule_data when missing."""
    conn = pyodbc.connect(_conn_str(SQL_DATABASE), autocommit=True)
    cursor = conn.cursor()

    for column_name, data_type in [
        ("fee_id", "NVARCHAR(50) NULL"),
        ("primary_fs", "NVARCHAR(500) NULL"),
        ("fs_segment", "NVARCHAR(500) NULL"),
        ("file_url", "NVARCHAR(1000) NULL"),
        ("table_index", "INT NULL"),
        ("source_row_number", "INT NULL"),
        ("code_type", "NVARCHAR(20) NULL"),
        ("code_value", "NVARCHAR(50) NULL"),
        ("short_description", "NVARCHAR(1000) NULL"),
        ("modifier", "NVARCHAR(200) NULL"),
        ("age_range", "NVARCHAR(100) NULL"),
        ("non_fac_fee", "DECIMAL(18,4) NULL"),
        ("fac_fee", "DECIMAL(18,4) NULL"),
        ("rate", "DECIMAL(18,4) NULL"),
        ("effective_date_text", "NVARCHAR(100) NULL"),
        ("comments", "NVARCHAR(MAX) NULL"),
        ("extra_fields", "NVARCHAR(MAX) NULL"),
    ]:
        cursor.execute(f"""
            IF COL_LENGTH('dbo.fee_schedule_data', '{column_name}') IS NULL
            ALTER TABLE dbo.fee_schedule_data ADD {column_name} {data_type};
        """)

    conn.close()
    print("[+] Migrated fee_schedule_data to structured extraction schema.")


def migrate_extraction_metadata():
    """Create extraction_metadata table if it doesn't exist (for existing DBs)."""
    conn = pyodbc.connect(_conn_str(SQL_DATABASE), autocommit=True)
    cursor = conn.cursor()
    cursor.execute("""
        IF OBJECT_ID('dbo.extraction_metadata', 'U') IS NULL
        CREATE TABLE dbo.extraction_metadata (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            fee_id          NVARCHAR(50)    NOT NULL,
            primary_fs      NVARCHAR(500)   NOT NULL,
            fs_segment      NVARCHAR(500)   NOT NULL,
            file_url        NVARCHAR(1000)  NOT NULL,
            file_name       NVARCHAR(500)   NOT NULL,
            sheet_count      INT            NULL,
            table_count      INT            NULL,
            row_count        INT            NOT NULL DEFAULT 0,
            extraction_status NVARCHAR(20)  NOT NULL DEFAULT 'success',
            error_message    NVARCHAR(MAX)  NULL,
            file_size_bytes  BIGINT         NULL,
            extracted_at     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_extraction_meta UNIQUE (fee_id, primary_fs, fs_segment, file_url)
        );
    """)
    conn.close()
    print("[+] Table 'extraction_metadata' ensured.")


if __name__ == "__main__":
    create_database()
    create_tables()
    migrate_unique_constraint()
    migrate_fee_schedule_data_schema()
    migrate_extraction_metadata()
    print("[✓] Database setup complete.")
