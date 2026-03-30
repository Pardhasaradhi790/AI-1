# SQL Test Queries — Fee Schedule Database

Use these queries in **SQL Server Management Studio** against `FeeScheduleDB` to verify downloads and extracted data.

---

## 1. Check All Downloaded Files

```sql
-- List every file that was downloaded
SELECT
    fee_id,
    primary_fs,
    fs_segment,
    file_name,
    file_label,
    file_url,
    downloaded_at
FROM dbo.downloaded_files
ORDER BY downloaded_at DESC;
```

---

## 2. Count Downloads Per Fee ID

```sql
-- How many files were downloaded per Fee ID
SELECT
    fee_id,
    COUNT(*) AS total_files
FROM dbo.downloaded_files
GROUP BY fee_id
ORDER BY fee_id;
```

---

## 3. Count Downloads Per FS Segment

```sql
-- How many files per segment
SELECT
    fee_id,
    primary_fs,
    fs_segment,
    COUNT(*) AS file_count
FROM dbo.downloaded_files
GROUP BY fee_id, primary_fs, fs_segment
ORDER BY fee_id, primary_fs, fs_segment;
```

---

## 4. Check If a Specific File Was Downloaded

```sql
-- Replace 'Anesthesia' with the segment you want to check
SELECT *
FROM dbo.downloaded_files
WHERE fs_segment = 'Anesthesia';
```

---

## 5. Check Extraction Metadata (Summary Per File)

```sql
-- Overview of every file's extraction result
SELECT
    fee_id,
    primary_fs,
    fs_segment,
    file_name,
    sheet_count,
    table_count,
    row_count,
    extraction_status,
    error_message,
    file_size_bytes,
    extracted_at
FROM dbo.extraction_metadata
ORDER BY extracted_at DESC;
```

---

## 6. Check Extraction Status Totals

```sql
-- How many files succeeded, failed, or were empty
SELECT
    extraction_status,
    COUNT(*)        AS file_count,
    SUM(row_count)  AS total_rows
FROM dbo.extraction_metadata
GROUP BY extraction_status;
```

---

## 7. Find Extraction Errors

```sql
-- List files that failed to extract
SELECT
    fee_id,
    fs_segment,
    file_name,
    error_message,
    extracted_at
FROM dbo.extraction_metadata
WHERE extraction_status = 'error'
ORDER BY extracted_at DESC;
```

---

## 8. Find Empty Extractions (File Downloaded But No Rows)

```sql
-- Files where no rows were extracted
SELECT
    fee_id,
    fs_segment,
    file_name,
    extraction_status,
    extracted_at
FROM dbo.extraction_metadata
WHERE extraction_status = 'empty';
```

---

## 9. View Extracted Data Rows

```sql
-- See the first 100 extracted rows across all files
SELECT TOP 100
    fee_id,
    primary_fs,
    fs_segment,
    file_name,
    sheet_name,
    code_type,
    code_value,
    short_description,
    modifier,
    age_range,
    non_fac_fee,
    fac_fee,
    rate,
    effective_date_text,
    comments,
    loaded_at
FROM dbo.fee_schedule_data
ORDER BY loaded_at DESC;
```

---

## 10. Count Extracted Rows Per Segment

```sql
-- Row count per fee segment in fee_schedule_data
SELECT
    fee_id,
    primary_fs,
    fs_segment,
    COUNT(*) AS row_count
FROM dbo.fee_schedule_data
GROUP BY fee_id, primary_fs, fs_segment
ORDER BY fee_id, primary_fs, fs_segment;
```

---

## 11. Search by Code Value

```sql
-- Find a specific procedure code
SELECT
    fee_id,
    fs_segment,
    code_type,
    code_value,
    short_description,
    non_fac_fee,
    fac_fee,
    rate,
    file_name
FROM dbo.fee_schedule_data
WHERE code_value = '90791';  -- replace with the code you want
```

---

## 12. Search by Description Keyword

```sql
-- Find rows matching a keyword in description
SELECT
    fee_id,
    fs_segment,
    code_value,
    short_description,
    non_fac_fee,
    fac_fee,
    rate
FROM dbo.fee_schedule_data
WHERE short_description LIKE '%telemedicine%';  -- replace keyword
```

---

## 13. View Raw JSON Row Data

```sql
-- See the original raw row as JSON for a specific file
SELECT TOP 20
    code_value,
    short_description,
    row_data,
    extra_fields
FROM dbo.fee_schedule_data
WHERE file_name = 'Anesthesia-012026.xlsx';  -- replace with actual filename
```

---

## 14. Find Rows With Extra/Custom Columns

```sql
-- Rows where extra non-standard columns were captured
SELECT
    fee_id,
    fs_segment,
    code_value,
    short_description,
    extra_fields
FROM dbo.fee_schedule_data
WHERE extra_fields != '{}' AND extra_fields IS NOT NULL
ORDER BY fee_id;
```

---

## 15. Files Processed Today

```sql
-- Quick daily check — files extracted today
SELECT
    fee_id,
    fs_segment,
    file_name,
    row_count,
    extraction_status,
    extracted_at
FROM dbo.extraction_metadata
WHERE CAST(extracted_at AS DATE) = CAST(SYSUTCDATETIME() AS DATE)
ORDER BY extracted_at DESC;
```

---

## 16. Verify No Duplicates in downloaded_files

```sql
-- Should return 0 rows if deduplication is working correctly
SELECT
    fee_id,
    primary_fs,
    fs_segment,
    file_url,
    COUNT(*) AS cnt
FROM dbo.downloaded_files
GROUP BY fee_id, primary_fs, fs_segment, file_url
HAVING COUNT(*) > 1;
```

---

## 17. Full Health Check in One Query

```sql
-- Single snapshot of the entire pipeline state
SELECT
    'downloaded_files'   AS table_name, COUNT(*) AS total_records FROM dbo.downloaded_files
UNION ALL
SELECT
    'extraction_metadata', COUNT(*) FROM dbo.extraction_metadata
UNION ALL
SELECT
    'fee_schedule_data',   COUNT(*) FROM dbo.fee_schedule_data;
```

---

## Quick Reference

| What to check | Query # |
|---|---|
| All downloaded files | 1 |
| Downloads per Fee ID | 2 |
| Downloads per segment | 3 |
| Specific segment files | 4 |
| Extraction summary | 5, 6 |
| Extraction errors | 7 |
| Empty extractions | 8 |
| Extracted rows | 9 |
| Rows per segment | 10 |
| Search by code | 11 |
| Search by keyword | 12 |
| Raw JSON data | 13 |
| Custom columns | 14 |
| Today's runs | 15 |
| Duplicate check | 16 |
| Full health check | 17 |
