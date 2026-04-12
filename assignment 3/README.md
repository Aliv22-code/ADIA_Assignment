# Assignment 3 — DC File Ingestion (Stock Loan Data Pipeline)

## Overview

Build a Python pipeline that processes stock loan data sent daily by prime brokers.
Each broker delivers a ZIP file containing CSV data with stock trading records.
The pipeline opens the ZIP, cleans the data, stores it in a SQLite table, and
returns the table name as a string.

**Function signature:**
```python
def load_data_file(filename: str) -> str
```
- **Input** — path to the broker's `.zip` file
- **Output** — name of the SQLite table where clean data is stored (`"stock_bars"`)

---

## Files

| File | Description |
|---|---|
| `solution.py` | Main pipeline — cleans data and loads into SQLite |
| `create_sample_zip.py` | Generates `dc-test-file-ingestion.zip` for local testing |
| `dc-test-file-ingestion.zip` | Sample ZIP with intentionally messy broker data |

---

## Data Schema

Each CSV file from brokers contains these columns:

| Column | Type | Description |
|---|---|---|
| `date` | TEXT | Trade date (various formats from brokers) |
| `stockid` | TEXT | Stock ticker symbol (e.g. AAPL, MSFT) |
| `broker` | TEXT | Prime broker name |
| `measure_one` | REAL | Primary numeric measure (e.g. quantity on loan) |
| `measure_two` | REAL | Secondary numeric measure (e.g. rate or fee) |

### SQLite Table: `stock_bars`

```sql
CREATE TABLE stock_bars (
    date        TEXT  NOT NULL,
    stockid     TEXT  NOT NULL,
    broker      TEXT  NOT NULL,
    measure_one REAL  NOT NULL,
    measure_two REAL,
    PRIMARY KEY (date, stockid, broker)
)
```

- `PRIMARY KEY (date, stockid, broker)` — prevents duplicate entries at the database level
- `measure_two` is nullable — some brokers omit it
- No raw/audit table in this version; cleaning happens in-memory before insert

---

## Data Cleaning Rules

| Problem | How It's Fixed |
|---|---|
| Inconsistent date formats | `parse_date()` tries 5 formats: `YYYY-MM-DD`, `DD/MM/YYYY`, `MM/DD/YYYY`, `DD-MM-YYYY`, `YYYYMMDD` |
| Non-numeric measures (`N/A`, `#REF!`, `$150`) | `parse_number()` strips symbols; returns `None` if unparseable |
| Missing `date` or `stockid` | Row is skipped with `[SKIP]` log |
| Duplicate rows | In-memory `seen_keys` set + `INSERT OR IGNORE` at DB level |
| Whitespace in column names | Headers normalised: strip + lowercase |
| Mixed-case broker names | Broker field lowercased |
| Mixed-case stockids | Stockid uppercased |
| Different CSV delimiters | `detect_delimiter()` checks `,` `\t` `\|` `;` |
| Encoding issues | Tries `utf-8-sig` first, falls back to `latin-1` |
| Corrupt/unreadable ZIP | Caught with `BadZipFile` exception |
| Hidden system files (`__MACOSX`) | Skipped by prefix check |

---

## Helper Functions

### `parse_date(value)`
Tries multiple date formats in order and returns a normalised `YYYY-MM-DD` string.
Returns `None` if no format matches — the row is then skipped.
```
"2024-01-15"     → "2024-01-15"  ✓
"15/01/2024"     → "2024-01-15"  ✓
"01/15/2024"     → "2024-01-15"  ✓
"20240115"       → "2024-01-15"  ✓
"BADDATE"        → None          → [SKIP]
```

### `parse_number(value)`
Strips non-numeric characters (`$`, `,`, `%`) and converts to float.
Returns `None` for values that cannot be converted.
```
"150.5"    → 150.5   ✓
"$1,500"   → 1500.0  ✓
"N/A"      → None    → [SKIP]
"#REF!"    → None    → [SKIP]
```

### `detect_delimiter(sample_line)`
Sniffs the first line of a CSV to determine the column separator.
```
"date,stockid,broker"    → ","
"date\tstockid\tbroker"  → "\t"
```

---

## How to Run

### 1. Generate the test ZIP
```bash
python3 create_sample_zip.py
```

### 2. Run the pipeline
```bash
python3 solution.py
```

### Expected output
```
[SKIP] ... row 4: bad measure_one 'N/A'
[SKIP] ... row 7: bad date 'January 17 2024'
[SKIP] ... row 8: missing stockid
[DUP]  ... row 13: duplicate ('2024-01-15', 'AAPL', 'goldman sachs')
...
[INFO] Successfully loaded 11 rows into 'stock_bars'
Data is stored in table: 'stock_bars'
```

---

## Real-World Pipeline Design

### Infrastructure
- **Scheduler** — Apache Airflow or AWS Step Functions triggers the job daily per broker
- **Storage** — Incoming ZIPs land in AWS S3; processed data goes to PostgreSQL or Snowflake
- **Monitoring** — Row count anomaly alerts via PagerDuty or email
- **Logging** — Every skipped row is logged with reason for audit purposes

### Real-World Scenarios & Handling

| Scenario | Detection | Handling |
|---|---|---|
| Late / missing file | Check arrival by deadline | Alert ops; backfill or mark dates as estimated |
| Broker resends corrected data | Hash file on arrival; compare to history | Upsert logic with `INSERT OR REPLACE` |
| Schema change (new/renamed column) | Compare headers against expected schema | Log schema-drift alert; use column mapping config |
| Corrupt / bad ZIP | `BadZipFile` exception | Quarantine file; alert ops team |
| Encoding issues | `UnicodeDecodeError` | Try `utf-8-sig` → `latin-1` → `cp1252` |
| Very large files | Memory error | Stream CSV in chunks via `csv.reader` |
| Multiple file formats in one ZIP | File extension check | Dispatch `.csv` vs `.xlsx` to separate parsers |

---

## Requirements

- Python 3.6+
- Standard library only (`csv`, `io`, `re`, `sqlite3`, `zipfile`, `os`, `datetime`) — no installs needed
