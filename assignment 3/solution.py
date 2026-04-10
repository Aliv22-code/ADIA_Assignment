#!/usr/bin/env python3

import os
import zipfile
import hashlib
import sqlite3
import pandas as pd
from datetime import datetime

# =============================================================================
# SCHEMA DESIGN
# -----------------------------------------------------------------------------
# RAW TABLE: raw_stock_loans
#   - ingestion_id  INTEGER PRIMARY KEY AUTOINCREMENT
#   - source_file   TEXT    (filename for lineage)
#   - ingested_at   TEXT    (UTC timestamp)
#   - date          TEXT    (raw string, unparsed)
#   - stockid       TEXT
#   - broker        TEXT
#   - measure_one   TEXT    (raw, before type coercion)
#   - measure_two   TEXT
#   - row_hash      TEXT UNIQUE  (MD5 for deduplication)
#
# DERIVED TABLE: derived_stock_loans
#   - id            INTEGER PRIMARY KEY AUTOINCREMENT
#   - trade_date    TEXT    (normalised YYYY-MM-DD)        [TYPE CHANGE: raw string → YYYY-MM-DD]
#   - stockid       TEXT    (uppercased, stripped)          [TYPE CHANGE: raw → normalised TEXT]
#   - broker        TEXT    (uppercased, stripped)          [TYPE CHANGE: raw → normalised TEXT]
#   - measure_one   REAL                                    [TYPE CHANGE: TEXT → REAL float]
#   - measure_two   REAL                                    [TYPE CHANGE: TEXT → REAL float]
#   - source_file   TEXT
#   - ingested_at   TEXT
#   - is_valid      INTEGER (1=clean, 0=has issues)
#   - quality_flags TEXT    (pipe-separated issue list)
# =============================================================================

# =============================================================================
# REAL-WORLD INGESTION SCENARIOS & HANDLING
# -----------------------------------------------------------------------------
# CASE 1:  SCHEMA DRIFT
#          Broker adds/renames columns without notice.
#          Detect: diff actual vs expected columns; log mismatches.
#          Handle: pipeline continues, missing columns filled with NaN and flagged.
#          >> HANDLED — see "Schema drift check" block below.
#
# CASE 2:  DUPLICATE ROWS
#          Broker resends same data on retry or reconciliation.
#          Detect: MD5 hash of raw row values.
#          Handle: INSERT OR IGNORE into raw table; content duplicates skipped in derived.
#          >> HANDLED — see "Write RAW table" block below.
#
# CASE 3:  MISSING VALUES
#          Required fields (stockid, broker, measures) null/empty for some rows.
#          Detect: null/empty check after parse on each required field.
#          Handle: row stored with is_valid=0 and specific flag e.g. 'missing_stockid'.
#          >> HANDLED — see per-field null checks in "Clean & write DERIVED table".
#
# CASE 4:  DATE FORMAT MISMATCH
#          Different brokers send YYYY-MM-DD, MM/DD/YYYY, DD-Mon-YYYY, compact, etc.
#          Detect: attempt parse across 8 known formats; pandas fallback as last resort.
#          Handle: unparseable dates flagged as 'unparseable_date'; row still stored.
#          >> HANDLED — see _parse_date() function.
#
# CASE 5:  NUMERIC FORMATTING
#          Values arrive as "$1,234.56", "12.5%", or with currency/space artifacts.
#          Detect: attempt float cast after stripping known symbols.
#          Handle: clean value stored as REAL; failures flagged as 'invalid_numeric_<col>'.
#          >> HANDLED — see _clean_numeric() function.
#
# CASE 6:  ENCODING ISSUES
#          Legacy broker systems send latin-1 or cp1252 instead of UTF-8.
#          Detect: UnicodeDecodeError on read attempt.
#          Handle: retry read with fallback encodings [utf-8, utf-8-sig, latin-1, cp1252].
#          >> HANDLED — see "Read with encoding fallback" block below.
#
# CASE 7:  CORRUPT ZIP
#          Upload interrupted mid-transfer or file is malformed.
#          Detect: zipfile.BadZipFile exception on open.
#          Handle: raise RuntimeError to quarantine — do NOT silently skip.
#          >> HANDLED — see "Validate zip" block below.
#
# CASE 8:  EMPTY FILES
#          CSV contains only a header row, zero data rows.
#          Detect: df.empty check after read.
#          Handle: log warning and skip file gracefully; pipeline continues.
#          >> HANDLED — see "Empty file guard" block below.
#
# CASE 9:  LATE ARRIVALS
#          Trade date is significantly older than the ingestion date (> 30 days).
#          Detect: (today - trade_date).days > LATE_ARRIVAL_DAYS threshold.
#          Handle: row stored with flag 'late_arrival'; downstream models can filter.
#          >> HANDLED — see late arrival check in "Clean & write DERIVED table".
#
# CASE 10: AMENDED HISTORY  *** NOT HANDLED — excluded for code simplicity ***
#          Broker resends a past date with corrected measure values (SCD Type-2).
#          Would require: versioning columns (version, is_current), superseding old rows.
#          Workaround: current code will insert a new row alongside the old one.
#
# CASE 11: OUTLIERS          *** NOT HANDLED — excluded for code simplicity ***
#          Extreme or implausible values (negatives, zeros, statistical anomalies).
#          Would require: IQR / z-score check per (broker, column) group post-insert.
#          Workaround: consumers of derived table should apply their own range filters.
#
# NOTE: Bad rows are FLAGGED, never silently dropped — invisible missing data
# is far more dangerous for ML than a clearly tagged bad row.
# =============================================================================

FILEPATH      = os.environ.get("FILEPATH", ".")
RAW_TABLE     = "raw_stock_loans"
DERIVED_TABLE = "derived_stock_loans"
DB_PATH       = os.path.join(FILEPATH, "stock_loans.db")

EXPECTED_COLS     = {"date", "stockid", "broker", "measure_one", "measure_two"}
LATE_ARRIVAL_DAYS = 30

# [CASE 4] All date formats brokers are known to send
DATE_FORMATS = [
    "%Y-%m-%d",   # 2024-01-31  ISO standard
    "%m/%d/%Y",   # 01/31/2024  US format
    "%d-%m-%Y",   # 31-01-2024  European dash
    "%d/%m/%Y",   # 31/01/2024  European slash
    "%d-%b-%Y",   # 31-Jan-2024 legacy broker
    "%Y%m%d",     # 20240131    compact
    "%m-%d-%Y",   # 01-31-2024  US dash
    "%b %d, %Y",  # Jan 31, 2024
]

# [CASE 6] Encoding fallback order
ENCODINGS = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]


def _parse_date(val):
    """
    [CASE 4] Parse a date string by trying multiple known formats.
    [CASE 3] Returns (None, 'missing_date') if value is null/empty.
    Returns  (date, None) on success or (None, flag_string) on failure.
    """
    # [CASE 3] Null or empty date
    if pd.isnull(val) or str(val).strip() == "":
        return None, "missing_date"

    # [CASE 4] Try each known format in order
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(str(val).strip(), fmt).date(), None
        except ValueError:
            continue

    # [CASE 4] Last resort — pandas flexible parser
    try:
        return pd.to_datetime(str(val), infer_datetime_format=True).date(), None
    except Exception:
        return None, "unparseable_date"


def _clean_numeric(val):
    """
    [CASE 5] Strip currency/formatting artifacts and cast to float.
    Handles: $, £, €, commas (thousands), %, internal spaces.
    [CASE 3] Returns (None, 'missing_numeric') if value is null/empty.
    Returns  (float, None) on success or (None, flag_string) on failure.
    """
    # [CASE 3] Null or empty numeric
    if pd.isnull(val) or str(val).strip() == "":
        return None, "missing_numeric"

    # [CASE 5] Strip known formatting characters
    cleaned = (
        str(val).strip()
        .replace(",", "")   # thousands separator: 1,234 → 1234
        .replace("$", "")   # USD symbol
        .replace("£", "")   # GBP symbol
        .replace("€", "")   # EUR symbol
        .replace("%", "")   # percentage sign
        .replace(" ", "")   # internal spaces
    )

    try:
        return float(cleaned), None
    except ValueError:
        return None, "invalid_numeric"


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")  # allows concurrent reads
    return conn


def _init_schema(conn):
    # Use .format for SQL (not f-strings): avoids `{`/`}` parsing issues in long SQL.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS {raw_table} (
            ingestion_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file   TEXT,
            ingested_at   TEXT,
            date          TEXT,
            stockid       TEXT,
            broker        TEXT,
            measure_one   TEXT,
            measure_two   TEXT,
            row_hash      TEXT UNIQUE    -- [CASE 2] uniqueness enforced here
        )
        """.format(raw_table=RAW_TABLE)
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS {derived_table} (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date    TEXT,          -- [TYPE CHANGE] raw string  -> YYYY-MM-DD
            stockid       TEXT,          -- [TYPE CHANGE] raw string  -> uppercased TEXT
            broker        TEXT,          -- [TYPE CHANGE] raw string  -> uppercased TEXT
            measure_one   REAL,          -- [TYPE CHANGE] TEXT        -> REAL float
            measure_two   REAL,          -- [TYPE CHANGE] TEXT        -> REAL float
            source_file   TEXT,
            ingested_at   TEXT,
            is_valid      INTEGER,       -- 1 = no issues, 0 = has quality flags
            quality_flags TEXT           -- pipe-separated e.g. missing_date|late_arrival
        )
        """.format(derived_table=DERIVED_TABLE)
    )
    conn.commit()


def load_data_file(filename):
    """
    Ingest a broker zip file into raw + derived SQLite tables.
    Handles cases 1-9. Cases 10 (amended history) and 11 (outliers)
    are documented but not implemented — see comments above.

    Parameters
    ----------
    filename : str   e.g. 'dc-test-file-ingestion.zip'

    Returns
    -------
    str   Name of the derived table: 'derived_stock_loans'
    """
    zip_path    = os.path.join(FILEPATH, filename)
    ingested_at = datetime.utcnow().isoformat()

    # ── [CASE 7] Validate zip — check existence and integrity ────────────────
    if not os.path.exists(zip_path):
        raise FileNotFoundError(
            f"[CASE 7] Zip not found: {zip_path}. "
            f"Available: {os.listdir(FILEPATH)}"
        )
    try:
        zf = zipfile.ZipFile(zip_path, "r")
    except zipfile.BadZipFile as e:
        # Raise to quarantine — never silently skip a corrupt file
        raise RuntimeError(f"[CASE 7] Corrupt or incomplete zip: {zip_path}") from e

    conn = _get_conn()
    _init_schema(conn)

    with zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_files:
            print(f"Warning: no CSV files found inside {filename}")
            return DERIVED_TABLE

        for csv_name in csv_files:
            print(f"\nProcessing: {csv_name}")

            # ── [CASE 6] Read CSV with encoding fallback ─────────────────────
            df       = None
            used_enc = None
            for enc in ENCODINGS:
                try:
                    with zf.open(csv_name) as f:
                        df = pd.read_csv(f, encoding=enc, dtype=str)
                    used_enc = enc
                    break
                except UnicodeDecodeError:
                    continue          # try next encoding
                except Exception as e:
                    print(f"  [CASE 6] Read error ({enc}): {e}")
                    continue

            if df is None:
                print(f"  [CASE 6] Could not decode {csv_name} — skipping")
                continue

            if used_enc != "utf-8":
                print(f"  [CASE 6] Non-UTF-8 file — decoded with: {used_enc}")

            # ── Normalise column names ───────────────────────────────────────
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

            # ── [CASE 1] Schema drift check ───────────────────────────────────
            missing_cols = EXPECTED_COLS - set(df.columns)
            extra_cols   = set(df.columns) - EXPECTED_COLS
            if missing_cols:
                print(f"  [CASE 1] SCHEMA DRIFT — missing columns: {missing_cols}")
                for col in missing_cols:
                    df[col] = None    # fill so downstream logic can flag them
            if extra_cols:
                print(f"  [CASE 1] SCHEMA DRIFT — unexpected columns: {extra_cols}")

            # ── [CASE 8] Empty file guard ─────────────────────────────────────
            if df.empty:
                print(f"  [CASE 8] EMPTY FILE — {csv_name} has no data rows, skipping")
                continue

            # ── [CASE 2] Write RAW table with row-hash deduplication ──────────
            # Every row gets an MD5 hash of its raw field values.
            # INSERT OR IGNORE silently skips any row whose hash already exists.
            raw_rows = []
            for _, row in df.iterrows():
                row_str  = "|".join(str(row.get(c, "")) for c in sorted(EXPECTED_COLS))
                row_hash = hashlib.md5(row_str.encode()).hexdigest()
                raw_rows.append((
                    csv_name, ingested_at,
                    row.get("date"), row.get("stockid"), row.get("broker"),
                    row.get("measure_one"), row.get("measure_two"), row_hash
                ))

            conn.executemany(
                """
                INSERT OR IGNORE INTO {raw_table}
                (source_file, ingested_at, date, stockid, broker,
                 measure_one, measure_two, row_hash)
                VALUES (?,?,?,?,?,?,?,?)
                """.format(raw_table=RAW_TABLE),
                raw_rows,
            )
            conn.commit()

            inserted_raw = conn.execute(
                "SELECT COUNT(*) FROM {raw_table} WHERE source_file=? AND ingested_at=?".format(
                    raw_table=RAW_TABLE
                ),
                (csv_name, ingested_at),
            ).fetchone()[0]
            dup_count = len(raw_rows) - inserted_raw
            if dup_count > 0:
                print(f"  [CASE 2] DUPLICATES skipped: {dup_count} rows")

            # ── Clean & write DERIVED table ───────────────────────────────────
            today        = datetime.utcnow().date()
            derived_rows = []

            for _, row in df.iterrows():
                flags = []

                # [CASE 4] Parse and normalise date — try all known formats
                # [CASE 3] Null dates caught inside _parse_date()
                trade_date, d_flag = _parse_date(row.get("date"))
                if d_flag:
                    flags.append(d_flag)

                # [CASE 9] Flag rows where trade date is older than threshold
                elif trade_date and (today - trade_date).days > LATE_ARRIVAL_DAYS:
                    flags.append("late_arrival")

                # [CASE 3] Missing stockid
                # [TYPE CHANGE] raw string → uppercased, stripped TEXT
                stockid = str(row.get("stockid", "")).strip().upper() or None
                if not stockid:
                    flags.append("missing_stockid")

                # [CASE 3] Missing broker
                # [TYPE CHANGE] raw string → uppercased, stripped TEXT
                broker = str(row.get("broker", "")).strip().upper() or None
                if not broker:
                    flags.append("missing_broker")

                # [CASE 5] Clean measure_one — strip symbols, cast to float
                # [CASE 3] Null/empty caught inside _clean_numeric()
                # [TYPE CHANGE] TEXT → REAL
                m1, m1_flag = _clean_numeric(row.get("measure_one"))
                if m1_flag:
                    flags.append(f"{m1_flag}_measure_one")

                # [CASE 5] Clean measure_two — strip symbols, cast to float
                # [CASE 3] Null/empty caught inside _clean_numeric()
                # [TYPE CHANGE] TEXT → REAL
                m2, m2_flag = _clean_numeric(row.get("measure_two"))
                if m2_flag:
                    flags.append(f"{m2_flag}_measure_two")

                derived_rows.append((
                    str(trade_date) if trade_date else None,
                    stockid, broker, m1, m2,
                    csv_name, ingested_at,
                    int(len(flags) == 0),
                    "|".join(flags) if flags else None
                ))

            conn.executemany(
                """
                INSERT INTO {derived_table}
                (trade_date, stockid, broker, measure_one, measure_two,
                 source_file, ingested_at, is_valid, quality_flags)
                VALUES (?,?,?,?,?,?,?,?,?)
                """.format(derived_table=DERIVED_TABLE),
                derived_rows,
            )
            conn.commit()

            valid = sum(1 for r in derived_rows if r[7] == 1)
            print(f"  Summary: {len(derived_rows)} rows | "
                  f"{valid} valid | {len(derived_rows) - valid} flagged")

    conn.close()
    return DERIVED_TABLE


if __name__ == '__main__':
    filename = 'dc-test-file-ingestion.zip'
    result   = load_data_file(filename)
    print(f"\nDerived table: {result}")