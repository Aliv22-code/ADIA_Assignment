#!/usr/bin/env python3
"""
Dump derived_stock_loans from stock_loans.db into two text files:

  valid_rows.txt   — is_valid = 1
  flagged_rows.txt — is_valid = 0

Run from assignment3/ after solution.py (same folder as stock_loans.db).

  python3 export_derived_txt.py
"""

import csv
import os
import sqlite3

FILEPATH = os.environ.get("FILEPATH", ".")
DB_PATH = os.path.join(FILEPATH, "stock_loans.db")
TABLE = "derived_stock_loans"

COLUMNS = [
    "id",
    "trade_date",
    "stockid",
    "broker",
    "measure_one",
    "measure_two",
    "source_file",
    "ingested_at",
    "is_valid",
    "quality_flags",
]


def main():
    if not os.path.isfile(DB_PATH):
        raise SystemExit(f"No database at {DB_PATH}. Run solution.py first.")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    for name, where in (("valid_rows.txt", "is_valid = 1"), ("flagged_rows.txt", "is_valid = 0")):
        path = os.path.join(FILEPATH, name)
        cur = conn.execute(
            f"SELECT {', '.join(COLUMNS)} FROM {TABLE} WHERE {where} ORDER BY id"
        )
        rows = cur.fetchall()
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(COLUMNS)
            for row in rows:
                w.writerow([row[c] for c in COLUMNS])
        print(f"Wrote {len(rows)} rows -> {path}")

    conn.close()


if __name__ == "__main__":
    main()
