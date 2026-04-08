# Assignment 4 — CORD-19 Text File Ingestion

## Overview

This project loads CORD-19 COVID-19 research paper metadata from a ZIP file into an in-memory SQLite database.  
It is designed for the ADIA HackerRank challenge on text file ingestion.

## Files

| File | Description |
|---|---|
| `solution.py` | Main solution — loads ZIP into SQLite and writes output |
| `create_sample_zip.py` | Generates `cord19_mini.zip` for local testing |
| `cord19_mini.zip` | Sample ZIP with 5 CORD-19-style JSON papers |
| `output.txt` | Output written by `solution.py` (mirrors HackerRank `OUTPUT_PATH`) |

## Database Schema

```
papers
├── paper_id   TEXT  PRIMARY KEY   (40-char SHA1)
└── title      TEXT

authors
├── id         INTEGER  PRIMARY KEY AUTOINCREMENT
├── paper_id   TEXT     FOREIGN KEY → papers.paper_id
├── first      TEXT
├── middle     TEXT
├── last       TEXT
├── email      TEXT
└── country    TEXT     (derived from email TLD, e.g. .uk → UK, .de → DE)
```

## JSON → SQLite Mapping

```
JSON field                        →   SQLite column
─────────────────────────────────────────────────────
paper_id                          →   papers.paper_id
metadata.title                    →   papers.title
metadata.authors[].first          →   authors.first
metadata.authors[].middle[]       →   authors.middle  (joined as string)
metadata.authors[].last           →   authors.last
metadata.authors[].email          →   authors.email
email TLD (.uk, .de, .au, .kr)    →   authors.country (derived)
```

## How to Run

### 1. Generate the test ZIP
```bash
python3 create_sample_zip.py
```

### 2. Run the solution
```bash
OUTPUT_PATH=output.txt python3 solution.py
```

Or simply:
```bash
python3 solution.py
```
The `__main__` block defaults `OUTPUT_PATH` to `output.txt` if not set.

## Requirements

- Python 3.6+
- Standard library only (`json`, `sqlite3`, `zipfile`, `os`) — no extra installs needed

## Sample Output

```
=== PAPERS ===
('abc123...', 'COVID-19 impact on government spending in United States')
('def456...', 'Research funding trends in United Kingdom during pandemic')
...

=== AUTHORS ===
(1, 'abc123...', 'Alice', 'R.', 'Smith', 'asmith@mit.edu', 'EDU')
(2, 'abc123...', 'Bob', '', 'Jones', 'bjones@harvard.edu', 'EDU')
...
```

## Notes

- Country derivation via TLD works for country-coded domains (`.uk`, `.de`, `.au`, `.kr`) but US authors using `.edu` or `.com` will not map to a country code — this is a known limitation noted in the problem statement.
- The in-memory SQLite database is ephemeral; all data is lost after the process exits.
