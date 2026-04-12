# ADIA Assignment Workspace

This repository contains solutions and written responses for multiple ADIA assessment tasks.

## Folder Overview

- `assignment 1/` - Tick data analysis on E-mini S&P 500 futures (`ES.h5`)
- `assignment 2/` - Company extraction from article text using Refinitiv/PermID APIs
- `assignment 3/` - Stock-loan data ingestion and quality validation pipeline
- `assignment 4/` - CORD-19 ZIP/JSON ingestion into SQLite
- `assignment 5/` - Written response: Define Statistical Biases
- `assignment 6/` - Written response: Data Monitoring
- `assignment 7/` - Written response: Point-in-time-ness

## Important Notes

- `ES.h5` is large and should not be committed/pushed unless explicitly required.
- Use the project virtual environment for Python runs:
  - `source .venv/bin/activate`
- For folders with spaces, quote paths:
  - `cd "assignment 1"`

## Quick Start

From repo root:

```bash
source .venv/bin/activate
```

Then run any assignment script, for example:

```bash
python "assignment 1/solution.py"
python "assignment 2/solution.py"
python "assignment 3/solution.py"
python "assignment 4/solution.py"
```
