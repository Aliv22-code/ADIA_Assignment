# Assignment 2 - Tickers from Articles

## Overview

This solution implements:

```python
def get_company_csv_list(from_article: str) -> List[str]
```

It calls the Refinitiv/Calais endpoint to extract company entities from article text and returns a list of CSV rows sorted by PermID.

Output row format:

`PermID,'Organization Name',Ticker,IPODate`

Rules handled:
- Company name is single-quoted.
- Missing ticker or IPO date is represented as `NULL`.
- IPO date is normalized to `YYYY-MM-DD` where available.
- One-second delay is applied before each API call.

## Files

| File | Description |
|---|---|
| `solution.py` | Main HackerRank-style solution |

## Notes

- This code expects a working token in `TEMP_API_KEY`.
- In local environments where external API calls are blocked, the function will return an empty list instead of crashing.
