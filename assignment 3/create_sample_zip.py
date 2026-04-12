"""
Generates dc-test-file-ingestion.zip with:
  - Clean rows dated relative to *today* (always valid for late_arrival in solution.py).
  - Messy rows (2024 dates, bad fields) to exercise cleaning and flags.
"""

import zipfile
from datetime import date, timedelta

# Dates within the last 30 days so solution.py does not flag late_arrival.
_today = date.today()
_d = lambda n: (_today - timedelta(days=n)).isoformat()

# Valid sample rows: parseable ISO dates, filled stockid/broker, numeric measures.
VALID_SAMPLE_ROWS = f"""{_d(1)},AAPL,Goldman Sachs,1500.00,3.25
{_d(2)},MSFT,Morgan Stanley,2300.50,1.80
{_d(3)},GOOG,Barclays,800.00,4.50
{_d(5)},AMD,Morgan Stanley,430.00,2.20
{_d(10)},META,JP Morgan,620.75,3.90
"""

# Messy CSV data reflecting real-world broker data quality issues:
#   - Inconsistent date formats
#   - Missing fields (empty date, stockid, broker)
#   - Non-numeric measures ("N/A", "#REF!", "")
#   - Negative measure values
#   - Duplicate rows (same date+stockid+broker)
#   - Whitespace padding and mixed-case broker names
# Historical dates -> will typically get late_arrival when run years later.
MESSY_ROWS = """2024-01-15,AAPL,Goldman Sachs,1500.00,3.25
2024-01-15,MSFT,  morgan stanley  ,2300.50,1.80
2024-01-15,TSLA,GOLDMAN SACHS,N/A,2.10
2024-01-16,AAPL,Goldman Sachs,1520.00,3.30
15/01/2024,GOOG,Barclays,800.00,4.50
January 17 2024,AMZN,JP Morgan,950.25,2.75
2024-01-17,,Goldman Sachs,1100.00,3.00
2024-01-17,NVDA,,750.00,1.95
,AAPL,Goldman Sachs,1500.00,3.25
2024-01-18,MSFT,Morgan Stanley,#REF!,
2024-01-18,TSLA,Goldman Sachs,,
2024-01-15,AAPL,Goldman Sachs,1500.00,3.25
2024-01-19,META,Barclays,-200.00,5.10
2024-01-19,NFLX,JP Morgan,620.75,3.90
01/20/2024,AMD,Morgan Stanley,430.00,2.20
2024-01-20,INTC,Goldman Sachs,310.50,1.75
BADDATE,ORCL,Barclays,275.00,3.45
2024-01-21,IBM,JP Morgan,195.00,4.10
2024-01-21,CRM,Morgan Stanley,540.25,2.85
"""

CSV_DATA = (
    "date,stockid,broker,measure_one,measure_two\n"
    + VALID_SAMPLE_ROWS
    + MESSY_ROWS
)

with zipfile.ZipFile("dc-test-file-ingestion.zip", "w", compression=zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("broker_data/stock_loans.csv", CSV_DATA.strip())

print("Created dc-test-file-ingestion.zip")
print("  Valid rows (relative to today, within 30 days): 5")
print("  Issues embedded in remaining rows:")
print("    - 3 rows with missing date/stockid/broker")
print("    - 1 row with unparseable date ('BADDATE')")
print("    - 3 rows with non-numeric measures ('N/A', '#REF!', empty)")
print("    - 1 row with negative measure_one (-200)")
print("    - 1 duplicate row (2024-01-15, AAPL, Goldman Sachs)")
print("    - Mixed date formats (ISO, DD/MM/YYYY, MM/DD/YYYY, Month DD YYYY)")
print("    - Whitespace padding and mixed-case broker names")
print("    - Old (2024) dates -> late_arrival when run in a later year")
