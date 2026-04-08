"""
Generates dc-test-file-ingestion.zip with intentionally messy stock loan
CSV data to test all cleaning rules in solution.py.
"""

import zipfile

# Messy CSV data reflecting real-world broker data quality issues:
#   - Inconsistent date formats
#   - Missing fields (empty date, stockid, broker)
#   - Non-numeric measures ("N/A", "#REF!", "")
#   - Negative measure values
#   - Duplicate rows (same date+stockid+broker)
#   - Whitespace padding and mixed-case broker names
CSV_DATA = """date,stockid,broker,measure_one,measure_two
2024-01-15,AAPL,Goldman Sachs,1500.00,3.25
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

with zipfile.ZipFile("dc-test-file-ingestion.zip", "w", compression=zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("broker_data/stock_loans.csv", CSV_DATA.strip())

print("Created dc-test-file-ingestion.zip")
print(f"  Issues embedded in data:")
print(f"    - 3 rows with missing date/stockid/broker")
print(f"    - 1 row with unparseable date ('BADDATE')")
print(f"    - 3 rows with non-numeric measures ('N/A', '#REF!', empty)")
print(f"    - 1 row with negative measure_one (-200)")
print(f"    - 1 duplicate row (2024-01-15, AAPL, Goldman Sachs)")
print(f"    - Mixed date formats (ISO, DD/MM/YYYY, MM/DD/YYYY, Month DD YYYY)")
print(f"    - Whitespace padding and mixed-case broker names")
