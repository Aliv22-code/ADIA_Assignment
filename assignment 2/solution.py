import os
import sys
import time
import re
from typing import List
import requests

TEMP_API_KEY = "Kf1fmqa3XaGGGsh6wMw5OPlYgsHA1FTz"

def get_company_csv_list(from_article: str) -> List[str]:
    session = requests.Session()

    # ----------------------------------------------------------------
    # STEP 1: Call Calais API to extract company entities
    # Wait 1 second before API call (dev environment rate limit)
    # ----------------------------------------------------------------
    time.sleep(1)
    try:
        response = session.post(
            "https://api-eit.refinitiv.com/permid/calais",
            headers={
                "X-AG-Access-Token": TEMP_API_KEY,
                "Content-Type": "text/xml",
                "outputFormat": "application/json",
                "x-calais-selectiveTags": "company"
            },
            data=from_article,
            timeout=25
        )
        data = response.json()
    except Exception:
        return []
    print(data)
    print(data.items())
    # ----------------------------------------------------------------
    # STEP 2: Extract company entities from Calais response
    # ----------------------------------------------------------------
    companies = []
    for _, entity in data.items():
        if not isinstance(entity, dict):
            continue
        if entity.get("_typeGroup") != "entities":
            continue
        if entity.get("_type", "").lower() != "company":
            continue

        resolutions = entity.get("resolutions", [])
        if not resolutions:
            continue

        res = resolutions[0]
        print("res", res)
        permid = str(res.get("permid", "")).strip()
        name   = res.get("name", "")
        ticker = res.get("ticker", "") or "NULL"

        if permid and name:
            companies.append((permid, name, ticker))

    # ----------------------------------------------------------------
    # STEP 3: Sort companies by PermId numerically
    # ----------------------------------------------------------------
    companies.sort(key=lambda x: int(x[0]) if x[0].isdigit() else x[0])

    # ----------------------------------------------------------------
    # STEP 4: For each company, fetch IPO date from PermId Info API
    # Wait 1 second before EACH API call (dev environment rate limit)
    # ----------------------------------------------------------------
    result = []
    for permid, name, ticker in companies:
        ipo_date = "NULL"
        try:
            time.sleep(1)  # mandatory 1 second wait before each PermId API call
            perm_resp = session.get(
                f"https://permid.org/1-{permid}",
                headers={"X-AG-Access-Token": TEMP_API_KEY},
                params={"format": "json-ld"},
                timeout=25
            )
            perm_data = perm_resp.json()
            raw_ipo = perm_data.get("hasIPODate", "")
            if raw_ipo:
                # Extract YYYY-MM-DD from "2019-12-11T05:00:00Z"
                m = re.search(r"(\d{4}-\d{2}-\d{2})", str(raw_ipo))
                ipo_date = m.group(1) if m else "NULL"
        except Exception:
            pass

        # ----------------------------------------------------------------
        # STEP 5: Format CSV row
        # - Name wrapped in single quotes
        # - Escape apostrophes in name e.g. McDonald's → McDonald''s
        # - NULL for missing ticker or IPO date
        # - Format: permid,'NAME',TICKER,YYYY-MM-DD
        # ----------------------------------------------------------------
        safe_name = name.replace("'", "''")
        result.append(f"{permid},'{safe_name}',{ticker},{ipo_date}")

    return result


if __name__ == '__main__':
    # HackerRank sets OUTPUT_PATH; default for local runs
    os.environ.setdefault('OUTPUT_PATH', 'output.txt')
    fptr = open(os.environ['OUTPUT_PATH'], 'w')
    # Read full stdin so multi-line XML works; input() stops at the first newline.
    from_article = input
    result = get_company_csv_list(from_article)
    fptr.write('\n'.join(result))
    fptr.write('\n')
    fptr.close()
