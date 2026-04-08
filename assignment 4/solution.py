"""
Assignment 4 — CORD-19 Text File Ingestion
-------------------------------------------
This script reads a ZIP file containing CORD-19 COVID-19 research papers
(each stored as a JSON file), extracts key metadata, and loads it into an
in-memory SQLite database. The results are written to an output file.

The CORD-19 dataset is a collection of academic papers about COVID-19.
Each JSON file follows a standard schema with fields like paper_id, title,
authors (with emails), abstract, and body text.

Metadata schema reference:
    Source  : Kaggle — Allen Institute for AI, CORD-19 Research Challenge
    Path    : https://www.kaggle.com/datasets/allen-institute-for-ai/
              CORD-19-research-challenge/data?select=json_schema.txt
    Fields used in this script:
        paper_id                      — 40-char SHA1 of the PDF
        metadata.title                — paper title
        metadata.authors[].first      — author first name
        metadata.authors[].middle[]   — author middle names (list)
        metadata.authors[].last       — author last name
        metadata.authors[].email      — author email (used to derive country)
"""

import json       # for parsing JSON files inside the ZIP
import sqlite3    # for creating and querying the in-memory database
import os         # for reading the OUTPUT_PATH environment variable
from zipfile import ZipFile   # for opening and reading the ZIP archive


def load_cord19_files(filename):
    """
    Opens a CORD-19 ZIP archive, reads every JSON paper inside it,
    and stores the metadata in an in-memory SQLite database.
    Finally writes the contents of both tables to the output file.

    Parameters:
        filename (str): Path to the .zip file containing CORD-19 JSON papers.
    """

    # ── Create an in-memory SQLite database ───────────────────────────────────
    # ":memory:" means the database lives only in RAM — nothing is saved to disk.
    # This is fast and disposable, which suits HackerRank's judge environment.
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()

    # ── Schema Design ─────────────────────────────────────────────────────────
    # Table 1: papers
    #   Stores the core identity of each research paper.
    #   paper_id is the primary key — a 40-character SHA1 hash of the PDF.
    #   title is the paper's full title from metadata.
    c.execute("""
        CREATE TABLE papers (
            paper_id  TEXT PRIMARY KEY,
            title     TEXT
        )
    """)

    # Table 2: authors
    #   Stores one row per author per paper (a paper can have many authors).
    #   Linked back to the papers table via paper_id as a foreign key.
    #   'middle' stores the list of middle names joined as a single string.
    #   'country' is derived from the author's email domain TLD
    #       e.g. researcher@uni.ac.uk  → country = 'UK'
    #            researcher@iit.ac.in  → country = 'IN'
    #            researcher@mit.edu    → country = 'EDU' (no country code for .edu)
    c.execute("""
        CREATE TABLE authors (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            paper_id  TEXT,
            first     TEXT,
            middle    TEXT,
            last      TEXT,
            email     TEXT,
            country   TEXT,
            FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
        )
    """)

    # ── Parse the ZIP Archive ─────────────────────────────────────────────────
    # Open the ZIP file in read mode and iterate through every file inside it.
    # We only process files that end in '.json' to skip any other contents
    # (e.g. folder entries, README files, etc.).
    with ZipFile(filename, 'r') as z:
        for name in z.namelist():
            if name.endswith('.json'):

                # Open and parse the JSON file directly from the ZIP (no extraction needed)
                with z.open(name) as f:
                    data = json.load(f)

                # ── Extract paper-level fields ────────────────────────────────
                # paper_id sits at the top level of the JSON object.
                # title is nested inside the 'metadata' object.
                # We use .get() with '' as a fallback to avoid KeyErrors on missing fields.
                paper_id = data.get('paper_id', '')
                title    = data.get('metadata', {}).get('title', '')

                # INSERT OR IGNORE ensures we skip duplicate paper_ids
                # (in case the same paper appears more than once in the ZIP).
                c.execute(
                    "INSERT OR IGNORE INTO papers VALUES (?, ?)",
                    (paper_id, title)
                )

                # ── Extract author-level fields ───────────────────────────────
                # 'authors' is a list inside metadata — one dict per author.
                # We build a list of tuples (rows) and insert them all at once
                # using executemany() for efficiency.
                authors = data.get('metadata', {}).get('authors', [])
                rows = []
                for author in authors:
                    first  = author.get('first', '')
                    # 'middle' is a list (e.g. ["R.", "J."]) — join into one string
                    middle = ', '.join(author.get('middle', []))
                    last   = author.get('last', '')
                    email  = author.get('email', '')

                    # ── Derive country from email TLD ─────────────────────────
                    # The top-level domain (TLD) of an email address often
                    # indicates the author's country:
                    #   .de → Germany, .uk → UK, .au → Australia, .kr → South Korea
                    # We split on '.' and take the last segment, uppercased.
                    # Limitation: generic TLDs like .edu, .com, .org don't map
                    # to a specific country (mainly used by US institutions).
                    country = ''
                    if email and '.' in email:
                        tld = email.split('.')[-1].upper()
                        country = tld

                    rows.append((paper_id, first, middle, last, email, country))

                # Bulk insert all authors for this paper in one operation
                c.executemany("""
                    INSERT INTO authors (paper_id, first, middle, last, email, country)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, rows)

    # Commit all inserts to the in-memory database before querying
    conn.commit()

    # ── Write Output ──────────────────────────────────────────────────────────
    # OUTPUT_PATH is an environment variable set by the HackerRank judge.
    # We open the file and write both table contents as plain text.
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    fptr.write("=== PAPERS ===\n")
    for row in c.execute("SELECT * FROM papers"):
        fptr.write(str(row) + '\n')

    fptr.write("\n=== AUTHORS ===\n")
    for row in c.execute("SELECT * FROM authors"):
        fptr.write(str(row) + '\n')

    conn.close()
    fptr.close()


if __name__ == '__main__':
    # ── Local Testing Entry Point ─────────────────────────────────────────────
    # When running locally (outside HackerRank), OUTPUT_PATH won't be set.
    # setdefault() assigns 'output.txt' only if the variable isn't already defined,
    # so the HackerRank judge's value is never overwritten.
    os.environ.setdefault('OUTPUT_PATH', 'output.txt')
    load_cord19_files("cord19_mini.zip")

    # Print the output file contents to the terminal for quick inspection
    print(open(os.environ['OUTPUT_PATH']).read())
