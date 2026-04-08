"""
Creates cord19_mini.zip with realistic CORD-19 JSON files
matching the full official schema for testing load_cord19_files().
"""

import json
import zipfile

PAPERS = [
    {
        "paper_id": "abc1234567890abcdef1234567890abcdef123456",
        "metadata": {
            "title": "COVID-19 impact on government spending in United States",
            "authors": [
                {
                    "first": "Alice", "middle": ["R."], "last": "Smith",
                    "suffix": "",
                    "affiliation": {"institution": "MIT", "location": {"country": "USA"}},
                    "email": "asmith@mit.edu"
                },
                {
                    "first": "Bob", "middle": [], "last": "Jones",
                    "suffix": "",
                    "affiliation": {"institution": "Harvard", "location": {"country": "USA"}},
                    "email": "bjones@harvard.edu"
                },
            ],
            "abstract": [
                {
                    "text": "This paper examines US fiscal policy changes during COVID-19.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "section": "Abstract"
                }
            ],
            "body_text": [
                {
                    "text": "The pandemic triggered unprecedented government spending.",
                    "cite_spans": [{"start": 4, "end": 13, "text": "[1]", "ref_id": "BIBREF0"}],
                    "ref_spans": [],
                    "eq_spans": [],
                    "section": "Introduction"
                },
                {
                    "text": "Federal stimulus packages exceeded $2 trillion in 2020.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "eq_spans": [],
                    "section": "Conclusion"
                }
            ],
            "bib_entries": {
                "BIBREF0": {
                    "ref_id": "BIBREF0",
                    "title": "Fiscal policy in a pandemic",
                    "authors": [{"first": "John", "middle": [], "last": "Doe", "suffix": ""}],
                    "year": 2020,
                    "venue": "Journal of Economics",
                    "volume": "12",
                    "issn": "1234-5678",
                    "pages": "1-20",
                    "other_ids": {"DOI": ["10.1000/xyz123"]}
                }
            },
            "ref_entries": {
                "FIGREF0": {"text": "Fig 1: US Federal spending 2019-2021", "type": "figure"},
                "TABREF0": {"text": "Table 1: Stimulus package breakdown", "type": "table"}
            },
            "back_matter": []
        }
    },
    {
        "paper_id": "def4567890abcdef1234567890abcdef12345678",
        "metadata": {
            "title": "Research funding trends in United Kingdom during pandemic",
            "authors": [
                {
                    "first": "Clara", "middle": [], "last": "Brown",
                    "suffix": "",
                    "affiliation": {"institution": "University of Oxford", "location": {"country": "UK"}},
                    "email": "cbrown@ox.ac.uk"
                },
                {
                    "first": "David", "middle": ["T."], "last": "Wilson",
                    "suffix": "PhD",
                    "affiliation": {"institution": "University of Cambridge", "location": {"country": "UK"}},
                    "email": "dwilson@cam.ac.uk"
                },
            ],
            "abstract": [
                {
                    "text": "Analysis of UK research budgets and funding allocation under COVID-19.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "section": "Abstract"
                }
            ],
            "body_text": [
                {
                    "text": "UK research councils reallocated funds toward COVID-19 research.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "eq_spans": [],
                    "section": "Introduction"
                }
            ],
            "bib_entries": {},
            "ref_entries": {},
            "back_matter": []
        }
    },
    {
        "paper_id": "ghi7890abcdef1234567890abcdef1234567890ab",
        "metadata": {
            "title": "Epidemiological modelling of SARS-CoV-2 spread in Germany",
            "authors": [
                {
                    "first": "Eva", "middle": [], "last": "Müller",
                    "suffix": "",
                    "affiliation": {"institution": "Charité Berlin", "location": {"country": "Germany"}},
                    "email": "emuller@charite.de"
                },
            ],
            "abstract": [
                {
                    "text": "German epidemiological data and SIR model fitting for COVID-19 spread.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "section": "Abstract"
                }
            ],
            "body_text": [
                {
                    "text": "We applied an SEIR model to German infection data from March 2020.",
                    "cite_spans": [],
                    "ref_spans": [{"start": 0, "end": 12, "text": "Figure 1", "ref_id": "FIGREF0"}],
                    "eq_spans": [],
                    "section": "Methods"
                }
            ],
            "bib_entries": {},
            "ref_entries": {
                "FIGREF0": {"text": "Fig 1: SEIR model fit to German data", "type": "figure"}
            },
            "back_matter": []
        }
    },
    {
        "paper_id": "jkl0123456789abcdef1234567890abcdef123456",
        "metadata": {
            "title": "Vaccine rollout effectiveness: a global perspective",
            "authors": [
                {
                    "first": "Frank", "middle": [], "last": "Lee",
                    "suffix": "",
                    "affiliation": {"institution": "WHO", "location": {"country": "Switzerland"}},
                    "email": "frank.lee@who.int"
                },
                {
                    "first": "Grace", "middle": ["S."], "last": "Kim",
                    "suffix": "",
                    "affiliation": {"institution": "KAIST", "location": {"country": "South Korea"}},
                    "email": "gkim@kaist.ac.kr"
                },
                {
                    "first": "Henry", "middle": [], "last": "Patel",
                    "suffix": "",
                    "affiliation": {"institution": "AIIMS", "location": {"country": "India"}},
                    "email": "hpatel@aiims.edu"
                },
            ],
            "abstract": [
                {
                    "text": "Comparative analysis of COVID-19 vaccine rollout efficiency across 50 nations.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "section": "Abstract"
                }
            ],
            "body_text": [
                {
                    "text": "Nations with pre-existing healthcare infrastructure vaccinated faster.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "eq_spans": [],
                    "section": "Results"
                }
            ],
            "bib_entries": {},
            "ref_entries": {
                "TABREF0": {"text": "Table 1: Vaccination rates by country", "type": "table"}
            },
            "back_matter": []
        }
    },
    {
        "paper_id": "mno3456789abcdef1234567890abcdef123456789",
        "metadata": {
            "title": "Mental health outcomes in Australia post-lockdown",
            "authors": [
                {
                    "first": "Isla", "middle": ["J."], "last": "Turner",
                    "suffix": "",
                    "affiliation": {"institution": "University of Melbourne", "location": {"country": "Australia"}},
                    "email": "iturner@unimelb.edu.au"
                },
            ],
            "abstract": [
                {
                    "text": "Psychological impact assessment of prolonged lockdowns in Australia.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "section": "Abstract"
                }
            ],
            "body_text": [
                {
                    "text": "Anxiety and depression rates rose significantly during lockdown periods.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "eq_spans": [],
                    "section": "Introduction"
                },
                {
                    "text": "Post-lockdown recovery showed gradual improvement over 6 months.",
                    "cite_spans": [],
                    "ref_spans": [],
                    "eq_spans": [],
                    "section": "Conclusion"
                }
            ],
            "bib_entries": {},
            "ref_entries": {},
            "back_matter": []
        }
    },
]

ZIP_NAME = "cord19_mini.zip"

with zipfile.ZipFile(ZIP_NAME, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for paper in PAPERS:
        json_bytes = json.dumps(paper, indent=2).encode("utf-8")
        zf.writestr(f"papers/{paper['paper_id']}.json", json_bytes)

print(f"Created '{ZIP_NAME}' with {len(PAPERS)} papers:")
with zipfile.ZipFile(ZIP_NAME, "r") as zf:
    for name in zf.namelist():
        print(f"  {name}")
