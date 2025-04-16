from collections import namedtuple
from pathlib import Path
import pandas as pd
from datetime import datetime
from pybliometrics.scopus import ScopusSearch, init

init()

timestamp = datetime.now().isoformat(timespec='seconds').replace(":", "-")

# Search parameters
UNICAMP_AFFILIATION_ID = '60029570'
END_YEAR = 2025
START_YEAR = 2020

# Directory structure
DATA_DIRECTORY: Path = Path("data")
RESULTS_DIRECTORY: Path = DATA_DIRECTORY / Path(timestamp)

# Ensure results directory exists
RESULTS_DIRECTORY.mkdir(parents=True, exist_ok=True)

# Taken from pybliometrics.scopus.scopus_search
# The following code snippet is licensed under the MIT License.
# SPDX-License-Identifier: MIT
fields = 'eid doi pii pubmed_id title subtype subtypeDescription ' \
         'creator afid affilname affiliation_city ' \
         'affiliation_country author_count author_names author_ids '\
         'author_afids coverDate coverDisplayDate publicationName '\
         'issn source_id eIssn aggregationType volume '\
         'issueIdentifier article_number pageRange description '\
         'authkeywords citedby_count openaccess freetoread '\
         'freetoreadLabel fund_acr fund_no fund_sponsor'
Document = namedtuple('Document', fields)

for year in range(START_YEAR, END_YEAR + 1):
    query = f"AF-ID({UNICAMP_AFFILIATION_ID}) AND PUBYEAR = {year}"
    search = ScopusSearch(query, subscriber=True)

    documents = []
    authorships = []
    affiliations = {}

    for result in search.results:
        result = Document(result)
        eid = result.eid
        documents.append({
            "title": result.title,
            "eid": eid,
            "doi": result.doi,
            "openaccess": result.openaccess,
            "date": result.coverDate,
            "document_type": result.subtype,
            "document_type_description": result.subtypeDescription,
            "first_author": result.creator,
            "volume": result.volume,
            "issue": result.issueIdentifier,
            "page": result.pageRange,
            "citedby_count": result.citedby_count,
            "funding_acronym": result.fund_acr,
            "funding_number": result.fund_no,
            "funding_name": result.fund_sponsor,
            "source_name": result.publicationName,
            "source_type": result.aggregationType,
            "source_id": result.source_id,
            "source_issn": result.issn,
            "source_eissn": result.eIssn
        })

        if result.afid:
            afids = result.afid.split(";")
            names = result.affilname.split(";")
            cities = result.affiliation_city.split(";")
            countries = result.affiliation_country.split(";")
            for i in range(len(afids)):
                affiliations[afids[i]] = {
                    "id": afids[i],
                    "name": names[i],
                    "city": cities[i],
                    "country": countries[i]
                }

        if result.author_ids:
            ids = result.author_ids.split(";")
            names = result.author_names.split(";")
            afids = result.author_afids.split(";")
            for i in range(len(ids)):
                authorships.append({
                    "eid": eid,
                    "author_id": ids[i],
                    "author_name": names[i],
                    "affiliations": ",".join(afids[i].split("-"))
                })

    pd.DataFrame(documents).to_csv(RESULTS_DIRECTORY / f"documents_{year}.tsv", sep="\t", index=False)
    pd.DataFrame(authorships).to_csv(RESULTS_DIRECTORY / f"authorships_{year}.tsv", sep="\t", index=False)
    pd.DataFrame(affiliations.values()).to_csv(RESULTS_DIRECTORY / f"affiliations_{year}.tsv", sep="\t", index=False)
