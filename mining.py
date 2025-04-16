from pathlib import Path
import pandas as pd
from datetime import datetime
from pybliometrics.scopus import ScopusSearch, init

init()

timestamp = datetime.now().isoformat(timespec='seconds').replace(":", "-")

DATA_DIRECTORY: Path = Path("data")
RESULTS_DIRECTORY: Path = DATA_DIRECTORY / Path(timestamp)

# Ensure results directory exists
RESULTS_DIRECTORY.mkdir(parents=True, exist_ok=True)

documents_path = RESULTS_DIRECTORY / Path("documents.tsv")
authorships_path = RESULTS_DIRECTORY / Path("authorships.tsv")
affiliations_path = RESULTS_DIRECTORY / Path("affiliations.tsv")

# Initialize files with headers
def init_tsv(filename, fieldnames):
    pd.DataFrame(columns=fieldnames).to_csv(filename, sep="\t", index=False)

init_tsv(documents_path, [
    "title",
    "eid",
    "doi",
    "openaccess",
    "date",
    "document_type",
    "document_type_description",
    "first_author",
    "volume",
    "issue",
    "page",
    "citedby_count",
    "funding_acronym",
    "funding_number",
    "funding_name",
    "source_name",
    "source_type",
    "source_id",
    "source_issn",
    "source_eissn",
])
init_tsv(authorships_path, ["eid", "author_id", "author_name", "affiliations"])
init_tsv(affiliations_path, ["id", "name", "city", "country"])

UNICAMP_AFFILIATION_ID = '60029570'

END_YEAR = 2025
START_YEAR = 2025

for year in range(START_YEAR, END_YEAR + 1):
    query = f"AF-ID({UNICAMP_AFFILIATION_ID}) AND PUBYEAR > {year - 1} AND PUBYEAR < {year + 1}"
    search = ScopusSearch(query, subscriber=True)

    # Deduplication store for affiliations
    affiliations = {}

    def append_row(filename, row, fieldnames):
        df = pd.DataFrame([row], columns=fieldnames)
        df.to_csv(filename, sep="\t", index=False, header=False, mode="a")

    for result in search.results:
        # Document info
        title = result.title
        eid = result.eid
        doi = result.doi
        openaccess = result.openaccess
        date = result.coverDate
        document_type = result.subtype
        document_type_description = result.subtypeDescription
        first_author = result.creator
        volume = result.volume
        issue = result.issueIdentifier
        page = result.pageRange
        citedby_count = result.citedby_count
        funding_acronym = result.fund_acr
        funding_number = result.fund_no
        funding_name = result.fund_sponsor
        source_name = result.publicationName
        source_type = result.aggregationType
        source_id = result.source_id
        source_issn = result.issn
        source_eissn = result.eIssn

        append_row(documents_path, {
            "title": title,
            "eid": eid,
            "doi": doi,
            "openaccess": openaccess,
            "date": date,
            "document_type": document_type,
            "document_type_description": document_type_description,
            "first_author": first_author,
            "volume": volume,
            "issue": issue,
            "page": page,
            "citedby_count": citedby_count,
            "funding_acronym": funding_acronym,
            "funding_number": funding_number,
            "funding_name": funding_name,
            "source_name": source_name,
            "source_type": source_type,
            "source_id": source_id,
            "source_issn": source_issn,
            "source_eissn": source_eissn
        }, fieldnames=pd.read_csv(documents_path, sep="\t", nrows=0).columns)

        # Affiliations
        if result.afid:
            afids = result.afid.split(";")
            affilnames = result.affilname.split(";")
            cities = result.affiliation_city.split(";")
            countries = result.affiliation_country.split(";")
            for i in range(len(afids)):
                affiliations[afids[i]] = {
                    "id": afids[i],
                    "name": affilnames[i],
                    "city": cities[i],
                    "country": countries[i]
                }

        # Authors
        if result.author_ids:
            author_ids = result.author_ids.split(";")
            author_names = result.author_names.split(";")
            author_afids = result.author_afids.split(";")
            for i in range(len(author_ids)):
                affils = author_afids[i].split("-")
                append_row(authorships_path, {
                    "eid": eid,
                    "author_id": author_ids[i],
                    "author_name": author_names[i],
                    "affiliations": ",".join(affils)
                }, fieldnames=pd.read_csv(authorships_path, sep="\t", nrows=0).columns)

    # Write deduplicated affiliations at once
    for affiliation in affiliations.values():
        append_row(
            affiliations_path,
            affiliation,
            fieldnames=pd.read_csv(affiliations_path, sep="\t", nrows=0).columns
        )

    print(f"{year} done!")
