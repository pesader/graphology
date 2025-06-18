import pickle
import logging
from pathlib import Path
from collections import namedtuple
from datetime import datetime
from pybliometrics.scopus import ScopusSearch, init as scopus_init

from graphology.etl._helpers import is_empty, raw_data_directory_path
from graphology import log

fields = (
    "eid doi pii pubmed_id title subtype subtypeDescription "
    "creator afid affilname affiliation_city "
    "affiliation_country author_count author_names author_ids "
    "author_afids coverDate coverDisplayDate publicationName "
    "issn source_id eIssn aggregationType volume "
    "issueIdentifier article_number pageRange description "
    "authkeywords citedby_count openaccess freetoread "
    "freetoreadLabel fund_acr fund_no fund_sponsor"
)
ScopusSearchResult = namedtuple("ScopusSearchResult", fields)


class Extractor:
    def __init__(
        self,
        timestamp: str,
        start_year: int,
        end_year: int,
        data_directory: Path,
    ) -> None:
        # This statement reads the credentials needed to access the Scopus API
        scopus_init()

        self.timestamp = timestamp
        self.start_year: int = start_year
        self.end_year: int = end_year

        self.RAW_DATA_DIRECTORY: Path = raw_data_directory_path(
            self.timestamp, start_year, end_year, data_directory
        )
        self.RAW_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

    def fetch(self) -> None:
        # Search parameters
        UNICAMP_AFFILIATION_ID = "60029570"

        for year in range(self.end_year, self.start_year - 1, -1):
            query = f"AF-ID({UNICAMP_AFFILIATION_ID}) AND PUBYEAR = {year}"
            search = ScopusSearch(query, subscriber=True)
            if search.results:
                results = [ScopusSearchResult(*result) for result in search.results]
                with open(self.RAW_DATA_DIRECTORY / f"results_{year}.pkl", "wb") as f:
                    pickle.dump(results, f)
            log(
                f"finished extracting data from {year}",
                self.timestamp,
            )

        log(
            f"finished extracting data from all requested years",
            self.timestamp,
        )

    def extract(self):
        # Do nothing if data has already been extracted
        if not is_empty(self.RAW_DATA_DIRECTORY):
            log(
                "Skipped data extraction, because data has already been extracted.",
                self.timestamp,
            )
            return

        self.fetch()
