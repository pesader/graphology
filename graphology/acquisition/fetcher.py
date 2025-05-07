import pickle
from collections import namedtuple
from datetime import datetime
from pybliometrics.scopus import ScopusSearch, init as scopus_init
from .constants import DATA_DIRECTORY

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


class Fetcher:
    def __init__(self, start_year: int, end_year: int) -> None:
        # This statement reads the credentials needed to access the Scopus API
        scopus_init()

        self.start_year: int = start_year
        self.end_year: int = end_year

    def fetch(self) -> str:
        # Search parameters
        UNICAMP_AFFILIATION_ID = "60029570"

        # Directory structure
        timestamp = datetime.now().isoformat(timespec="seconds").replace(":", "-")
        RAW_DATA_DIRECTORY = DATA_DIRECTORY / timestamp / "raw"
        RAW_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

        for year in range(self.end_year, self.start_year - 1, -1):
            query = f"AF-ID({UNICAMP_AFFILIATION_ID}) AND PUBYEAR = {year}"
            search = ScopusSearch(query, subscriber=True)
            if search.results:
                results = [ScopusSearchResult(*result) for result in search.results]
                with open(RAW_DATA_DIRECTORY / f"results_{year}.pkl", "wb") as f:
                    pickle.dump(results, f)

        return timestamp
