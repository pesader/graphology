import pickle
import pandas as pd
from collections import namedtuple
from pathlib import Path

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


class Processor:
    def __init__(
        self,
        timestamp: str,
        start_year: int,
        end_year: int,
    ) -> None:
        self.timestamp: str = timestamp
        self.start_year: int = start_year
        self.end_year: int = end_year

    def process(self):
        RAW_DATA_DIRECTORY = DATA_DIRECTORY / self.timestamp / "raw"
        PROCESSED_DATA_DIRECTORY = DATA_DIRECTORY / self.timestamp / "processed"
        PROCESSED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

        for year in range(self.end_year, self.start_year - 1, -1):
            pickle_path = RAW_DATA_DIRECTORY / f"results_{year}.pkl"
            if not pickle_path.exists():
                continue

            with open(pickle_path, "rb") as f:
                results = pickle.load(f)

            documents = []
            authorships = []
            authors = {}
            affiliations = {}

            for result in results:
                eid = result.eid
                documents.append(
                    {
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
                        "source_eissn": result.eIssn,
                    }
                )

                if result.afid:
                    afids = result.afid.split(";")
                    names = result.affilname.split(";")
                    cities = result.affiliation_city.split(";")
                    countries = result.affiliation_country.split(";")
                    for i in range(len(afids)):
                        affiliations[afids[i]] = {
                            "affiliation_id": afids[i],
                            "name": names[i],
                            "city": cities[i],
                            "country": countries[i],
                        }

                if result.author_ids:
                    ids = result.author_ids.split(";")
                    names = result.author_names.split(";")
                    afids = result.author_afids.split(";")
                    for i in range(len(ids)):
                        author_id = ids[i]
                        author_name = names[i]
                        authors[author_id] = {
                            "author_id": author_id,
                            "name": author_name,
                        }

                        authorships.append(
                            {
                                "eid": eid,
                                "author_id": author_id,
                                "affiliations": ",".join(afids[i].split("-")),
                                "first_author": i == 0,
                            }
                        )

            pd.DataFrame(documents).to_csv(
                PROCESSED_DATA_DIRECTORY / f"documents_{year}.tsv",
                sep="\t",
                index=False,
            )
            pd.DataFrame(authors.values()).to_csv(
                PROCESSED_DATA_DIRECTORY / f"authors_{year}.tsv",
                sep="\t",
                index=False,
            )
            pd.DataFrame(authorships).to_csv(
                PROCESSED_DATA_DIRECTORY / f"authorships_{year}.tsv",
                sep="\t",
                index=False,
            )
            pd.DataFrame(affiliations.values()).to_csv(
                PROCESSED_DATA_DIRECTORY / f"affiliations_{year}.tsv",
                sep="\t",
                index=False,
            )

    def merge(self):
        TABLE_PREFIXES = ["documents", "affiliations", "authors", "authorships"]

        PROCESSED_DATA_DIRECTORY = DATA_DIRECTORY / self.timestamp / "processed"
        MERGED_DATA_DIRECTORY: Path = DATA_DIRECTORY / self.timestamp / "merged"
        MERGED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

        for prefix in TABLE_PREFIXES:
            # Find all matching authorship files
            tsv_files = sorted(PROCESSED_DATA_DIRECTORY.glob(f"{prefix}_*.tsv"))

            # Load and concatenate all files
            df = pd.concat(
                (pd.read_csv(f, sep="\t", dtype=str) for f in tsv_files),
                ignore_index=True,
            )

            if prefix == "authorships":
                df["affiliations"] = df["affiliations"].str.split(",")
                df = df.explode("affiliations").reset_index(drop=True)
                df = df.rename(columns={"affiliations": "affiliation_id"})

            # Save to a single merged file
            df.to_csv(MERGED_DATA_DIRECTORY / f"{prefix}.tsv", sep="\t", index=False)
