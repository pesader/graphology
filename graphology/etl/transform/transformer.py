import pickle
import logging
import uuid

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlmodel import Session

from graphology.etl._helpers import (
    is_empty,
    merged_data_directory,
    neo4j_data_directory,
    processed_data_directory,
    raw_data_directory_path,
)

from graphology.etl.load.rdbms.database import engine

from graphology import log


class Transformer:
    def __init__(
        self,
        timestamp: str,
        start_year: int,
        end_year: int,
        data_directory: Path,
    ) -> None:
        self.timestamp: str = timestamp
        self.start_year: int = start_year
        self.end_year: int = end_year

        self.RAW_DATA_DIRECTORY: Path = raw_data_directory_path(
            self.timestamp, start_year, end_year, data_directory
        )

        self.PROCESSED_DATA_DIRECTORY: Path = processed_data_directory(
            self.timestamp, start_year, end_year, data_directory
        )
        self.PROCESSED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

        self.MERGED_DATA_DIRECTORY: Path = merged_data_directory(
            self.timestamp, start_year, end_year, data_directory
        )
        self.MERGED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

    def process(self) -> None:
        for year in range(self.end_year, self.start_year - 1, -1):
            pickle_path = self.RAW_DATA_DIRECTORY / f"results_{year}.pkl"
            if not pickle_path.exists():
                continue

            with open(pickle_path, "rb") as f:
                results = pickle.load(f)

            documents = []
            authorships = []
            authors = {}
            institutions = {}

            for result in results:
                document_id = result.eid
                documents.append(
                    {
                        "title": result.title,
                        "scopus_id": document_id,
                        "doi": result.doi,
                        "openaccess": result.openaccess,
                        "date": result.coverDate,
                        "document_type": result.subtype,
                        "document_type_description": result.subtypeDescription,
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
                        institutions[afids[i]] = {
                            "scopus_id": afids[i],
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
                            "scopus_id": author_id,
                            "name": author_name,
                        }

                        authorships.append(
                            {
                                "document_id": document_id,
                                "author_id": author_id,
                                "institution_ids": ",".join(afids[i].split("-")),
                                "first_author": i == 0,
                            }
                        )

            pd.DataFrame(documents).to_csv(
                self.PROCESSED_DATA_DIRECTORY / f"documents_{year}.tsv",
                sep="\t",
                index=False,
            )

            pd.DataFrame(authors.values()).to_csv(
                self.PROCESSED_DATA_DIRECTORY / f"authors_{year}.tsv",
                sep="\t",
                index=False,
            )

            pd.DataFrame(institutions.values()).to_csv(
                self.PROCESSED_DATA_DIRECTORY / f"institutions_{year}.tsv",
                sep="\t",
                index=False,
            )

            pd.DataFrame(authorships).to_csv(
                self.PROCESSED_DATA_DIRECTORY / f"authorships_{year}.tsv",
                sep="\t",
                index=False,
            )

    def merge(self):
        TABLE_PREFIXES = ["documents", "institutions", "authors", "authorships"]

        for prefix in TABLE_PREFIXES:
            # Find all matching authorship files
            tsv_files = sorted(self.PROCESSED_DATA_DIRECTORY.glob(f"{prefix}_*.tsv"))

            # Load and concatenate all files
            df = pd.concat(
                (pd.read_csv(f, sep="\t", dtype=str) for f in tsv_files),
                ignore_index=True,
            )

            # Save to a single merged file
            df.to_csv(
                self.MERGED_DATA_DIRECTORY / f"{prefix}.tsv",
                sep="\t",
                index=False,
            )

    def normalize(self):
        """
        Normalize the data in authorships.tsv by splitting the "institution_ids" row
        """
        # fmt: off
        df_authorships = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / "authorships.tsv",
            sep="\t",
            dtype=str,
        )
        df_authorships["institution_ids"] = df_authorships["institution_ids"].str.split(",")
        df_authorships = df_authorships.explode("institution_ids").reset_index(drop=True)
        df_authorships = df_authorships.rename(
            columns={
                "institution_ids": "institution_id",
            }
        )
        df_authorships.to_csv(
            self.MERGED_DATA_DIRECTORY / "authorships.tsv",
            sep="\t",
            index=False,
        )
        # fmt: on

    def remove_invalid_authorships(self):
        """
        Remove authorship.tsv entries from institutions not in institutions.tsv
        """
        df_authorships = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / "authorships.tsv",
            sep="\t",
            dtype=str,
        )
        df_institutions = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / "institutions.tsv",
            sep="\t",
            dtype=str,
        )

        valid_institutions = df_institutions["scopus_id"].unique().tolist()
        df_filtered_authorships = df_authorships[
            df_authorships["institution_id"].isnull()
            | df_authorships["institution_id"].isin(valid_institutions)
        ]

        n_authorships = len(df_authorships)
        n_filtered_authorships = len(df_filtered_authorships)
        removed_authorships_percentage = round(
            (n_authorships - n_filtered_authorships) / n_authorships * 100,
            5,  # Round to five decimal places
        )
        log(
            logging.INFO,
            self.timestamp,
            f"number of authorships before cleaning: {n_authorships}",
        )
        log(
            logging.INFO,
            self.timestamp,
            f"number of authorships after cleaning: {n_filtered_authorships}",
        )
        log(
            logging.INFO,
            self.timestamp,
            f"percentage of authorships removed: {removed_authorships_percentage}%",
        )

        df_filtered_authorships.to_csv(
            self.MERGED_DATA_DIRECTORY / "authorships.tsv",
            sep="\t",
            index=False,
        )

    def drop_duplicates(self):
        TABLE_PREFIXES = ["documents", "institutions", "authors", "authorships"]

        for prefix in TABLE_PREFIXES:
            df = pd.read_csv(
                self.MERGED_DATA_DIRECTORY / f"{prefix}.tsv",
                sep="\t",
                dtype=str,
            )

            if prefix == "authorships":
                df = df.drop_duplicates()
            else:
                df = df.drop_duplicates(subset="scopus_id", keep="first")

            df.to_csv(
                self.MERGED_DATA_DIRECTORY / f"{prefix}.tsv",
                sep="\t",
                index=False,
            )

    def transform(self):
        # Do nothing if data has already been processed
        if not is_empty(self.PROCESSED_DATA_DIRECTORY):
            log(
                logging.INFO,
                self.timestamp,
                "Skipped data processing, because data has already been processed.",
            )
            return

        self.process()

        # Do nothing more if data has already been merged
        if not is_empty(self.MERGED_DATA_DIRECTORY):
            log(
                logging.INFO,
                self.timestamp,
                "Skipped data merging, because data has already been merged.",
            )
            return

        self.merge()
        self.normalize()
        self.remove_invalid_authorships()
        self.drop_duplicates()


class RDBMSTransformer(Transformer):
    pass


class GDBMSTransformer(Transformer):
    def __init__(
        self,
        timestamp: str,
        start_year: int,
        end_year: int,
        data_directory: Path,
    ) -> None:
        super().__init__(timestamp, start_year, end_year, data_directory)

        self.NEO4J_DATA_DIRECTORY: Path = neo4j_data_directory(
            self.timestamp, start_year, end_year, data_directory
        )
        self.NEO4J_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

    def format_neo4j_import(self):
        # Load cleaned authorships
        df_authorships = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / "authorships.tsv",
            sep="\t",
            dtype=str,
        )

        # Generate UUIDs for authorship nodes
        df_authorships["id:ID(Authorship)"] = [
            str(uuid.uuid4()) for _ in range(len(df_authorships))
        ]

        # Save authorship nodes
        df_authorship_nodes = df_authorships[
            [
                "id:ID(Authorship)",
                "author_id",
                "document_id",
                "institution_id",
                "first_author",
            ]
        ]
        df_authorship_nodes.to_csv(
            self.NEO4J_DATA_DIRECTORY / "node_authorships.tsv",
            sep="\t",
            index=False,
        )

        # Load cleaned authors
        nodes = ["authors", "documents", "institutions"]
        for node in nodes:
            df = pd.read_csv(
                self.MERGED_DATA_DIRECTORY / f"{node}.tsv",
                sep="\t",
                dtype=str,
            )

            df = df.rename(
                columns={
                    "scopus_id": f"scopus_id:ID({node.capitalize()[:-1]})",
                }
            )

            df.to_csv(
                self.NEO4J_DATA_DIRECTORY / f"node_{node}.tsv",
                sep="\t",
                index=False,
            )

        # Create relationship files
        def rel_df(start_col, end_col):
            df = pd.DataFrame(
                {
                    ":START_ID(Authorship)": df_authorships["id:ID(Authorship)"],
                    f":END_ID({end_col})": df_authorships[f"{end_col.lower()}_id"],
                }
            )
            df = df[df[f":END_ID({end_col})"].notna()]
            return df

        rel_df("id", "Author").to_csv(
            self.NEO4J_DATA_DIRECTORY / "rel_authorship_author.tsv",
            sep="\t",
            index=False,
        )
        rel_df("id", "Document").to_csv(
            self.NEO4J_DATA_DIRECTORY / "rel_authorship_document.tsv",
            sep="\t",
            index=False,
        )
        rel_df("id", "Institution").to_csv(
            self.NEO4J_DATA_DIRECTORY / "rel_authorship_institution.tsv",
            sep="\t",
            index=False,
        )

    def add_neo4j_author_edges(self):
        with Session(engine) as session:
            data = session.exec(
                text(
                    """
                        SELECT
                            a1.scopus_id AS author1_id,
                            a2.scopus_id AS author2_id,
                            COUNT(DISTINCT au1.document_id) AS collaboration_count
                        FROM
                            authorship au1
                        JOIN
                            authorship au2 ON au1.document_id = au2.document_id AND au1.author_id < au2.author_id
                        JOIN
                            author a1 ON au1.author_id = a1.scopus_id
                        JOIN
                            author a2 ON au2.author_id = a2.scopus_id
                        GROUP BY
                            a1.scopus_id, a2.scopus_id
                        """
                )  # type: ignore
            )

        df = pd.DataFrame(
            data,
            columns=[
                ":START_ID(Author)",
                ":END_ID(Author)",
                "count:int",
            ],  # type: ignore
        )
        df.to_csv(
            self.NEO4J_DATA_DIRECTORY / "rel_author_author.tsv",
            sep="\t",
            index=False,
        )

    def transform(self):
        super().transform()

        # Do nothing if data has already been formatted for neo4j import
        if not is_empty(self.NEO4J_DATA_DIRECTORY):
            log(
                logging.INFO,
                self.timestamp,
                "Skipped data formatting for neo4j, because that has already been done.",
            )
            return

        self.format_neo4j_import()
        self.add_neo4j_author_edges()
