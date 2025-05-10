import logging
from pathlib import Path

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session
import pandas as pd

from graphology.etl.load.rdbms.database import engine, init_db
from graphology.etl._helpers import merged_data_directory
from graphology.etl.load.rdbms.entities import (
    Author,
    Authorship,
    Document,
    Institution,
)


class RDBMSLoader:
    def __init__(
        self,
        timestamp: str,
    ) -> None:
        self.MERGED_DATA_DIRECTORY: Path = merged_data_directory(timestamp)
        self.MERGED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

        init_db()

    def _batch_insert(self, model, mappings):
        with Session(engine) as session:
            try:
                session.bulk_insert_mappings(model, mappings)
                session.commit()
            except IntegrityError as e:
                logging.warning(
                    f"Ignored duplicated and/or invalid entry for {model}. Exception caught: {e}"
                )
                session.rollback()

    def _populate_institutions(self):
        df = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("affiliations.tsv"),
            sep="\t",
        )
        df = df.where(pd.notnull(df), None)

        mappings = df.to_dict(orient="records")
        self._batch_insert(Institution, mappings)

    def _populate_authors(self):
        df = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("authors.tsv"),
            sep="\t",
        )
        df = df.where(pd.notnull(df), None)

        mappings = df.to_dict(orient="records")
        self._batch_insert(Author, mappings)

    def _populate_documents(self):
        df = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("documents.tsv"),
            sep="\t",
        )
        df = df.where(pd.notnull(df), None)

        mappings = df.to_dict(orient="records")
        self._batch_insert(Document, mappings)

    def _populate_authorships(self):
        df = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("authorships.tsv"),
            sep="\t",
            dtype={"affiliation_id": str},
        )
        df = df.drop_duplicates()
        df = df.where(pd.notnull(df), None)

        mappings = df.to_dict(orient="records")
        self._batch_insert(Authorship, mappings)

    def load(self):
        # Independent entities
        self._populate_authors()
        self._populate_documents()
        self._populate_institutions()

        # Associative entity
        self._populate_authorships()
