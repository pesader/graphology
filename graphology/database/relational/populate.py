import logging
from pathlib import Path

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session
import pandas as pd

from graphology.acquisition.processing import PROCESSED_DATA_DIRECTORY
from graphology.database.relational.create import engine, init_db
from graphology.database.relational.entities import (
    Author,
    Authorship,
    Document,
    Institution,
)


def _batch_insert(model, mappings):
    with Session(engine) as session:
        try:
            session.bulk_insert_mappings(model, mappings)
            session.commit()
        except IntegrityError as e:
            logging.warning(
                f"Ignored duplicated and/or invalid entry for {model}. Exception caught: {e}"
            )
            session.rollback()


def populate_institutions():
    df = pd.read_csv(
        PROCESSED_DATA_DIRECTORY / Path("affiliations.tsv"),
        sep="\t",
    )
    df = df.rename(columns={"affiliation_id": "scopus_id"})
    df = df.drop_duplicates(subset="scopus_id", keep="first")
    df = df.where(pd.notnull(df), None)

    mappings = df.to_dict(orient="records")
    _batch_insert(Institution, mappings)


def populate_authors():
    df = pd.read_csv(
        PROCESSED_DATA_DIRECTORY / Path("authorships.tsv"),
        sep="\t",
    )
    df = df.drop(columns=["eid", "affiliation_id", "first_author"])
    df = df.rename(
        columns={
            "author_name": "name",
            "author_id": "scopus_id",
        }
    )
    df = df.drop_duplicates(subset="scopus_id", keep="first")
    df = df.where(pd.notnull(df), None)

    mappings = df.to_dict(orient="records")
    _batch_insert(Author, mappings)


def populate_documents():
    df = pd.read_csv(
        PROCESSED_DATA_DIRECTORY / Path("documents.tsv"),
        sep="\t",
    )
    df = df.drop(columns=["first_author"])
    df["scopus_id"] = df["eid"].str.removeprefix("2-s2.0-")
    df = df.drop_duplicates(subset="scopus_id", keep="first")
    df = df.where(pd.notnull(df), None)

    mappings = df.to_dict(orient="records")
    _batch_insert(Document, mappings)


def populate_authorships():
    df = pd.read_csv(
        PROCESSED_DATA_DIRECTORY / Path("authorships.tsv"),
        sep="\t",
        dtype={"affiliation_id": str},
    )
    df = df.drop(columns=["author_name"])
    df["document_id"] = df["eid"].str.removeprefix("2-s2.0-")
    df = df.drop(columns=["eid"])
    df = df.rename(
        columns={
            "affiliation_id": "institution_id",
        }
    )
    df = df.drop_duplicates()
    df = df.where(pd.notnull(df), None)

    mappings = df.to_dict(orient="records")
    _batch_insert(Authorship, mappings)


def main():
    init_db()

    # Independent entities
    populate_authors()
    populate_documents()
    populate_institutions()

    # Associative entity
    populate_authorships()


if __name__ == "__main__":
    main()
