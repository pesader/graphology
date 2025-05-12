import uuid
from pathlib import Path
import pandas as pd

from graphology.etl._helpers import merged_data_directory
from .database import driver


class GDBMSLoader:
    def __init__(self, timestamp: str):
        self.MERGED_DATA_DIRECTORY: Path = merged_data_directory(timestamp)
        self.MERGED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

    def load(self):
        # Load TSVs
        df_authors = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("authors.tsv"),
            sep="\t",
        )
        df_documents = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("documents.tsv"),
            sep="\t",
        )
        df_institutions = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("institutions.tsv"),
            sep="\t",
        )
        df_authorships = pd.read_csv(
            self.MERGED_DATA_DIRECTORY / Path("authorships.tsv"),
            sep="\t",
            dtype={"institution_id": str},
        )

        with driver.session() as session:
            # Create Author nodes
            for _, row in df_authors.iterrows():
                session.run(
                    """
                    MERGE (a:Author {scopus_id: $scopus_id})
                    SET a.name = $name
                """,
                    {
                        "scopus_id": str(row["scopus_id"]),
                        "name": row["name"],
                    },
                )

            # Create Document nodes
            for _, row in df_documents.iterrows():
                session.run(
                    """
                    MERGE (d:Document {scopus_id: $scopus_id})
                    SET d += $props
                """,
                    {
                        "scopus_id": row["scopus_id"],
                        "props": row.drop("scopus_id").to_dict(),
                    },
                )

            # Create Institution nodes
            for _, row in df_institutions.iterrows():
                session.run(
                    """
                    MERGE (i:Institution {scopus_id: $scopus_id})
                    SET i += $props
                """,
                    {
                        "scopus_id": str(row["scopus_id"]),
                        "props": row.drop("scopus_id").to_dict(),
                    },
                )

            # Create Authorship relationships
            for _, row in df_authorships.iterrows():
                author_id = row["author_id"]
                document_id = row["document_id"]
                institution_id = row["institution_id"]
                first_author = row["first_author"]

                # UUID as authorship primary key
                authorship_id = str(uuid.uuid4())

                session.run(
                    """
                    CREATE (authorship:Authorship {
                        id: $authorship_id,
                        first_author: $first_author
                    })
                    WITH authorship
                    MATCH (a:Author {scopus_id: $author_id}),
                          (d:Document {scopus_id: $document_id}),
                          (i:Institution {scopus_id: $institution_id})
                    MERGE (authorship)-[:INVOLVES_AUTHOR]->(a)
                    MERGE (authorship)-[:INVOLVES_DOCUMENT]->(d)
                    MERGE (authorship)-[:INVOLVES_INSTITUTION]->(i)
                """,
                    {
                        "authorship_id": authorship_id,
                        "first_author": first_author,
                        "author_id": author_id,
                        "document_id": document_id,
                        "institution_id": institution_id,
                    },
                )
