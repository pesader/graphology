from pathlib import Path
import uuid
import logging
import subprocess

from graphology.etl._helpers import neo4j_data_directory

from .database import driver
from graphology import log


class GDBMSLoader:
    def __init__(
        self,
        timestamp: str,
        start_year: int,
        end_year: int,
        data_directory: Path,
    ):
        self.timestamp = timestamp
        self.NEO4J_DATA_DIRECTORY: Path = neo4j_data_directory(
            timestamp, start_year, end_year, data_directory
        )

    def _run_neo4j_admin(self) -> None:
        """Runs a bash command and returns its output."""
        command = f"""
        sudo neo4j stop &&
        sudo neo4j-admin database import full \
          --delimiter="\t" \
          --nodes=Author={self.NEO4J_DATA_DIRECTORY}/node_authors.tsv \
          --nodes=Document={self.NEO4J_DATA_DIRECTORY}/node_documents.tsv \
          --nodes=Institution={self.NEO4J_DATA_DIRECTORY}/node_institutions.tsv \
          --nodes=Authorship={self.NEO4J_DATA_DIRECTORY}/node_authorships.tsv \
          --relationships=INVOLVES_AUTHOR={self.NEO4J_DATA_DIRECTORY}/rel_authorship_author.tsv \
          --relationships=INVOLVES_DOCUMENT={self.NEO4J_DATA_DIRECTORY}/rel_authorship_document.tsv \
          --relationships=INVOLVES_INSTITUTION={self.NEO4J_DATA_DIRECTORY}/rel_authorship_institution.tsv \
          --relationships=COLLABORATED_WITH={self.NEO4J_DATA_DIRECTORY}/rel_author_author.tsv \
          --overwrite-destination \
          --verbose
        """
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception("Unable to populate neo4j database")

        log(
            logging.INFO,
            self.timestamp,
            f"finished populating graph database",
        )

    def _create_indexes(self) -> None:
        def create_author_indexes(tx):
            statements = [
                "CREATE INDEX range_author_scopus_id FOR (a:Author) ON (a.scopus_id)",
                "CREATE INDEX range_author_name FOR (a:Author) ON (a.name)",
            ]
            for s in statements:
                tx.run(s)

            log(
                logging.INFO,
                self.timestamp,
                f"finished creating indexes for Author nodes",
            )

        def create_document_indexes(tx):
            statements = [
                "CREATE INDEX range_document_scopus_id FOR (d:Document) ON (d.scopus_id)",
                "CREATE INDEX range_document_doi FOR (d:Document) ON (d.doi)",
                "CREATE INDEX range_document_title FOR (d:Document) ON (d.title)",
            ]
            for s in statements:
                tx.run(s)

            log(
                logging.INFO,
                self.timestamp,
                f"finished creating indexes for Document nodes",
            )

        def create_institution_indexes(tx):
            statements = [
                "CREATE INDEX range_institution_scopus_id FOR (i:Institution) ON (i.scopus_id)",
                "CREATE INDEX range_institution_name FOR (i:Institution) ON (i.name)",
                "CREATE INDEX range_institution_city FOR (i:Institution) ON (i.city)",
                "CREATE INDEX range_institution_country FOR (i:Institution) ON (i.country)",
            ]
            for s in statements:
                tx.run(s)

            log(
                logging.INFO,
                self.timestamp,
                f"finished creating indexes for Intitution nodes",
            )

        def create_authorship_indexes(tx):
            statements = [
                "CREATE INDEX range_authorship_author_id FOR (auth:Authorship) ON (auth.author_id)",
                "CREATE INDEX range_authorship_document_id FOR (auth:Authorship) ON (auth.document_id)",
                "CREATE INDEX range_authorship_institution_id FOR (auth:Authorship) ON (auth.institution_id)",
            ]
            for s in statements:
                tx.run(s)

            log(
                logging.INFO,
                self.timestamp,
                f"finished creating indexes for Authorship nodes",
            )

        # First, start the database
        command = "sudo neo4j start"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception("Unable to create neo4j indexes")

        # Then, run the indexing commands
        indexing_functions = [
            create_author_indexes,
            create_document_indexes,
            create_institution_indexes,
            create_authorship_indexes,
        ]
        with driver.session() as session:
            for f in indexing_functions:
                session.execute_write(f)

        log(
            logging.INFO,
            self.timestamp,
            f"finished creating all indexes",
        )

    def load(self):
        self._run_neo4j_admin()
        self._create_indexes()
