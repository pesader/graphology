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

    def load(self):
        self._run_neo4j_admin()
