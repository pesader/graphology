import uuid
import logging
import subprocess

from .database import driver
from graphology import log


class GDBMSLoader:
    def __init__(self, timestamp: str):
        self.timestamp: str = timestamp

    def _run_neo4j_admin(self) -> None:
        """Runs a bash command and returns its output."""
        print('The "neo4j-admin" command requires privilege to run.')
        command = f"""
        sudo neo4j-admin database import full \
          --delimiter="\t" \
          --nodes=Author=data/{self.timestamp}/neo4j/node_authors.tsv \
          --nodes=Document=data/{self.timestamp}/neo4j/node_documents.tsv \
          --nodes=Institution=data/{self.timestamp}/neo4j/node_institutions.tsv \
          --nodes=Authorship=data/{self.timestamp}/neo4j/node_authorships.tsv \
          --relationships=INVOLVES_AUTHOR=data/{self.timestamp}/neo4j/rel_authorship_author.tsv \
          --relationships=INVOLVES_DOCUMENT=data/{self.timestamp}/neo4j/rel_authorship_document.tsv \
          --relationships=INVOLVES_INSTITUTION=data/{self.timestamp}/neo4j/rel_authorship_institution.tsv \
          --relationships=COLLABORATED_WITH=data/{self.timestamp}/neo4j/rel_author_author.tsv \
          --overwrite-destination \
          --verbose
        """
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        log(logging.INFO, self.timestamp, result.stdout)

        if result.returncode != 0:
            log(logging.ERROR, self.timestamp, result.stderr)
            raise Exception("Unable to populate neo4j database")

    def load(self):
        self._run_neo4j_admin()
