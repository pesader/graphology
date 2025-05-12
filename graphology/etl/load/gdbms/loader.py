import uuid

from .database import driver


class GDBMSLoader:
    def __init__(self, timestamp: str):
        self.timestamp: str = timestamp

    def _populate_institutions(self):
        file_url = f"file:///{self.timestamp}/institutions.tsv"
        with driver.session() as session:
            session.run(
                """
                LOAD CSV WITH HEADERS FROM $file_url AS row FIELDTERMINATOR '\t'
                MERGE (i:Institution {scopus_id: row.scopus_id})
                SET i += row;
                """,
                {
                    "file_url": file_url,
                },
            )
        print("Finished populating Institutions!")

    def _populate_authors(self):
        file_url = f"file:///{self.timestamp}/authors.tsv"
        with driver.session() as session:
            session.run(
                """
                LOAD CSV WITH HEADERS FROM $file_url AS row FIELDTERMINATOR '\t'
                MERGE (a:Author {scopus_id: row.scopus_id})
                SET a.name = row.name;
                """,
                {
                    "file_url": file_url,
                },
            )
        print("Finished populating Authors!")

    def _populate_documents(self):
        file_url = f"file:///{self.timestamp}/documents.tsv"
        with driver.session() as session:
            session.run(
                """
                LOAD CSV WITH HEADERS FROM $file_url AS row FIELDTERMINATOR '\t'
                MERGE (d:Document {scopus_id: row.scopus_id})
                SET d += row;
                """,
                {
                    "file_url": file_url,
                },
            )
        print("Finished populating Documents!")

    def _populate_authorships(self):
        file_url = f"file:///{self.timestamp}/authorships.tsv"
        with driver.session() as session:
            session.run(
                """
                LOAD CSV WITH HEADERS FROM $file_url AS row FIELDTERMINATOR '\t'
                CREATE (authorship:Authorship {
                    id: $authorship_id,
                    first_author: row.first_author
                })
                WITH authorship, row
                MATCH (a:Author {scopus_id: row.author_id}),
                      (d:Document {scopus_id: row.document_id}),
                      (i:Institution {scopus_id: row.institution_id})
                MERGE (authorship)-[:INVOLVES_AUTHOR]->(a)
                MERGE (authorship)-[:INVOLVES_DOCUMENT]->(d)
                MERGE (authorship)-[:INVOLVES_INSTITUTION]->(i)
                """,
                {
                    "file_url": file_url,
                    "authorship_id": str(uuid.uuid4()),
                },
            )
        print("Finished populating Authorships!")

    def load(self):
        # Independent nodes
        self._populate_institutions()
        self._populate_authors()
        self._populate_documents()

        # Associative node
        self._populate_authorships()
