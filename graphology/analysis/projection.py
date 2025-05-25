from sqlmodel import Session, text

from graphology.etl.load.rdbms.database import engine
from graphology.etl.load.gdbms.database import driver


def get_projection_edges_from_rdbms():
    with Session(engine) as session:
        result = session.exec(
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
                ORDER BY
                    collaboration_count DESC
                LIMIT 100;
                """
            )  # type: ignore
        )

    for row in result:
        if row[2]:
            print(row)


def get_projection_edges_from_gdbms():
    query = """
        MATCH (a1:Author)<-[:INVOLVES_AUTHOR]-(auth1:Authorship)-[:INVOLVES_DOCUMENT]->(d:Document)<-[:INVOLVES_DOCUMENT]-(auth2:Authorship)-[:INVOLVES_AUTHOR]->(a2:Author)
        WHERE a1.scopus_id < a2.scopus_id
        WITH DISTINCT a1, a2, d
        RETURN 
            a1.scopus_id AS author1_id,
            a2.scopus_id AS author2_id,
            count(d) AS collaboration_count
        ORDER BY collaboration_count DESC
        LIMIT 100
        """

    with driver.session() as session:
        result = session.run(query)
        rows = [record.data() for record in result]

    for row in rows:
        print(row)


def hunting_fakes():
    query = """
        MATCH (a:Author)<-[:INVOLVES_AUTHOR]-(auth:Authorship)-[:INVOLVES_DOCUMENT]->(d:Document)
        WITH a, d, count(*) AS c
        WHERE c > 3
        RETURN a.scopus_id, d.scopus_id, c
        ORDER BY c DESC
        """

    with driver.session() as session:
        result = session.run(query)
        rows = [record.data() for record in result]

    for row in rows:
        print("Ten authors with the most institutions in the same document")
        print(row)


if __name__ == "__main__":
    get_projection_edges_from_rdbms()
