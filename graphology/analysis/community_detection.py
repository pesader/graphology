from graphology.etl.load.gdbms.database import driver
from graphology import log
import logging


def _community_label(algorithm_name: str, iteration: int) -> str:
    return f"community_{algorithm_name}_{iteration}"


def analyze():
    with driver.session() as session:
        # 1. Delete projection if it already exists
        session.run(
            """
            WITH 'authorGraph' AS graphName
            CALL gds.graph.exists(graphName) YIELD exists
            WITH graphName, exists
            WHERE exists
            CALL gds.graph.drop(graphName) YIELD graphName AS _
            RETURN null
            """
        )

        # 2. Create projection if it doesn't exist
        session.run(
            """
            MATCH (a1:Author)-[r:COLLABORATED_WITH]->(a2:Author)
            WHERE r.count >= 2
            WITH a1, a2, r
            RETURN gds.graph.project(
              'authorGraph',
              a1,
              a2,
              {
                relationshipProperties: r { .count }
              },
              {
                  undirectedRelationshipTypes: ['*']
              }
            )
            """
        )

        algorithms = {
            "labelPropagation": {
                "query": """
                    CALL gds.labelPropagation.mutate(
                        'authorGraph',
                        {
                            mutateProperty: $label,
                            relationshipWeightProperty: 'count'
                        }
                    )
                    """,
                "modularity_in_stats": False,
            },
            "louvain": {
                "query": """
                    CALL gds.louvain.mutate(
                        'authorGraph',
                        {
                            mutateProperty: $label,
                            relationshipWeightProperty: 'count'
                        }
                    ) YIELD modularity as totalModularity
                    """,
                "modularity_in_stats": True,
            },
            "leiden": {
                "query": """
                    CALL gds.leiden.mutate(
                        'authorGraph',
                        {
                            mutateProperty: $label,
                            relationshipWeightProperty: 'count'
                        }
                    ) YIELD modularity as totalModularity
                    """,
                "modularity_in_stats": True,
            },
        }

        # 3. Run community detection algorithms
        label_to_modularity: dict[str, float] = {}
        for name, props in algorithms.items():

            # Repeat stochastic algorithms
            TIMES: int = 10
            for i in range(1, TIMES + 1):
                label = _community_label(name, i)

                result = session.run(
                    props["query"],
                    label=label,
                )

                if not props["modularity_in_stats"]:
                    # 4. Compute modularity
                    result = session.run(
                        """
                        CALL gds.modularity.stream('authorGraph', {
                          communityProperty: $label,
                          relationshipWeightProperty: 'count'
                        })
                        YIELD modularity
                        RETURN sum(modularity) as totalModularity
                        """,
                        label=label,
                    )

                modularity = result.single()["totalModularity"]  # type:ignore
                label_to_modularity[label] = modularity

                log(
                    f"{name} modularity (iteration {i}): {modularity}",
                )

        best = max(label_to_modularity, key=label_to_modularity.get)  # type:ignore

        log(f"best was {best} with modularity {label_to_modularity[best]}")

        session.run(
            f"""
            CALL gds.graph.nodeProperties.write('authorGraph', [
                {{
                    {best}: "community"
                }}
            ])
            YIELD nodeProperties
            """,  # type:ignore
        )


if __name__ == "__main__":
    analyze()
