from graphology.etl.load.gdbms.database import driver


def rename():
    with driver.session() as session:
        # 1. Create projection if it doesn't exist
        session.run(
            """
            MATCH (n:Author)
            WHERE n.community_louvain_7 IS NOT NULL
            SET n.best_community = n.community_louvain_7
            REMOVE n.community_louvain_7
            """
        )


def _community_label(algorithm_name: str, iteration: int) -> str:
    return f"community_{algorithm_name}_{iteration}"


def analyze():
    with driver.session() as session:
        # 1. Create projection if it doesn't exist
        session.run(
            """
            CALL () {
              WITH 'authorGraph' AS graphName
              CALL gds.graph.exists(graphName) YIELD exists
              WITH graphName, exists
              WHERE NOT exists
              CALL gds.graph.project(
                graphName,
                {
                  Author: { properties: ['community_labelprop'] }
                },
                {
                  COLLABORATED_WITH: { orientation: 'UNDIRECTED' }
                }
              )
              YIELD graphName AS createdGraph
              RETURN createdGraph
            }
            RETURN 'Projection step completed'
            """
        )

        algorithms = {
            "labelPropagation": {
                "query": f"""
                    CALL gds.labelPropagation.write(
                        'authorGraph',
                        {{
                            writeProperty: $label
                        }}
                    )
                    """,
                "modularity_in_stats": False,
            },
            "louvain": {
                "query": f"""
                    CALL gds.louvain.write(
                        'authorGraph',
                        {{
                            writeProperty: $label
                        }}
                    ) YIELD modularity as totalModularity
                    """,
                "modularity_in_stats": True,
            },
            "leiden": {
                "query": f"""
                    CALL gds.leiden.write(
                        'authorGraph',
                        {{
                            writeProperty: $label
                        }}
                    ) YIELD modularity as totalModularity
                    """,
                "modularity_in_stats": True,
            },
        }

        # 2. Run label propagation
        for name, props in algorithms.items():

            # Only repeat stochastic algorithms
            times: int
            match (name):
                case "labelPropagation":
                    times = 10
                case "louvain":
                    times = 10
                case "leiden":
                    times = 25
                case _:
                    times = 1

            for i in range(1, times + 1):
                result = session.run(
                    props["query"],
                    label=_community_label(name, i),
                )

                if not props["modularity_in_stats"]:
                    # 3. Compute modularity
                    result = session.run(
                        """
                        CALL gds.modularity.stream('authorGraph', {
                          communityProperty: 'community_labelprop'
                        })
                        YIELD modularity
                        RETURN sum(modularity) as totalModularity
                        """
                    )

                print(
                    f"{name} modularity (iteration {i}):",
                    result.single()["totalModularity"],
                )


if __name__ == "__main__":
    analyze()
