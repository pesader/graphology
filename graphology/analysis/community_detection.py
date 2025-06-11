from graphology.etl.load.gdbms.database import driver


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
            # "labelPropagation": {
            #     "query": f"""
            #         CALL gds.labelPropagation.write(
            #             'authorGraph',
            #             {{
            #                 writeProperty: $label,
            #                 relationshipWeightProperty: 'count'
            #             }}
            #         )
            #         """,
            #     "modularity_in_stats": False,
            # },
            "louvain": {
                "query": f"""
                    CALL gds.louvain.write(
                        'authorGraph',
                        {{
                            writeProperty: $label,
                            relationshipWeightProperty: 'count'
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
                            writeProperty: $label,
                            relationshipWeightProperty: 'count'
                        }}
                    ) YIELD modularity as totalModularity
                    """,
                "modularity_in_stats": True,
            },
        }

        # 3. Run community detection algorithms
        for name, props in algorithms.items():

            # Repeat stochastic algorithms
            times: int
            match (name):
                case "labelPropagation":
                    times = 1
                case "louvain":
                    times = 1
                case "leiden":
                    times = 1
                case _:
                    times = 1

            for i in range(1, times + 1):
                result = session.run(
                    props["query"],
                    label=_community_label(name, i),
                )

                if not props["modularity_in_stats"]:
                    # 4. Compute modularity
                    result = session.run(
                        """
                        CALL gds.modularity.stream('authorGraph', {
                          communityProperty: $label
                        })
                        YIELD modularity
                        RETURN sum(modularity) as totalModularity
                        """,
                        label=_community_label(name, i),
                    )

                print(
                    f"{name} modularity (iteration {i}):",
                    result.single()["totalModularity"],
                )


if __name__ == "__main__":
    analyze()
