from graphology.etl.load.gdbms.database import driver

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

    # 2. Run label propagation
    session.run(
        """
        CALL gds.labelPropagation.write(
          'authorGraph',
          {
            writeProperty: 'community_labelprop'
          }
        )
        """
    )

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

    print("Modularity:", result.single()["totalModularity"])
