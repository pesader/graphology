from graphology.etl.load.gdbms.database import driver

with driver.session() as session:
    # 1. Drop graph if it exists
    session.run(
        """
        CALL gds.graph.exists('authorGraph') YIELD exists 
        WITH exists WHERE exists 
        CALL gds.graph.drop('authorGraph') YIELD graphName 
        RETURN graphName
        """
    )

    # 2. Project the graph
    session.run(
        """
        CALL gds.graph.project(
          'authorGraph',
          {
            Author: {
              properties: ['community_labelprop']
            }
          },
          {
            COLLABORATED_WITH: {
              orientation: 'UNDIRECTED'
            }
          }
        )
        """
    )

    # 3. Run label propagation
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

    # 4. Compute modularity
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
