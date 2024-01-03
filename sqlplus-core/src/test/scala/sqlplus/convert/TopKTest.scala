package sqlplus.convert

import org.junit.Assert.assertTrue
import org.junit.Test
import sqlplus.catalog.CatalogManager
import sqlplus.expression.VariableManager
import sqlplus.parser.SqlPlusParser
import sqlplus.plan.SqlPlusPlanner

class TopKTest {
    @Test
    def testL2TopK(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    src    INT,
              |    dst    INT,
              |    rating DECIMAL
              |) WITH (
              |      'path' = 'examples/data/graph.dat'
              |      )
              |""".stripMargin

        val query =
            """
              |SELECT R.src AS node1, S.src AS node2, S.dst AS node3, R.rating + S.rating AS total_rating
              |FROM graph R,
              |     graph S
              |WHERE R.dst = S.src
              |ORDER BY total_rating DESC limit 5
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 4)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 2)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "S"))
    }

    @Test
    def testL3TopK(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    src    INT,
              |    dst    INT,
              |    rating DECIMAL
              |) WITH (
              |      'path' = 'examples/data/graph.dat'
              |      )
              |""".stripMargin

        val query =
            """
              |SELECT R.src AS node1, S.src AS node2, T.src AS node3, T.dst AS node4, R.rating + S.rating + T.rating AS total_rating
              |FROM graph R,
              |     graph S,
              |     graph T
              |WHERE R.dst = S.src
              |  AND S.dst = T.src
              |ORDER BY total_rating DESC limit 10
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 5)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 3)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "S"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "T"))
    }

    @Test
    def testL4TopK(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    src    INT,
              |    dst    INT,
              |    rating DECIMAL
              |) WITH (
              |      'path' = 'examples/data/graph.dat'
              |      )
              |""".stripMargin

        val query =
            """
              |SELECT R.src                                     AS node1,
              |       S.src                                     AS node2,
              |       T.src                                     AS node3,
              |       U.src                                     AS node4,
              |       U.dst                                     AS node5,
              |       R.rating + S.rating + T.rating + U.rating AS total_rating
              |FROM graph R,
              |     graph S,
              |     graph T,
              |     graph U
              |WHERE R.dst = S.src
              |  AND S.dst = T.src
              |  AND T.dst = U.src
              |ORDER BY total_rating DESC limit 7
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 6)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 4)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "S"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "T"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "U"))
    }
}
