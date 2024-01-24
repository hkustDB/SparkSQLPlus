package sqlplus.convert

import org.junit.Assert.{assertEquals, assertTrue}
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 4)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 2)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "S"))

        assertEquals(1, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "R").get._1.getMaxFanout())
        assertEquals(1, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "S").get._1.getMaxFanout())
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 5)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 3)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "S"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "T"))

        assertEquals(1, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "R").get._1.getMaxFanout())
        assertEquals(2, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "S").get._1.getMaxFanout())
        assertEquals(1, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "T").get._1.getMaxFanout())
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 6)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 4)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "S"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "T"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "U"))

        assertEquals(1, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "R").get._1.getMaxFanout())
        assertEquals(2, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "S").get._1.getMaxFanout())
        assertEquals(2, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "T").get._1.getMaxFanout())
        assertEquals(1, runResult.candidates.find(t => t._1.root.getTableDisplayName() == "U").get._1.getMaxFanout())
    }

    @Test
    def testStarTopK(): Unit = {
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
              |SELECT R.src AS node1, R.dst AS node2, S.dst AS node3, T.dst AS node4, R.rating + S.rating + T.rating AS total_rating
              |FROM graph R,
              |     graph S,
              |     graph T
              |WHERE R.src = S.src
              |  AND R.src = T.src
              |ORDER BY total_rating DESC limit 5
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 5)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 9)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "S"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "T"))

        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "R" && t._1.getMaxFanout() == 1))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "R" && t._1.getMaxFanout() == 2))
        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "S" && t._1.getMaxFanout() == 1))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "S" && t._1.getMaxFanout() == 2))
        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "T" && t._1.getMaxFanout() == 1))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "T" && t._1.getMaxFanout() == 2))
    }

    @Test
    def testTreeTopK(): Unit = {
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
              |SELECT S.dst                                     AS A,
              |       S.src                                     AS B,
              |       R.src                                     AS C,
              |       T.dst                                     AS D,
              |       U.dst                                     AS E,
              |       R.rating + S.rating + T.rating + U.rating AS total_rating
              |FROM graph R,
              |     graph S,
              |     graph T,
              |     graph U
              |WHERE R.dst = S.src
              |  AND R.src = T.src
              |  AND R.src = U.src
              |ORDER BY total_rating DESC limit 5
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.outputVariables.size == 6)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 12)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "R"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "S"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "T"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "U"))

        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "R" && t._1.getMaxFanout() == 2))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "R" && t._1.getMaxFanout() == 3))
        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "S" && t._1.getMaxFanout() == 1))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "S" && t._1.getMaxFanout() == 2))
        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "T" && t._1.getMaxFanout() == 2))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "T" && t._1.getMaxFanout() == 1))
        assertEquals(2, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "U" && t._1.getMaxFanout() == 2))
        assertEquals(1, runResult.candidates.count(t => t._1.root.getTableDisplayName() == "U" && t._1.getMaxFanout() == 1))
    }
}
