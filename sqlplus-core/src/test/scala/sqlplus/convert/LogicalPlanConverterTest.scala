package sqlplus.convert

import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test
import sqlplus.catalog.CatalogManager
import sqlplus.expression.VariableManager
import sqlplus.graph.{AuxiliaryRelation, TableScanRelation}
import sqlplus.parser.SqlPlusParser
import sqlplus.plan.SqlPlusPlanner

class LogicalPlanConverterTest {
    @Test
    def testLine3(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT g1.src AS a, g1.dst AS b, g2.dst AS c, g3.dst AS d
              |FROM Graph AS g1, Graph AS g2, Graph AS g3
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g1.src < g3.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 4)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 3)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g1"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g2"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g3"))

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g1")))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g2")))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g3")))
    }

    @Test
    def testLine4Project1(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT g2.src, g2.dst
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 2)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g2"))

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.head.getTableDisplayName() == "g2")
    }

    @Test
    def testLine4Project2(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT g2.dst
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 1)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 1)
        val root = runResult.joinTreesWithComparisonHyperGraph.head._1.root
        assertTrue(root.isInstanceOf[AuxiliaryRelation])
        assertTrue(root.getVariableList().size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.contains(root))
    }

    @Test
    def testFreeConnex(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT g2.src, g2.dst, g3.dst
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 3)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 2)
        val jointree1 = runResult.joinTreesWithComparisonHyperGraph.find(t => t._1.root.getTableDisplayName() == "g2").get._1
        assertTrue(jointree1.subset.size == 2)
        assertTrue(jointree1.subset.exists(r => r.getTableDisplayName() == "g2"))
        assertTrue(jointree1.subset.exists(r => r.getTableDisplayName() == "g3"))

        val jointree2 = runResult.joinTreesWithComparisonHyperGraph.find(t => t._1.root.getTableDisplayName() == "g3").get._1
        assertTrue(jointree2.subset.size == 2)
        assertTrue(jointree2.subset.exists(r => r.getTableDisplayName() == "g2"))
        assertTrue(jointree2.subset.exists(r => r.getTableDisplayName() == "g3"))
    }

    @Test
    def testAggregation1(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT g2.src, SUM(g2.dst)
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |GROUP BY g2.src
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFreeConnex)

        assertTrue(runResult.outputVariables.size == 2)
        assertTrue(runResult.groupByVariables.size == 1)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations.head._2 == "SUM")

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 1)
        val root = runResult.joinTreesWithComparisonHyperGraph.head._1.root
        assertTrue(root.isInstanceOf[AuxiliaryRelation])
        assertTrue(root.getVariableList().size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.contains(root))
    }

    @Test
    def testAggregation2(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT g2.src, SUM(g3.dst)
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |GROUP BY g2.src, g2.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFreeConnex)

        assertTrue(runResult.outputVariables.size == 2)
        assertTrue(runResult.groupByVariables.size == 2)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations.head._2 == "SUM")

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 1)
        val root = runResult.joinTreesWithComparisonHyperGraph.head._1.root
        assertTrue(root.isInstanceOf[TableScanRelation])
        assertTrue(root.getVariableList().size == 2)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.contains(root))
    }

    @Test
    def testOutputVariablesOrder(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT COUNT(*), g2.src, SUM(g3.dst), AVG(g4.dst + g1.src)
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |GROUP BY g2.src, g2.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.groupByVariables.size == 2)
        assertTrue(runResult.aggregations.size == 3)
        assertTrue(runResult.aggregations(0)._2 == "COUNT")
        assertTrue(runResult.aggregations(1)._2 == "SUM")
        assertTrue(runResult.aggregations(2)._2 == "AVG")
        assertTrue(runResult.outputVariables.size == 4)
        assertTrue(runResult.outputVariables(0) == runResult.aggregations(0)._1)
        assertTrue(runResult.outputVariables(1) == runResult.groupByVariables(0))
        assertTrue(runResult.outputVariables(2) == runResult.aggregations(1)._1)
        assertTrue(runResult.outputVariables(3) == runResult.aggregations(2)._1)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 1)
        val root = runResult.joinTreesWithComparisonHyperGraph.head._1.root
        assertTrue(root.isInstanceOf[TableScanRelation])
        assertTrue(root.getVariableList().size == 2)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.size == 1)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.head._1.subset.contains(root))
    }

    @Test
    def testNonFreeConnexAggregation(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT COUNT(*)
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |GROUP BY g2.src, g3.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFreeConnex)

        assertTrue(runResult.groupByVariables.size == 2)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations(0)._2 == "COUNT")
        assertTrue(runResult.outputVariables.size == 1)
        assertTrue(runResult.outputVariables(0) == runResult.aggregations(0)._1)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 2)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g2"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g3"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.size == 2))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g2")))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g3")))
    }

    @Test
    def testAggregationWithoutGroupBy(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph (
              |    src INT,
              |    dst INT
              |) WITH (
              |    'path' = 'examples/data/graph.dat'
              |)
              |""".stripMargin
        val dml =
            """
              |SELECT COUNT(*)
              |FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
              |WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFreeConnex)

        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations(0)._2 == "COUNT")
        assertTrue(runResult.outputVariables.size == 1)
        assertTrue(runResult.outputVariables(0) == runResult.aggregations(0)._1)

        assertTrue(runResult.joinTreesWithComparisonHyperGraph.size == 4)
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g1"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g2"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g3"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.exists(t => t._1.root.getTableDisplayName() == "g4"))
        assertTrue(runResult.joinTreesWithComparisonHyperGraph.forall(t => t._1.subset.isEmpty))
    }
}
