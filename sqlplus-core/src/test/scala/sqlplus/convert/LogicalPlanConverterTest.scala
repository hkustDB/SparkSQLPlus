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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 4)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 3)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g1"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g2"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g3"))

        assertTrue(runResult.candidates.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g1")))
        assertTrue(runResult.candidates.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g2")))
        assertTrue(runResult.candidates.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g3")))
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 2)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 1)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g2"))

        assertTrue(runResult.candidates.head._1.subset.size == 1)
        assertTrue(runResult.candidates.head._1.subset.head.getTableDisplayName() == "g2")
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 1)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 1)
        val root = runResult.candidates.head._1.root
        assertTrue(root.isInstanceOf[AuxiliaryRelation])
        assertTrue(root.getVariableList().size == 1)
        assertTrue(runResult.candidates.head._1.subset.size == 1)
        assertTrue(runResult.candidates.head._1.subset.contains(root))
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFull)
        assertTrue(runResult.isFreeConnex)
        assertTrue(runResult.outputVariables.size == 3)
        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.isEmpty)

        assertTrue(runResult.candidates.size == 2)
        val jointree1 = runResult.candidates.find(t => t._1.root.getTableDisplayName() == "g2").get._1
        assertTrue(jointree1.subset.size == 2)
        assertTrue(jointree1.subset.exists(r => r.getTableDisplayName() == "g2"))
        assertTrue(jointree1.subset.exists(r => r.getTableDisplayName() == "g3"))

        val jointree2 = runResult.candidates.find(t => t._1.root.getTableDisplayName() == "g3").get._1
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFreeConnex)

        assertTrue(runResult.outputVariables.size == 2)
        assertTrue(runResult.groupByVariables.size == 1)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations.head._2 == "SUM")

        assertTrue(runResult.candidates.size == 1)
        val root = runResult.candidates.head._1.root
        assertTrue(root.isInstanceOf[AuxiliaryRelation])
        assertTrue(root.getVariableList().size == 1)
        assertTrue(runResult.candidates.head._1.subset.size == 1)
        assertTrue(runResult.candidates.head._1.subset.contains(root))
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFreeConnex)

        assertTrue(runResult.outputVariables.size == 2)
        assertTrue(runResult.groupByVariables.size == 2)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations.head._2 == "SUM")

        assertTrue(runResult.candidates.size == 1)
        val root = runResult.candidates.head._1.root
        assertTrue(root.isInstanceOf[TableScanRelation])
        assertTrue(root.getVariableList().size == 2)
        assertTrue(runResult.candidates.head._1.subset.size == 1)
        assertTrue(runResult.candidates.head._1.subset.contains(root))
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
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

        assertTrue(runResult.candidates.size == 1)
        val root = runResult.candidates.head._1.root
        assertTrue(root.isInstanceOf[TableScanRelation])
        assertTrue(root.getVariableList().size == 2)
        assertTrue(runResult.candidates.head._1.subset.size == 1)
        assertTrue(runResult.candidates.head._1.subset.contains(root))
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertFalse(runResult.isFreeConnex)

        assertTrue(runResult.groupByVariables.size == 2)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations(0)._2 == "COUNT")
        assertTrue(runResult.outputVariables.size == 1)
        assertTrue(runResult.outputVariables(0) == runResult.aggregations(0)._1)

        assertTrue(runResult.candidates.size == 2)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g2"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g3"))
        assertTrue(runResult.candidates.forall(t => t._1.subset.size == 2))
        assertTrue(runResult.candidates.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g2")))
        assertTrue(runResult.candidates.forall(t => t._1.subset.exists(r => r.getTableDisplayName() == "g3")))
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
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan)

        assertTrue(runResult.isFreeConnex)

        assertTrue(runResult.groupByVariables.isEmpty)
        assertTrue(runResult.aggregations.size == 1)
        assertTrue(runResult.aggregations(0)._2 == "COUNT")
        assertTrue(runResult.outputVariables.size == 1)
        assertTrue(runResult.outputVariables(0) == runResult.aggregations(0)._1)

        assertTrue(runResult.candidates.size == 4)
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g1"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g2"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g3"))
        assertTrue(runResult.candidates.exists(t => t._1.root.getTableDisplayName() == "g4"))
        assertTrue(runResult.candidates.forall(t => t._1.subset.isEmpty))
    }

    @Test
    def testFixRoot(): Unit = {
        val ddl =
            """
              |CREATE TABLE nation
              |(
              |    n_nationkey INTEGER,
              |    n_name      VARCHAR,
              |    n_regionkey INTEGER,
              |    n_comment   VARCHAR,
              |    PRIMARY KEY (n_nationkey)
              |) WITH (
              |    'cardinality' = '25'
              |);
              |
              |CREATE TABLE region
              |(
              |    r_regionkey INTEGER,
              |    r_name      VARCHAR,
              |    r_comment   VARCHAR,
              |    PRIMARY KEY (r_regionkey)
              |) WITH (
              |      'cardinality' = '5'
              |);
              |
              |CREATE TABLE part
              |(
              |    p_partkey     INTEGER,
              |    p_name        VARCHAR,
              |    p_mfgr        VARCHAR,
              |    p_brand       VARCHAR,
              |    p_type        VARCHAR,
              |    p_size        INTEGER,
              |    p_container   VARCHAR,
              |    p_retailprice DECIMAL,
              |    p_comment     VARCHAR,
              |    PRIMARY KEY (p_partkey)
              |) WITH (
              |      'cardinality' = '200000'
              |);
              |
              |CREATE TABLE supplier
              |(
              |    s_suppkey   INTEGER,
              |    s_name      VARCHAR,
              |    s_address   VARCHAR,
              |    s_nationkey INTEGER,
              |    s_phone     VARCHAR,
              |    s_acctbal   DECIMAL,
              |    s_comment   VARCHAR,
              |    PRIMARY KEY (s_suppkey)
              |) WITH (
              |      'cardinality' = '10000'
              |);
              |
              |CREATE TABLE customer
              |(
              |    c_custkey    INTEGER,
              |    c_name       VARCHAR,
              |    c_address    VARCHAR,
              |    c_nationkey  INTEGER,
              |    c_phone      VARCHAR,
              |    c_acctbal    DECIMAL,
              |    c_mktsegment VARCHAR,
              |    c_comment    VARCHAR,
              |    PRIMARY KEY (c_custkey)
              |) WITH (
              |     'cardinality' = '150000'
              |);
              |
              |CREATE TABLE orders
              |(
              |    o_orderkey      INTEGER,
              |    o_custkey       INTEGER,
              |    o_orderstatus   VARCHAR,
              |    o_totalprice    DECIMAL,
              |    o_orderdate     DATE,
              |    o_orderpriority VARCHAR,
              |    o_clerk         VARCHAR,
              |    o_shippriority  INTEGER,
              |    o_comment       VARCHAR,
              |    PRIMARY KEY (o_orderkey)
              |) WITH (
              |     'cardinality' = '1500000'
              |);
              |
              |CREATE TABLE lineitem
              |(
              |    l_orderkey      INTEGER,
              |    l_partkey       INTEGER,
              |    l_suppkey       INTEGER,
              |    l_linenumber    INTEGER,
              |    l_quantity      DECIMAL,
              |    l_extendedprice DECIMAL,
              |    l_discount      DECIMAL,
              |    l_tax           DECIMAL,
              |    l_returnflag    VARCHAR,
              |    l_linestatus    VARCHAR,
              |    l_shipdate      DATE,
              |    l_commitdate    DATE,
              |    l_receiptdate   DATE,
              |    l_shipinstruct  VARCHAR,
              |    l_shipmode      VARCHAR,
              |    l_comment       VARCHAR,
              |    PRIMARY KEY (l_orderkey, l_linenumber)
              |) WITH (
              |      'cardinality' = '6001215'
              |);
              |""".stripMargin
        val dml =
            """
              |SELECT c_custkey,
              |       c_name,
              |       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
              |       c_acctbal,
              |       n_name,
              |       c_address,
              |       c_phone,
              |       c_comment
              |FROM customer,
              |     orders,
              |     lineitem,
              |     nation
              |WHERE c_custkey = o_custkey
              |  AND l_orderkey = o_orderkey
              |  AND o_orderdate >= DATE '1993-10-01'
              |  AND o_orderdate < DATE '1994-01-01'
              |  AND l_returnflag = 'R'
              |  AND c_nationkey = n_nationkey
              |GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(dml)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val runResult = converter.run(logicalPlan, hint = null, fixRootEnable = true, pruneEnable = false)

        assertTrue(runResult.candidates.forall(c => {
            (c._1.isFixRoot && c._1.root.getTableDisplayName() == "lineitem") ||
                (!c._1.isFixRoot && c._1.root.asInstanceOf[AuxiliaryRelation].supportingRelation.getTableDisplayName() == "customer") ||
                (!c._1.isFixRoot && c._1.root.asInstanceOf[AuxiliaryRelation].supportingRelation.getTableDisplayName() == "nation")
        }))
    }
}
