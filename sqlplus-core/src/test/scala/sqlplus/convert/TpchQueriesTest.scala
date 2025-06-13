package sqlplus.convert

import org.junit.Assert.assertTrue
import org.junit.Test
import sqlplus.catalog.CatalogManager
import sqlplus.expression.VariableManager
import sqlplus.parser.SqlPlusParser
import sqlplus.plan.SqlPlusPlanner

class TpchQueriesTest {
    val ddl =
        """
          |CREATE TABLE nation
          |(
          |    n_nationkey INTEGER,
          |    n_name      VARCHAR,
          |    n_regionkey INTEGER,
          |    n_comment   VARCHAR,
          |    PRIMARY KEY (n_nationkey)
          |);
          |
          |CREATE TABLE region
          |(
          |    r_regionkey INTEGER,
          |    r_name      VARCHAR,
          |    r_comment   VARCHAR,
          |    PRIMARY KEY (r_regionkey)
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
          |);
          |
          |CREATE TABLE partsupp
          |(
          |    ps_partkey    INTEGER,
          |    ps_suppkey    INTEGER,
          |    ps_availqty   INTEGER,
          |    ps_supplycost DECIMAL,
          |    ps_comment    VARCHAR,
          |    PRIMARY KEY (ps_partkey, ps_suppkey)
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
          |);
          |""".stripMargin

    @Test
    def testTpchQ1(): Unit = {
        val query =
            """
              |SELECT l_returnflag,
              |       l_linestatus,
              |       SUM(l_quantity)                                       AS sum_qty,
              |       SUM(l_extendedprice)                                  AS sum_base_price,
              |       SUM(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
              |       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
              |       AVG(l_quantity)                                       AS avg_qty,
              |       AVG(l_extendedprice)                                  AS avg_price,
              |       AVG(l_discount)                                       AS avg_disc,
              |       COUNT(*)                                              AS count_order
              |FROM lineitem
              |WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
              |GROUP BY l_returnflag, l_linestatus
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ2View1(): Unit = {
        val view =
            """
              |SELECT ps_partkey, MIN(ps_supplycost)
              |FROM partsupp,
              |     supplier,
              |     nation,
              |     region
              |WHERE s_suppkey = ps_suppkey
              |  AND s_nationkey = n_nationkey
              |  AND n_regionkey = r_regionkey
              |  AND r_name = 'EUROPE'
              |GROUP BY ps_partkey
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(view)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ2(): Unit = {
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    v1_partkey        INTEGER,
              |    v1_supplycost_min DECIMAL
              |);
              |""".stripMargin

        val query =
            """
              |SELECT s_acctbal,
              |       s_name,
              |       n_name,
              |       p_partkey,
              |       p_mfgr,
              |       s_address,
              |       s_phone,
              |       s_comment
              |FROM part,
              |     supplier,
              |     partsupp,
              |     nation,
              |     region,
              |     view1
              |WHERE p_partkey = ps_partkey
              |  AND s_suppkey = ps_suppkey
              |  AND p_size = 15
              |  AND p_type LIKE '%BRASS'
              |  AND s_nationkey = n_nationkey
              |  AND n_regionkey = r_regionkey
              |  AND r_name = 'EUROPE'
              |  AND p_partkey = ps_partkey
              |  AND p_partkey = v1_partkey
              |  AND ps_supplycost = v1_supplycost_min
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ3(): Unit = {
        val query =
            """
              |SELECT l_orderkey,
              |       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
              |       o_orderdate,
              |       o_shippriority
              |FROM customer,
              |     orders,
              |     lineitem
              |WHERE c_mktsegment = 'BUILDING'
              |  AND c_custkey = o_custkey
              |  AND l_orderkey = o_orderkey
              |  AND o_orderdate < DATE '1995-03-15'
              |  AND l_shipdate > DATE '1995-03-15'
              |GROUP BY l_orderkey, o_orderdate, o_shippriority
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ4(): Unit = {
        /**
         * SELECT DISTINCT l_orderkey
         *  FROM lineitem
         *  WHERE l_commitdate < l_receiptdate
         */
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    v1_orderkey_distinct INTEGER
              |);
              |""".stripMargin

        val query =
            """
              |SELECT o_orderpriority,
              |       COUNT(*) AS order_count
              |FROM orders,
              |     view1
              |WHERE o_orderdate >= DATE '1993-07-01'
              |  AND o_orderdate < DATE '1993-10-01'
              |  AND v1_orderkey_distinct = o_orderkey
              |GROUP BY o_orderpriority
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ5(): Unit = {
        val query =
            """
              |SELECT n_name,
              |       SUM(l_extendedprice * (1 - l_discount)) AS revenue
              |FROM customer,
              |     orders,
              |     lineitem,
              |     supplier,
              |     nation,
              |     region
              |WHERE c_custkey = o_custkey
              |  AND l_orderkey = o_orderkey
              |  AND l_suppkey = s_suppkey
              |  AND c_nationkey = s_nationkey
              |  AND s_nationkey = n_nationkey
              |  AND n_regionkey = r_regionkey
              |  AND r_name = 'ASIA'
              |  AND o_orderdate >= DATE '1994-01-01'
              |  AND o_orderdate < DATE '1995-01-01'
              |GROUP BY n_name
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.isEmpty)
        val convertResult = converter.convertCyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
        assertTrue(convertResult.candidates.forall(t => t._3.nonEmpty))
    }

    @Test
    def testTpchQ6(): Unit = {
        val query =
            """
              |SELECT SUM(l_extendedprice * l_discount) AS revenue
              |FROM lineitem
              |WHERE l_shipdate >= DATE '1994-01-01'
              |  AND l_shipdate < DATE '1995-01-01'
              |  AND l_discount BETWEEN 0.05 AND 0.07
              |  AND l_quantity < 24
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ7(): Unit = {
        val query =
            """
              |SELECT supp_nation,
              |       cust_nation,
              |       l_year,
              |       SUM(volume) AS revenue
              |FROM (SELECT n1.n_name                          AS supp_nation,
              |             n2.n_name                          AS cust_nation,
              |             EXTRACT(YEAR FROM l_shipdate)      AS l_year,
              |             l_extendedprice * (1 - l_discount) AS volume
              |      FROM supplier,
              |           lineitem,
              |           orders,
              |           customer,
              |           nation n1,
              |           nation n2
              |      WHERE s_suppkey = l_suppkey
              |        AND o_orderkey = l_orderkey
              |        AND c_custkey = o_custkey
              |        AND s_nationkey = n1.n_nationkey
              |        AND c_nationkey = n2.n_nationkey
              |        AND (
              |              (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
              |              OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
              |          )
              |        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping
              |GROUP BY supp_nation,
              |         cust_nation,
              |         l_year
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ8(): Unit = {
        /**
         * SELECT o_orderkey,
         *  o_custkey,
         *  o_orderstatus,
         *  o_totalprice,
         *  o_orderdate,
         *  o_orderpriority,
         *  o_clerk,
         *  o_shippriority,
         *  o_comment,
         *  EXTRACT(YEAR FROM o_orderdate)
         *  FROM orders
         */
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    v1_orderkey      INTEGER,
              |    v1_custkey       INTEGER,
              |    v1_orderstatus   VARCHAR,
              |    v1_totalprice    DECIMAL,
              |    v1_orderdate     DATE,
              |    v1_orderpriority VARCHAR,
              |    v1_clerk         VARCHAR,
              |    v1_shippriority  INTEGER,
              |    v1_comment       VARCHAR,
              |    v1_year          INTEGER
              |);
              |""".stripMargin

        val query =
            """
              |SELECT v1_year,
              |       SUM(CASE
              |               WHEN n2.n_name = 'BRAZIL'
              |                   THEN l_extendedprice * (1 - l_discount)
              |               ELSE 0
              |           END) / SUM(l_extendedprice * (1 - l_discount)) AS mkt_share
              |FROM part,
              |     supplier,
              |     lineitem,
              |     customer,
              |     nation n1,
              |     nation n2,
              |     region,
              |     view1
              |WHERE p_partkey = l_partkey
              |  AND s_suppkey = l_suppkey
              |  AND l_orderkey = v1_orderkey
              |  AND v1_custkey = c_custkey
              |  AND c_nationkey = n1.n_nationkey
              |  AND n1.n_regionkey = r_regionkey
              |  AND r_name = 'AMERICA'
              |  AND s_nationkey = n2.n_nationkey
              |  AND v1_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
              |  AND p_type = 'ECONOMY ANODIZED STEEL'
              |GROUP BY v1_year
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ9(): Unit = {
        val query =
            """
              |SELECT nation,
              |       o_year,
              |       SUM(amount) AS sum_profit
              |FROM (SELECT n_name                                                          AS nation,
              |             EXTRACT(YEAR FROM o_orderdate)                                  AS o_year,
              |             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
              |      FROM part,
              |           supplier,
              |           lineitem,
              |           partsupp,
              |           orders,
              |           nation
              |      WHERE s_suppkey = l_suppkey
              |        AND ps_suppkey = l_suppkey
              |        AND ps_partkey = l_partkey
              |        AND p_partkey = l_partkey
              |        AND o_orderkey = l_orderkey
              |        AND s_nationkey = n_nationkey
              |        AND p_name LIKE '%green%') AS profit
              |GROUP BY nation, o_year
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ10(): Unit = {
        val query =
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
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ11(): Unit = {
        val query =
            """
              |SELECT ps_partkey,
              |       SUM(ps_supplycost * ps_availqty) AS `value`
              |FROM partsupp,
              |     supplier,
              |     nation
              |WHERE ps_suppkey = s_suppkey
              |  AND s_nationkey = n_nationkey
              |  AND n_name = 'GERMANY'
              |GROUP BY ps_partkey
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ12(): Unit = {
        val query =
            """
              |SELECT l_shipmode,
              |       SUM(CASE
              |               WHEN o_orderpriority = '1-URGENT'
              |                   OR o_orderpriority = '2-HIGH'
              |                   THEN 1
              |               ELSE 0
              |           END) AS high_line_count,
              |       SUM(CASE
              |               WHEN o_orderpriority <> '1-URGENT'
              |                   AND o_orderpriority <> '2-HIGH'
              |                   THEN 1
              |               ELSE 0
              |           END) AS low_line_count
              |FROM orders,
              |     lineitem
              |WHERE o_orderkey = l_orderkey
              |  AND l_shipmode IN ('MAIL', 'SHIP')
              |  AND l_commitdate < l_receiptdate
              |  AND l_shipdate < l_commitdate
              |  AND l_receiptdate >= DATE '1994-01-01'
              |  AND l_receiptdate < DATE '1995-01-01'
              |GROUP BY l_shipmode
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ14(): Unit = {
        val query =
            """
              |SELECT 100.00 * SUM(CASE
              |                        WHEN p_type LIKE 'PROMO%'
              |                            THEN l_extendedprice * (1 - l_discount)
              |                        ELSE 0
              |    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
              |FROM lineitem,
              |     part
              |WHERE l_partkey = p_partkey
              |  AND l_shipdate >= DATE '1995-09-01'
              |  AND l_shipdate < DATE '1995-10-01'
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ15(): Unit = {
        /**
         * SELECT l_suppkey,
         *  SUM(l_extendedprice * (1 - l_discount))
         *  FROM lineitem
         *  WHERE l_shipdate >= DATE '1996-01-01'
         *  AND l_shipdate < DATE '1996-04-01'
         *  GROUP BY l_suppkey
         */
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    supplier_no   INTEGER,
              |    total_revenue DECIMAL
              |);
              |""".stripMargin

        /**
         * SELECT MAX(total_revenue)
         *  FROM view1
         */
        val ddlView2 =
            """
              |CREATE TABLE view2
              |(
              |    v2_total_revenue_max DECIMAL
              |);
              |""".stripMargin

        val query =
            """
              |SELECT s_suppkey,
              |       s_name,
              |       s_address,
              |       s_phone,
              |       total_revenue
              |FROM supplier,
              |     view1,
              |     view2
              |WHERE s_suppkey = supplier_no
              |  AND total_revenue = v2_total_revenue_max
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1 + ddlView2)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ17(): Unit = {
        /**
         * SELECT l_partkey,
         *  0.2 * AVG(l_quantity)
         *  FROM lineitem
         *  GROUP BY l_partkey
         */
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    v1_partkey      INTEGER,
              |    v1_quantity_avg DECIMAL
              |);
              |""".stripMargin

        val query =
            """
              |SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
              |FROM lineitem,
              |     part,
              |     view1
              |WHERE p_partkey = l_partkey
              |  AND p_brand = 'Brand#23'
              |  AND p_container = 'MED BOX'
              |  AND l_partkey = v1_partkey
              |  AND l_quantity > v1_quantity_avg
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ18(): Unit = {
        /**
         * SELECT l_orderkey
         *  FROM lineitem
         *  GROUP BY l_orderkey
         *  HAVING SUM(l_quantity) > 300
         */
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    v1_orderkey INTEGER
              |);
              |""".stripMargin

        val query =
            """
              |SELECT c_name,
              |       c_custkey,
              |       o_orderkey,
              |       o_orderdate,
              |       o_totalprice,
              |       SUM(l_quantity)
              |FROM customer,
              |     orders,
              |     lineitem,
              |     view1
              |WHERE o_orderkey = v1_orderkey
              |  AND c_custkey = o_custkey
              |  AND o_orderkey = l_orderkey
              |GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ19(): Unit = {
        val query =
            """
              |SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
              |FROM lineitem,
              |     part
              |WHERE p_partkey = l_partkey
              |  AND p_brand = 'Brand#12'
              |  AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
              |  AND l_quantity >= 1
              |  AND l_quantity <= 11
              |  AND p_size BETWEEN 1 AND 5
              |  AND l_shipmode IN ('AIR', 'AIR REG')
              |  AND l_shipinstruct = 'DELIVER IN PERSON'
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    // TODO: support cross product. Remove the two dummy columns.
    @Test
    def testTpchQ20View3(): Unit = {
        /**
         * SELECT 1, p_partkey
         *  FROM part
         *  WHERE p_name LIKE 'forest%'
         */
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    v1_partkey INTEGER,
              |    v1_dummy   INTEGER
              |);
              |""".stripMargin

        /**
         * SELECT 1, 0.5 * SUM(l_quantity)
         *  FROM lineitem
         *  WHERE l_partkey = ps_partkey
         *  AND l_suppkey = ps_suppkey
         *  AND l_shipdate >= DATE '1994-01-01'
         *  AND l_shipdate < DATE '1995-01-01'
         */
        val ddlView2 =
            """
              |CREATE TABLE view2
              |(
              |    v2_quantity_sum DECIMAL,
              |    v2_dummy        INTEGER
              |);
              |""".stripMargin

        val query =
            """
              |SELECT ps_suppkey
              |FROM partsupp,
              |     view1,
              |     view2
              |WHERE ps_partkey = v1_partkey
              |  AND ps_availqty > v2_quantity_sum
              |  AND v1_dummy = v2_dummy
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1 + ddlView2)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }

    @Test
    def testTpchQ20(): Unit = {
        val ddlView3 =
            """
              |CREATE TABLE view3
              |(
              |    v3_suppkey INTEGER
              |);
              |""".stripMargin

        val query =
            """
              |SELECT s_name,
              |       s_address
              |FROM supplier,
              |     nation,
              |     view3
              |WHERE s_suppkey = v3_suppkey
              |  AND s_nationkey = n_nationkey
              |  AND n_name = 'CANADA'
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView3)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager, catalogManager)
        val context = converter.traverseLogicalPlan(logicalPlan)
        val optDryRunResult = converter.dryRun(context)
        assertTrue(optDryRunResult.nonEmpty)
        val convertResult = converter.convertAcyclic(context)
        assertTrue(convertResult.candidates.nonEmpty)
    }
}
