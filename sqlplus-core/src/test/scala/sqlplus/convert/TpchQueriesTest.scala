package sqlplus.convert

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
          |    n_comment   VARCHAR
          |);
          |
          |CREATE TABLE region
          |(
          |    r_regionkey INTEGER,
          |    r_name      VARCHAR,
          |    r_comment   VARCHAR
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
          |    p_comment     VARCHAR
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
          |    s_comment   VARCHAR
          |);
          |
          |CREATE TABLE partsupp
          |(
          |    ps_partkey    INTEGER,
          |    ps_suppkey    INTEGER,
          |    ps_availqty   INTEGER,
          |    ps_supplycost DECIMAL,
          |    ps_comment    VARCHAR
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
          |    c_comment    VARCHAR
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
          |    o_comment       VARCHAR
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
          |    l_comment       VARCHAR
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
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
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
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
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
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
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
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
    }

    @Test
    def testTpchQ4View1(): Unit = {
        val view =
            """
              |SELECT DISTINCT l_orderkey
              |FROM lineitem
              |WHERE l_commitdate < l_receiptdate
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(view)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
    }

    @Test
    def testTpchQ4(): Unit = {
        val ddlView1 =
            """
              |CREATE TABLE view1
              |(
              |    l_orderkey_distinct INTEGER
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
              |  AND l_orderkey_distinct = o_orderkey
              |GROUP BY o_orderpriority
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl + ddlView1)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
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
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
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
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
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
              |        AND n1.n_name = 'FRANCE'
              |        AND n2.n_name = 'GERMANY'
              |        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping
              |GROUP BY supp_nation, cust_nation, l_year
              |""".stripMargin

        val nodeList = SqlPlusParser.parseDdl(ddl)
        val catalogManager = new CatalogManager
        catalogManager.register(nodeList)
        val sqlNode = SqlPlusParser.parseDml(query)
        val sqlPlusPlanner = new SqlPlusPlanner(catalogManager)
        val logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode)
        val variableManager = new VariableManager
        val converter = new LogicalPlanConverter(variableManager)
        converter.run(logicalPlan)
    }
}
