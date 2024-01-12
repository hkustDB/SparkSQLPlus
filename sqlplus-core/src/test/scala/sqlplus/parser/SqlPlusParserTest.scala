package sqlplus.parser

import org.junit.Assert.assertTrue
import org.junit.Test
import sqlplus.parser.ddl.SqlCreateTable
import sqlplus.plan.table.SqlPlusTable

class SqlPlusParserTest {
    @Test
    def testParseTableName(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    x INT,
              |    y VARCHAR,
              |    z INT,
              |    u VARCHAR,
              |    v INT,
              |    w VARCHAR,
              |    PRIMARY KEY (x)
              |)
              |""".stripMargin
        val nodeList = SqlPlusParser.parseDdl(ddl)
        assertTrue(nodeList.size() == 1)
        val table = new SqlPlusTable(nodeList.get(0).asInstanceOf[SqlCreateTable])
        assertTrue(table.getTableName == "Graph")
        assertTrue(table.getTableProperties.isEmpty)
    }

    @Test
    def testParseSinglePrimaryKey(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    x INT,
              |    y VARCHAR,
              |    z INT,
              |    u VARCHAR,
              |    v INT,
              |    w VARCHAR,
              |    PRIMARY KEY (x)
              |)
              |""".stripMargin
        val nodeList = SqlPlusParser.parseDdl(ddl)
        assertTrue(nodeList.size() == 1)
        val table = new SqlPlusTable(nodeList.get(0).asInstanceOf[SqlCreateTable])
        assertTrue(table.getPrimaryKeys.sameElements(Array("x")))
    }

    @Test
    def testParseMultiplePrimaryKeys(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    x INT,
              |    y VARCHAR,
              |    z INT,
              |    u VARCHAR,
              |    v INT,
              |    w VARCHAR,
              |    PRIMARY KEY (x, z, v, w)
              |)
              |""".stripMargin
        val nodeList = SqlPlusParser.parseDdl(ddl)
        assertTrue(nodeList.size() == 1)
        val table = new SqlPlusTable(nodeList.get(0).asInstanceOf[SqlCreateTable])
        assertTrue(table.getPrimaryKeys.sameElements(Array("x", "z", "v", "w")))
    }

    @Test(expected = classOf[AssertionError])
    def testParseInvalidPrimaryKey(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    x INT,
              |    y VARCHAR,
              |    z INT,
              |    u VARCHAR,
              |    v INT,
              |    w VARCHAR,
              |    PRIMARY KEY (x, d, y)
              |)
              |""".stripMargin
        val nodeList = SqlPlusParser.parseDdl(ddl)
        assertTrue(nodeList.size() == 1)
        val table = new SqlPlusTable(nodeList.get(0).asInstanceOf[SqlCreateTable])
    }

    @Test
    def testParseTableProperties(): Unit = {
        val ddl =
            """
              |CREATE TABLE Graph
              |(
              |    x INT,
              |    y VARCHAR,
              |    z INT,
              |    u VARCHAR,
              |    v INT,
              |    w VARCHAR
              |) WITH (
              |      'k1' = 'v1',
              |      'k2' = 'v2',
              |      'k3' = 'v3'
              |)
              |""".stripMargin
        val nodeList = SqlPlusParser.parseDdl(ddl)
        assertTrue(nodeList.size() == 1)
        val table = new SqlPlusTable(nodeList.get(0).asInstanceOf[SqlCreateTable])
        assertTrue(table.getTableProperties.size() == 3)
        assertTrue(table.getTableProperties.get("k1") == "v1")
        assertTrue(table.getTableProperties.get("k2") == "v2")
        assertTrue(table.getTableProperties.get("k3") == "v3")
    }

    @Test
    def testParseMultipleTables(): Unit = {
        val ddl =
            """
              |CREATE TABLE T1
              |(
              |    x INT,
              |    y VARCHAR,
              |    z INT,
              |    u VARCHAR,
              |    v INT,
              |    w VARCHAR,
              |    PRIMARY KEY (x)
              |) WITH (
              |      'k1' = 'v1',
              |      'k2' = 'v2',
              |      'k3' = 'v3'
              |);
              |
              |CREATE TABLE T2
              |(
              |    a INT,
              |    b VARCHAR
              |);
              |
              |CREATE TABLE T3
              |(
              |    r INT,
              |    s VARCHAR,
              |    t INT,
              |    a VARCHAR,
              |    b INT,
              |    PRIMARY KEY (r, s)
              |) WITH (
              |      'k1' = 'v1'
              |);
              |""".stripMargin
        val nodeList = SqlPlusParser.parseDdl(ddl)
        assertTrue(nodeList.size() == 3)
        val tableT1 = new SqlPlusTable(nodeList.get(0).asInstanceOf[SqlCreateTable])
        val tableT2 = new SqlPlusTable(nodeList.get(1).asInstanceOf[SqlCreateTable])
        val tableT3 = new SqlPlusTable(nodeList.get(2).asInstanceOf[SqlCreateTable])

        assertTrue(tableT1.getTableName == "T1")
        assertTrue(tableT1.getTableColumns.length == 6)
        assertTrue(tableT1.getPrimaryKeys.length == 1)
        assertTrue(tableT1.getTableProperties.size() == 3)

        assertTrue(tableT2.getTableName == "T2")
        assertTrue(tableT2.getTableColumns.length == 2)
        assertTrue(tableT2.getPrimaryKeys.isEmpty)
        assertTrue(tableT2.getTableProperties.isEmpty)

        assertTrue(tableT3.getTableName == "T3")
        assertTrue(tableT3.getTableColumns.length == 5)
        assertTrue(tableT3.getPrimaryKeys.length == 2)
        assertTrue(tableT3.getTableProperties.size() == 1)
    }
}
