package sqlplus.codegen

import sqlplus.plan.table.SqlPlusTable

import scala.collection.mutable

abstract class AbstractSparkSQLCodeGenerator(tables: List[SqlPlusTable], sql: String) extends AbstractScalaCodeGenerator {
    def getAppName: String

    def getMaster: String

    def getLogger: String

    def getQueryName: String

    def getSourceTablePath(table: SqlPlusTable): String

    override def getType: String = "object"

    override def getConstructorParameters: List[Parameter] = List()

    override def getExtends: String = ""

    override def getSuperClassParameters: List[String] = List()

    override def generateBody(builder: mutable.StringBuilder): Unit = {
        if (getLogger.nonEmpty) {
            indent(builder, 1).append("val LOGGER = LoggerFactory.getLogger(\"").append(getLogger).append("\")\n")
            newLine(builder)
        }

        generateExecuteMethod(builder)
    }

    private def generateExecuteMethod(builder: mutable.StringBuilder): Unit = {
        indent(builder, 1).append("def main(args: Array[String]): Unit = {").append("\n")
        generateSparkInit(builder)

        generateLoadTables(builder, tables)

        generateQuery(builder, sql)

        generateCount(builder)

        generateSparkClose(builder)
        indent(builder, 1).append("}").append("\n")
    }

    private def generateSparkInit(builder: mutable.StringBuilder): Unit = {
        indent(builder, 2).append("val conf = new SparkConf()").append("\n")
        indent(builder, 2).append("conf.setAppName(\"").append(getAppName).append("\")\n")

        if (getMaster.nonEmpty)
            indent(builder, 2).append("conf.setMaster(\"").append(getMaster).append("\")\n")

        indent(builder, 2).append("val spark = SparkSession.builder.config(conf).getOrCreate()").append("\n")
    }

    private def generateSparkClose(builder: mutable.StringBuilder): Unit = {
        newLine(builder)
        indent(builder, 2).append("spark.close()").append("\n")
    }

    private def generateLoadTables(builder: mutable.StringBuilder, tables: List[SqlPlusTable]): Unit = {
        for (i <- tables.indices) {
            newLine(builder)

            val table = tables(i)
            val schema = table.getTableColumns.map(col => s"${col.getName} ${col.getType.toUpperCase}").mkString(", ")

            indent(builder, 2).append(s"val schema${i} = ").append("\"").append(schema).append("\"\n")
            indent(builder, 2).append(s"val df${i} = spark.read.format(").append("\"csv\")").append("\n")
            indent(builder, 3).append(".option(\"delimiter\", \",\")").append("\n")
            indent(builder, 3).append(".option(\"quote\", \"\")").append("\n")
            indent(builder, 3).append(".option(\"header\", \"false\")").append("\n")
            indent(builder, 3).append(s".schema(schema${i})").append("\n")
            indent(builder, 3).append(".load(").append(getSourceTablePath(table)).append(").persist()").append("\n")
            indent(builder, 2).append(s"df${i}.count()").append("\n")
            indent(builder, 2).append(s"df${i}.createOrReplaceTempView(").append("\"").append(table.getTableName).append("\")").append("\n")
        }
    }

    def generateQuery(builder: mutable.StringBuilder, sql: String): Unit = {
        newLine(builder)

        val lines = sql.split("\n")
        indent(builder, 2).append(s"val result = spark.sql(").append("\n")

        for (i <- lines.indices) {
            if (i < lines.size - 1)
                indent(builder, 3).append("\"").append(lines(i).trim).append(" \" + ").append("\n")
            else
                indent(builder, 3).append("\"").append(lines(i).trim).append("\"").append("\n")
        }

        indent(builder, 2).append(")\n")
    }

    def generateCount(builder: mutable.StringBuilder): Unit = {
        newLine(builder)
        indent(builder, 2).append("val ts1 = System.currentTimeMillis()").append("\n")
        indent(builder, 2).append("val cnt = result.count()").append("\n")
        indent(builder, 2).append("val ts2 = System.currentTimeMillis()").append("\n")
        indent(builder, 2).append("LOGGER.info(\"").append(getQueryName).append("-SparkSQL cnt: \" + cnt)").append("\n")
        indent(builder, 2).append("LOGGER.info(\"").append(getQueryName).append("-SparkSQL time: \" + (ts2 - ts1) / 1000f)").append("\n")
    }
}
