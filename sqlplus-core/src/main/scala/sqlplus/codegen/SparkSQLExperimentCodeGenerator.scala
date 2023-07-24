package sqlplus.codegen

import sqlplus.plan.table.SqlPlusTable

class SparkSQLExperimentCodeGenerator(tables: List[SqlPlusTable], sql: String, classname: String, queryName: String, shortQueryName: String)
    extends AbstractSparkSQLCodeGenerator(tables, sql) {
    override def getAppName: String = classname

    override def getMaster: String = ""

    override def getLogger: String = "SparkSQLPlusExperiment"

    override def getQueryName: String = queryName

    override def getSourceTablePath(path: String): String = {
        val fileName = if (path.contains("/")) path.substring(path.lastIndexOf("/") + 1) else path
        "s\"${args.head}/" + fileName + "\""
    }

    override def getPackageName: String = s"sqlplus.example.custom.${shortQueryName}"

    override def getImports: List[String] = List(
        "org.apache.spark.sql.SparkSession",
        "org.apache.spark.SparkConf",
        "org.slf4j.LoggerFactory"
    )

    override def getName: String = classname
}
