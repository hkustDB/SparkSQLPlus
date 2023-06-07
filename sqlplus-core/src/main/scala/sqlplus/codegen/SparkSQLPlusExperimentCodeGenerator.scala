package sqlplus.codegen

import sqlplus.compile.{CompileResult, CountResultAction}
import sqlplus.plan.table.SqlPlusTable

class SparkSQLPlusExperimentCodeGenerator(compileResult: CompileResult, classname: String, queryName: String, shortQueryName: String)
    extends AbstractSparkSQLPlusCodeGenerator(compileResult.comparisonOperators, compileResult.sourceTables,
        compileResult.relationIdToInfo, compileResult.reduceActions, compileResult.enumerateActions, CountResultAction) {

    override def getAppName: String = classname

    override def getMaster: String = ""

    override def getLogger: String = "SparkSQLPlusExperiment"

    override def getQueryName: String = queryName

    override def getPackageName: String = s"sqlplus.example.custom.${shortQueryName}"

    override def getName: String = classname

    override def getImports: List[String] =
        super.getImports :+ "org.slf4j.LoggerFactory"

    override def getSourceTablePath(table: SqlPlusTable): String = {
        val pathInDdl = table.getTableProperties.get("path")
        val fileName = if (pathInDdl.contains("/")) pathInDdl.substring(pathInDdl.lastIndexOf("/") + 1) else pathInDdl
        "s\"${args.head}/" + fileName + "\""
    }
}
