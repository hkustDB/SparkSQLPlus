package sqlplus.codegen

import sqlplus.compile.CompileResult
import sqlplus.plan.table.SqlPlusTable

class SparkSQLPlusExampleCodeGenerator(compileResult: CompileResult)
    extends AbstractSparkSQLPlusCodeGenerator(compileResult.comparisonOperators, compileResult.sourceTables,
        compileResult.aggregatedRelations, compileResult.auxiliaryRelations, compileResult.bagRelations,
        compileResult.relationIdToInfo, compileResult.reduceActions, compileResult.enumerateActions, compileResult.formatResultAction) {

    override def getAppName: String = "SparkSQLPlusExample"

    override def getMaster: String = "local[*]"

    override def getLogger: String = ""

    override def getQueryName: String = ""

    override def getPackageName: String = "sqlplus.example"

    override def getName: String = "SparkSQLPlusExample"

    override def getSourceTablePath(table: SqlPlusTable): String =
        "\"" + table.getTableProperties.get("path") + "\""
}
