package sqlplus.codegen

import sqlplus.compile.CompileResult

class SparkSQLPlusExampleCodeGenerator(compileResult: CompileResult, packageName: String, objectName: String)
    extends AbstractSparkSQLPlusCodeGenerator(compileResult.setupActions, compileResult.reduceActions, compileResult.enumerateActions) {

    override def getAppName: String = "SparkSQLPlusExample"

    override def getMaster: String = "local[*]"

    override def getLogger: String = ""

    override def getQueryName: String = ""

    override def getPackageName: String = packageName

    override def getName: String = objectName

    override def getSourceTablePath(path: String): String =
        "\"" + path + "\""
}
