package sqlplus.codegen

import sqlplus.compile.CompileResult

class SparkSQLPlusExperimentCodeGenerator(compileResult: CompileResult, classname: String, queryName: String, shortQueryName: String)
    extends AbstractSparkSQLPlusCodeGenerator(compileResult.setupActions, compileResult.reduceActions, compileResult.enumerateActions) {

    override def getAppName: String = classname

    override def getMaster: String = ""

    override def getLogger: String = "SparkSQLPlusExperiment"

    override def getQueryName: String = queryName

    override def getPackageName: String = s"sqlplus.example.custom.${shortQueryName}"

    override def getName: String = classname

    override def getImports: List[String] =
        super.getImports :+ "org.slf4j.LoggerFactory"

    override def getSourceTablePath(path: String): String = {
        val fileName = if (path.contains("/")) path.substring(path.lastIndexOf("/") + 1) else path
        "s\"${args.head}/" + fileName + "\""
    }
}
