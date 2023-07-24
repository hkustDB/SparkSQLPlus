package sqlplus.codegen

import sqlplus.compile._

import scala.collection.mutable

abstract class AbstractSparkSQLPlusCodeGenerator(val setupActions: List[SetupAction],
                                                 val reduceActions: List[ReduceAction],
                                                 val enumerateActions: List[EnumerateAction])
    extends AbstractScalaCodeGenerator {

    def getAppName: String

    def getMaster: String

    def getLogger: String

    def getQueryName: String

    def getSourceTablePath(path: String): String

    override def getImports: List[String] = List(
        "sqlplus.helper.ImplicitConversions._",
        "org.apache.spark.sql.SparkSession",
        "org.apache.spark.SparkConf"
    )

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

        generateSetup(builder)

        generateReduction(builder)

        generateEnumeration(builder)

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

    private def generateSetup(builder: mutable.StringBuilder): Unit = {
        if (setupActions.nonEmpty)
            newLine(builder)

        for (action <- setupActions) {
            action match {
                case readSourceTableAction: ReadSourceTableAction =>
                    generateReadSourceTable(builder, readSourceTableAction)
                case computeAggregatedRelationAction: ComputeAggregatedRelationAction =>
                    generateComputeAggregatedRelationAction(builder, computeAggregatedRelationAction)
                case functionDefinitionAction: FunctionDefinitionAction =>
                    generateFunctionDefinitions(builder, functionDefinitionAction)
            }
        }
    }

    private def generateReadSourceTable(builder: mutable.StringBuilder, action: ReadSourceTableAction): Unit = {
        indent(builder, 2).append("val ").append(action.variableName).append(" = spark.sparkContext.textFile(")
            .append(getSourceTablePath(action.path)).append(").map(row => {").append("\n")
        indent(builder, 3).append("val f = row.split(\",\")").append("\n")

        val fields = action.columnIndices.zip(action.columnTypes).map(t => t._2.fromString(s"f(${t._1})"))

        indent(builder, 3).append("Array[Any](").append(fields.mkString(", ")).append(")").append("\n")
        indent(builder, 2).append("}).persist()").append("\n")
        indent(builder, 2).append(action.variableName).append(".count()").append("\n")
    }

    private def generateComputeAggregatedRelationAction(builder: mutable.StringBuilder, action: ComputeAggregatedRelationAction): Unit = {
        action.aggregateFunction match {
            case "COUNT" =>
                val groupFields = action.groupIndices.map(i => "f(" + i + ")")
                val groupFieldsString = if (groupFields.size > 1) groupFields.mkString("(", ", ", ")") else groupFields.head
                indent(builder, 2).append("val ").append(action.variableName).append(" = ").append(action.fromVariableName)
                    .append(".map(f => (").append(groupFieldsString).append(", 1L))")
                    .append(".reduceByKey(_ + _)")
                    .append(".map(x => Array[Any](" + (0 to action.groupIndices.length).map(i => "x._" + (i + 1)).mkString(", ") + ")).persist()").append("\n")
                indent(builder, 2).append(action.variableName).append(".count()").append("\n")
            case _ => throw new UnsupportedOperationException
        }
    }

    private def generateFunctionDefinitions(builder: mutable.StringBuilder, action: FunctionDefinitionAction): Unit = {
        action.func.foreach(row => indent(builder, 2).append(row).append("\n"))
    }

    private def generateReduction(builder: mutable.StringBuilder): Unit = {
        if (reduceActions.nonEmpty)
            newLine(builder)

        for (action <- reduceActions) {
            action match {
                case createCommonExtraColumnAction: CreateCommonExtraColumnAction =>
                    generateCreateCommonExtraColumnAction(builder, createCommonExtraColumnAction)
                case createTransparentCommonExtraColumnAction: CreateTransparentCommonExtraColumnAction =>
                    generateCreateTransparentCommonExtraColumnAction(builder, createTransparentCommonExtraColumnAction)
                case createComparisonExtraColumnAction: CreateComparisonExtraColumnAction =>
                    generateCreateComparisonExtraColumnAction(builder, createComparisonExtraColumnAction)
                case createComputationExtraColumnAction: CreateComputationExtraColumnAction =>
                    generateCreateComputationExtraColumnAction(builder, createComputationExtraColumnAction)
                case appendCommonExtraColumnAction: AppendCommonExtraColumnAction =>
                    generateAppendCommonExtraColumnAction(builder, appendCommonExtraColumnAction)
                case appendComparisonExtraColumnAction: AppendComparisonExtraColumnAction =>
                    generateAppendComparisonExtraColumnAction(builder, appendComparisonExtraColumnAction)
                case applySelfComparisonAction: ApplySelfComparisonAction =>
                    generateApplySelfComparisonAction(builder, applySelfComparisonAction)
                case applySemiJoinAction: ApplySemiJoinAction =>
                    generateApplySemiJoinAction(builder, applySemiJoinAction)
                case materializeAuxiliaryRelationAction: MaterializeAuxiliaryRelationAction =>
                    generateMaterializeAuxiliaryRelationAction(builder, materializeAuxiliaryRelationAction)
                case materializeBagRelationAction: MaterializeBagRelationAction =>
                    generateMaterializeBagRelationAction(builder, materializeBagRelationAction)
                case keyByAction: KeyByAction =>
                    generateKeyByAction(builder, keyByAction)
                case groupByAction: GroupByAction =>
                    generateGroupByAction(builder, groupByAction)
            }
        }
    }

    def generateCreateCommonExtraColumnAction(builder: mutable.StringBuilder, action: CreateCommonExtraColumnAction): Unit = {
        indent(builder, 2).append("val ").append(action.sortedVariableName).append(" = ")
            .append(action.fromVariableName).append(s".sortValuesWith[${action.typeParameters}](").append(action.compareKeyIndex).append(", ")
            .append(action.compareFunc).append(").persist()").append("\n")

        indent(builder, 2).append("val ").append(action.columnVariableName).append(" = ")
            .append(action.sortedVariableName).append(".extractFieldInHeadElement(").append(action.compareKeyIndex)
            .append(")").append("\n")
    }

    def generateCreateTransparentCommonExtraColumnAction(builder: mutable.StringBuilder, action: CreateTransparentCommonExtraColumnAction): Unit = {
        // the join key with parent must be the 1st field
        assert(action.joinKeyIndices.size == 1 && action.joinKeyIndices.head == 0 && action.joinKeyExtractFuncs.size == 1)
        val newKey = action.joinKeyExtractFuncs.head.apply("x(0)")
        indent(builder, 2).append("val ").append(action.columnVariableName).append(" = ")
            .append(action.fromVariableName).append(s".map(x => ($newKey, x(1)))").append("\n")
    }

    def generateCreateComparisonExtraColumnAction(builder: mutable.StringBuilder, action: CreateComparisonExtraColumnAction): Unit = {
        indent(builder, 2).append("val ").append(action.treeLikeArrayVariableName).append(" = ")
            .append(action.fromVariableName).append(s".constructTreeLikeArray[${action.typeParameters}](")
            .append(action.compareKeyIndex1).append(", ")
            .append(action.compareKeyIndex2).append(", ")
            .append(action.compareFunc1).append(", ")
            .append(action.compareFunc2).append(")").append("\n")

        indent(builder, 2).append("val ").append(action.columnVariableName).append(" = ")
            .append(action.treeLikeArrayVariableName).append(".createDictionary()").append("\n")
    }

    def generateCreateComputationExtraColumnAction(builder: mutable.StringBuilder, action: CreateComputationExtraColumnAction): Unit = {
        indent(builder, 2).append("val ").append(action.resultVariableName).append(" = ")
            .append(action.fromVariableName).append(".appendExtraColumn(")
            .append("x => ").append(action.functionGenerator("x")).append(")").append("\n")
    }

    def generateAppendCommonExtraColumnAction(builder: mutable.StringBuilder, action: AppendCommonExtraColumnAction): Unit = {
        indent(builder, 2).append("val ").append(action.appendedVariableName).append(" = ")
            .append(action.fromVariableName).append(".appendExtraColumn(")
            .append(action.columnVariableName).append(")").append("\n")
    }

    def generateAppendComparisonExtraColumnAction(builder: mutable.StringBuilder, action: AppendComparisonExtraColumnAction): Unit = {
        indent(builder, 2).append("val ").append(action.appendedVariableName).append(" = ")
            .append(action.fromVariableName).append(s".appendExtraColumn[${action.typeParameters}](")
            .append(action.columnVariableName).append(", ")
            .append(action.compareKeyIndex).append(", ")
            .append(action.compareFunc).append(")").append("\n")
    }

    def generateApplySemiJoinAction(builder: mutable.StringBuilder, action: ApplySemiJoinAction): Unit = {
        indent(builder, 2).append("val ").append(action.variableName).append(" = ")
            .append(action.fromVariableName).append(".semiJoin(")
            .append(action.childVariableName).append(")").append("\n")
    }

    def generateApplySelfComparisonAction(builder: mutable.StringBuilder, action: ApplySelfComparisonAction): Unit = {
        indent(builder, 2).append("val ").append(action.variableName).append(" = ")
            .append(action.fromVariableName).append(".filter(")
            .append("x => ").append(action.functionGenerator("x._2")).append(")").append("\n")
    }

    // TODO: we can use the group by and sorted RDD directly, avoiding the extra distinct
    def generateMaterializeAuxiliaryRelationAction(builder: mutable.StringBuilder, action: MaterializeAuxiliaryRelationAction): Unit = {
        // we cannot use Array to deduplicate
        val func1 = action.projectIndices.map(i => s"x($i)").mkString("x => (", ", ", ")")
        // change back to Array after distinct
        val func2 = action.projectIndices.indices.map(i => s"x._${i+1}").mkString("x => Array(", ", ", ")")

        indent(builder, 2).append("val ").append(action.variableName).append(" = ")
            .append(action.supportingVariableName).append(s".mapValues($func1).distinct().mapValues($func2)").append("\n")
    }

    def generateMaterializeBagRelationAction(builder: mutable.StringBuilder, action: MaterializeBagRelationAction): Unit = {
        indent(builder, 2).append("val ").append(action.variableName).append(" = spark.sparkContext.lftj(")
            .append(action.tableScanRelationVariableNames).append(", ")
            .append(action.relationCount).append(", ")
            .append(action.variableCount).append(", ").append("\n")
        indent(builder, 4).append(action.sourceTableIndexToRelations).append(", ").append("\n")
        indent(builder, 4).append(action.redirects).append(", ").append("\n")
        indent(builder, 4).append(action.variableIndices).append(").cache()").append("\n")
    }

    def generateKeyByAction(builder: mutable.StringBuilder, action: KeyByAction): Unit = {
        val keyByFunc = if (action.keyIndices.size == 1) {
            val cast = action.keyExtractFuncs.head.apply(s"x(${action.keyIndices.head})")
            s"x => $cast"
        } else {
            action.keyIndices.zip(action.keyExtractFuncs).map(t => {
                val keyIndex = t._1
                val keyExtractFunc = t._2
                keyExtractFunc.apply(s"x(${keyIndex})")
            }).mkString("x => (", ", ", ")")
        }

        indent(builder, 2).append("val ").append(action.variableName).append(" = ")
            .append(action.fromVariableName)

        if (action.isReKey) {
            builder.append(".reKeyBy(").append(keyByFunc).append(")").append("\n")
        } else {
            builder.append(".keyBy(").append(keyByFunc).append(")").append("\n")
        }
    }

    def generateGroupByAction(builder: mutable.StringBuilder, action: GroupByAction): Unit = {
        indent(builder, 2).append("val ").append(action.variableName).append(" = ")
            .append(action.fromVariableName).append(".groupBy()").append("\n")
    }

    private def generateEnumeration(builder: mutable.StringBuilder): Unit = {
        if (enumerateActions.size > 1)
            newLine(builder)

        for (action <- enumerateActions) {
            action match {
                case rootPrepareEnumerationAction: RootPrepareEnumerationAction =>
                    generateRootPrepareEnumerationAction(builder, rootPrepareEnumerationAction)
                case enumerateWithoutComparisonAction: EnumerateWithoutComparisonAction =>
                    generateEnumerateWithoutComparisonAction(builder, enumerateWithoutComparisonAction)
                case enumerateWithOneComparisonAction: EnumerateWithOneComparisonAction =>
                    generateEnumerateWithOneComparisonAction(builder, enumerateWithOneComparisonAction)
                case enumerateWithTwoComparisonsAction: EnumerateWithTwoComparisonsAction =>
                    generateEnumerateWithTwoComparisonsAction(builder, enumerateWithTwoComparisonsAction)
                case enumerateWithMoreThanTwoComparisonsAction: EnumerateWithMoreThanTwoComparisonsAction =>
                    generateEnumerateWithMoreThanTwoComparisonsAction(builder, enumerateWithMoreThanTwoComparisonsAction)
                case formatResultAction: FormatResultAction =>
                    generateFormatResultAction(builder, formatResultAction)
                case countResultAction: CountResultAction =>
                    generateCountResultAction(builder, countResultAction)
                case _ => throw new UnsupportedOperationException()
            }
        }
    }

    def generateRootPrepareEnumerationAction(builder: mutable.StringBuilder, action: RootPrepareEnumerationAction): Unit = {
        if (action.joinKeyIndices.nonEmpty) {
            val newKey = if (action.joinKeyIndices.size == 1 && action.joinKeyExtractFuncs.size == 1) {
                action.joinKeyExtractFuncs.head(s"t._2(${action.joinKeyIndices.head})")
            } else {
                action.joinKeyIndices.zip(action.joinKeyExtractFuncs).map(t => {
                    val joinKeyIndex = t._1
                    val joinKeyExtractFunc = t._2
                    joinKeyExtractFunc(s"t._2($joinKeyIndex)")
                }).mkString("(", ", ", ")")
            }

            val func = action.extractIndicesInCurrent.map(i => s"t._2($i)").mkString(s"t => ($newKey, Array(", ",", "))")
            indent(builder, 2).append("val ").append(action.variableName).append(" = ")
                .append(action.fromVariableName).append(s".map($func)").append("\n")
        } else {
            // joinKeyIndices is empty only when the root relation is the only output relation and there is no more enumeration
            // in this case, we simply do nothing
        }
    }

    def generateEnumerateWithoutComparisonAction(builder: mutable.StringBuilder, action: EnumerateWithoutComparisonAction): Unit = {
        indent(builder, 2).append("val ").append(action.newVariableName).append(" = ")
            .append(action.intermediateResultVariableName).append(".enumerateWithoutComparison(")
            .append(action.currentVariableName).append(", ")
            .append(action.extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(action.extractIndicesInCurrent.mkString("Array(", ",", ")"))

        if (action.resultKeySelectors.nonEmpty) {
            builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
        }
        builder.append(")").append("\n")
    }

    def generateEnumerateWithOneComparisonAction(builder: mutable.StringBuilder, action: EnumerateWithOneComparisonAction): Unit = {
        indent(builder, 2).append("val ").append(action.newVariableName).append(" = ")
            .append(action.intermediateResultVariableName).append(s".enumerateWithOneComparison[${action.typeParameters}](")
            .append(action.currentVariableName).append(", ")
            .append(action.compareKeyIndexInIntermediateResult).append(", ")
            .append(action.compareKeyIndexInCurrent).append(", ")
            .append(action.compareFunc).append(", ")
            .append(action.extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(action.extractIndicesInCurrent.mkString("Array(", ",", ")"))

        if (action.resultKeySelectors.nonEmpty) {
            builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
        }
        builder.append(")").append("\n")
    }

    def generateEnumerateWithTwoComparisonsAction(builder: mutable.StringBuilder, action: EnumerateWithTwoComparisonsAction): Unit = {
        indent(builder, 2).append("val ").append(action.newVariableName).append(" = ")
            .append(action.intermediateResultVariableName).append(s".enumerateWithTwoComparisons[${action.typeParameters}](")
            .append(action.currentVariableName).append(", ")
            .append(action.compareKeyIndexInIntermediateResult1).append(", ")
            .append(action.compareKeyIndexInIntermediateResult2).append(", ")
            .append(action.extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(action.extractIndicesInCurrent.mkString("Array(", ",", ")"))

        if (action.resultKeySelectors.nonEmpty) {
            builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
        }
        builder.append(")").append("\n")
    }

    def generateEnumerateWithMoreThanTwoComparisonsAction(builder: mutable.StringBuilder, action: EnumerateWithMoreThanTwoComparisonsAction): Unit = {
        indent(builder, 2).append("val ").append(action.newVariableName).append(" = ")
            .append(action.intermediateResultVariableName).append(s".enumerateWithMoreThanTwoComparisons[${action.typeParameters}](")
            .append(action.currentVariableName).append(", ")
            .append(action.compareKeyIndexInIntermediateResult).append(", ")
            .append(action.compareKeyIndexInCurrent).append(", ")
            .append(action.compareFunc).append(", ")
            .append(action.extraFilters.mkString("(l, r) => (", " && ", ")")).append(", ")
            .append(action.extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(action.extractIndicesInCurrent.mkString("Array(", ",", ")"))

        if (action.resultKeySelectors.nonEmpty) {
            builder.append(", ").append(action.resultKeySelectors.map(s => s("l", "r")).mkString("(l, r) => (", ",", ")"))
        }
        builder.append(")").append("\n")
    }

    def generateFormatResultAction(builder: mutable.StringBuilder, action: FormatResultAction): Unit = {
        val formatters = action.formatters
        val mapFunc = formatters.indices.map(i => formatters(i).apply(s"x._2($i)")).mkString("x => Array(", ", ", ")")
        newLine(builder)
        indent(builder, 2).append("val ").append(action.variableName).append(" = ")
            .append(action.intermediateResultVariableName).append(".map(").append(mapFunc).append(")").append("\n")
        indent(builder, 2).append(action.variableName).append(".take(20).map(r => r.mkString(\",\")).foreach(println)").append("\n")
        indent(builder, 2).append("println(\"only showing top 20 rows\")").append("\n")
    }

    def generateCountResultAction(builder: mutable.StringBuilder, action: CountResultAction): Unit = {
        if (getLogger.nonEmpty) {
            newLine(builder)
            indent(builder, 2).append("val ts1 = System.currentTimeMillis()").append("\n")
            indent(builder, 2).append("val cnt = ").append(action.intermediateResultVariableName).append(".count()").append("\n")
            indent(builder, 2).append("val ts2 = System.currentTimeMillis()").append("\n")
            indent(builder, 2).append("LOGGER.info(\"").append(getQueryName).append("-SparkSQLPlus cnt: \" + cnt)").append("\n")
            indent(builder, 2).append("LOGGER.info(\"").append(getQueryName).append("-SparkSQLPlus time: \" + (ts2 - ts1) / 1000f)").append("\n")
        } else {
            newLine(builder)
            indent(builder, 2).append(action.intermediateResultVariableName).append(".count()").append("\n")
        }
    }
}