package sqlplus.codegen

import sqlplus.compile.{AppendCommonExtraColumnAction, AppendComparisonExtraColumnAction, ApplySelfComparisonAction, ApplySemiJoinAction, CountResultAction, CreateCommonExtraColumnAction, CreateComparisonExtraColumnAction, CreateComputationExtraColumnAction, CreateTransparentCommonExtraColumnAction, EndOfReductionAction, EnumerateAction, EnumerateWithMoreThanTwoComparisonsAction, EnumerateWithOneComparisonAction, EnumerateWithTwoComparisonsAction, EnumerateWithoutComparisonAction, FinalAction, FormatResultAction, MaterializeAggregatedRelationAction, MaterializeAuxiliaryRelationAction, MaterializeBagRelationAction, ReduceAction, RelationInfo, RootPrepareEnumerationAction}
import sqlplus.expression.{Operator, Variable}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable
import sqlplus.types.DataType

import scala.collection.mutable

abstract class AbstractSparkSQLPlusCodeGenerator(val comparisonOperators: Set[Operator], val sourceTables: Set[SqlPlusTable],
                              val relationIdToInfo: Map[Int, RelationInfo],
                              val reduceActions: List[ReduceAction], val enumerateActions: List[EnumerateAction], val finalAction: FinalAction)
    extends AbstractScalaCodeGenerator {
    val sourceTableNameToVariableNameDict = new mutable.HashMap[String, String]()
    val aggregatedRelationIdToVariableNameDict = new mutable.HashMap[Int, String]()
    val auxiliaryRelationIdToVariableNameDict = new mutable.HashMap[Int, String]()
    val bagRelationIdToVariableNameDict = new mutable.HashMap[Int, String]()
    val convertedSourceTableNameToVariableNameDict = new mutable.HashMap[String, String]()

    val extraColumnVariableToVariableNameDict = new mutable.HashMap[Variable, String]()

    val sourceRelationVariableToKeyedVariableNameDict = new mutable.HashMap[(String, List[Int]), String]()

    val activeRelationRecord = new ActiveRelationRecord()
    
    val variableNameAssigner = new VariableNameAssigner

    val treeLikeArrayVariableNameToTypeParametersDict = new mutable.HashMap[String, String]()
    val treeLikeArrayDictionaryVariableNameToTypeParametersDict = new mutable.HashMap[String, String]()

    def getAppName: String

    def getMaster: String

    def getLogger: String

    def getQueryName: String

    def getSourceTablePath(table: SqlPlusTable): String

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

        generateCompareFunctionDefinitions(builder)

        generateSourceTables(builder)

        generateReduction(builder)

        val variableName = generateEnumeration(builder)

        generateFinalAction(builder, variableName, finalAction)

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

    private def generateCompareFunctionDefinitions(builder: mutable.StringBuilder): Unit = {
        for (op <- comparisonOperators) {
            newLine(builder)

            for (row <- op.getFuncDefinition())
                indent(builder, 2).append(row).append("\n")
        }
    }

    private def generateSourceTables(builder: mutable.StringBuilder): Unit = {
        for (table <- sourceTables) {
            newLine(builder)

            assert(!sourceTableNameToVariableNameDict.contains(table.getTableName))
            val variableName = variableNameAssigner.getNewVariableName()
            sourceTableNameToVariableNameDict(table.getTableName) = variableName

            indent(builder, 2).append("val ").append(variableName).append(" = spark.sparkContext.textFile(")
                .append(getSourceTablePath(table)).append(").map(line => {").append("\n")
            indent(builder, 3).append("val fields = line.split(\",\")").append("\n")

            val fields = table.getTableColumns.zipWithIndex.map(columnAndIndex => {
                val dataType = DataType.fromTypeName(columnAndIndex._1.getType)
                dataType.fromString(s"fields(${columnAndIndex._2})")
            })
            indent(builder, 3).append("Array[Any](").append(fields.mkString(", ")).append(")").append("\n")
            indent(builder, 2).append("}).persist()").append("\n")
            indent(builder, 2).append(variableName).append(".count()").append("\n")
        }
    }

    private def generateReduction(builder: mutable.StringBuilder): Unit = {
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
                case materializeAggregatedRelationAction: MaterializeAggregatedRelationAction =>
                    generateMaterializeAggregatedRelationAction(builder, materializeAggregatedRelationAction)
                case EndOfReductionAction(relationId) =>
            }
        }
    }

    def generateCreateCommonExtraColumnAction(builder: mutable.StringBuilder, action: CreateCommonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val joinKeyTypes = action.joinKeyTypes
        val compareKeyIndex = action.compareKeyIndex
        val func = action.func
        val typeParameters = action.typeParameters

        val relationGroupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndices, joinKeyTypes)
        val sortedVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(sortedVariableName).append(" = ")
            .append(relationGroupedVariableName).append(s".sortValuesWith[$typeParameters](").append(compareKeyIndex).append(", ")
            .append(func).append(").persist()").append("\n")
        activeRelationRecord.addGroupedVariableName(relationId, joinKeyIndices, sortedVariableName)

        val extraColumnVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(extraColumnVariableName).append(" = ")
            .append(sortedVariableName).append(".extractFieldInHeadElement(").append(compareKeyIndex)
            .append(")").append("\n")
        extraColumnVariableToVariableNameDict(extraColumnVariable) = extraColumnVariableName
    }

    def generateCreateTransparentCommonExtraColumnAction(builder: mutable.StringBuilder, action: CreateTransparentCommonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val joinKeyTypes = action.joinKeyTypes

        // the join key with parent must be the 1st field
        assert(joinKeyIndices.size == 1 && joinKeyIndices.head == 0 && joinKeyTypes.size == 1)

        // we issue CreateTransparentCommonExtraColumnAction only for AggregatedRelation
        assert(aggregatedRelationIdToVariableNameDict.contains(relationId))
        val rawVariableName = aggregatedRelationIdToVariableNameDict(relationId)
        val newKey = joinKeyTypes.head.castFromAny("x(0)")
        val extraColumnVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(extraColumnVariableName).append(" = ")
            .append(rawVariableName).append(s".map(x => ($newKey, x(1)))").append("\n")
        extraColumnVariableToVariableNameDict(extraColumnVariable) = extraColumnVariableName
    }

    def generateCreateComparisonExtraColumnAction(builder: mutable.StringBuilder, action: CreateComparisonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val joinKeyTypes = action.joinKeyTypes
        val compareKeyIndex1 = action.compareKeyIndex1
        val compareKeyIndex2 = action.compareKeyIndex2
        val func1 = action.func1
        val func2 = action.func2
        val typeParameters = action.typeParameters

        val relationGroupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndices, joinKeyTypes)
        val treeLikeArrayVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(treeLikeArrayVariableName).append(" = ")
            .append(relationGroupedVariableName).append(s".constructTreeLikeArray[$typeParameters](")
            .append(compareKeyIndex1).append(", ")
            .append(compareKeyIndex2).append(", ")
            .append(func1).append(", ")
            .append(func2).append(")").append("\n")
        activeRelationRecord.addGroupedVariableName(relationId, joinKeyIndices, treeLikeArrayVariableName)
        treeLikeArrayVariableNameToTypeParametersDict(treeLikeArrayVariableName) = typeParameters

        val extraColumnVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(extraColumnVariableName).append(" = ")
            .append(treeLikeArrayVariableName).append(".createDictionary()").append("\n")
        extraColumnVariableToVariableNameDict(extraColumnVariable) = extraColumnVariableName
        treeLikeArrayDictionaryVariableNameToTypeParametersDict(extraColumnVariableName) = typeParameters
    }

    def generateCreateComputationExtraColumnAction(builder: mutable.StringBuilder, action: CreateComputationExtraColumnAction): Unit = {
        val relationId = action.relationId
        val columnVariable = action.columnVariable
        val keyIndices = action.keyIndices
        val keyTypes = action.keyTypes
        val functionGenerator = action.functionGenerator

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, keyIndices, keyTypes)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".appendExtraColumn(")
            .append("x => ").append(functionGenerator("x")).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, keyIndices, newVariableName)
    }

    def generateAppendCommonExtraColumnAction(builder: mutable.StringBuilder, action: AppendCommonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val joinKeyTypes = action.joinKeyTypes

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, joinKeyIndices, joinKeyTypes)
        val extraColumnVariableName = extraColumnVariableToVariableNameDict(extraColumnVariable)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".appendExtraColumn(")
            .append(extraColumnVariableName).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, joinKeyIndices, newVariableName)
    }

    def generateAppendComparisonExtraColumnAction(builder: mutable.StringBuilder, action: AppendComparisonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val joinKeyTypes = action.joinKeyTypes
        val compareKeyIndex = action.compareKeyIndex
        val func = action.func
        val compareTypeParameter = action.compareTypeParameter

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, joinKeyIndices, joinKeyTypes)
        val extraColumnVariableName = extraColumnVariableToVariableNameDict(extraColumnVariable)
        val newVariableName = variableNameAssigner.getNewVariableName()
        val typeParameters = s"${treeLikeArrayDictionaryVariableNameToTypeParametersDict(extraColumnVariableName)},$compareTypeParameter"
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(s".appendExtraColumn[$typeParameters](")
            .append(extraColumnVariableName).append(", ")
            .append(compareKeyIndex).append(", ")
            .append(func).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, joinKeyIndices, newVariableName)
    }

    def generateApplySelfComparisonAction(builder: mutable.StringBuilder, action: ApplySelfComparisonAction): Unit = {
        val relationId = action.relationId
        val keyIndices = action.keyIndices
        val keyTypes = action.keyTypes
        val functionGenerator = action.functionGenerator

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, keyIndices, keyTypes)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".filter(")
            .append("x => ").append(functionGenerator("x._2")).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, keyIndices, newVariableName)
    }

    def generateApplySemiJoinAction(builder: mutable.StringBuilder, action: ApplySemiJoinAction): Unit = {
        val currentRelationId = action.currentRelationId
        val childRelationId = action.childRelationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyTypesInCurrent = action.joinKeyTypesInCurrent
        val joinKeyIndicesInChild = action.joinKeyIndicesInChild
        val joinKeyTypesInChild = action.joinKeyTypesInChild

        val currentKeyedVariableName = getKeyedVariableNameByRelationId(builder, currentRelationId, joinKeyIndicesInCurrent, joinKeyTypesInCurrent)
        val childKeyedVariableName = getKeyedVariableNameByRelationId(builder, childRelationId, joinKeyIndicesInChild, joinKeyTypesInChild)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(currentKeyedVariableName).append(".semiJoin(")
            .append(childKeyedVariableName).append(")").append("\n")
        activeRelationRecord.clean(currentRelationId)
        activeRelationRecord.addKeyedVariableName(currentRelationId, joinKeyIndicesInCurrent, newVariableName)
    }

    // TODO: we can use the group by and sorted RDD directly, avoiding the extra distinct
    def generateMaterializeAuxiliaryRelationAction(builder: mutable.StringBuilder, action: MaterializeAuxiliaryRelationAction): Unit = {
        val relationId = action.relationId
        val supportingRelationId = action.supportingRelationId
        val projectIndices = action.projectIndices
        val projectTypes = action.projectTypes
        val supportingKeyedVariableName = getKeyedVariableNameByRelationId(builder, supportingRelationId, projectIndices, projectTypes)
        val newVariableName = variableNameAssigner.getNewVariableName()

        // we cannot use Array to deduplicate
        val func1 = projectIndices.map(i => s"x($i)").mkString("x => (", ", ", ")")
        // change back to Array after distinct
        val func2 = projectIndices.indices.map(i => s"x._${i+1}").mkString("x => Array(", ", ", ")")

        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(supportingKeyedVariableName).append(s".mapValues($func1).distinct().mapValues($func2)").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, projectIndices, newVariableName)
    }

    def generateMaterializeBagRelationAction(builder: mutable.StringBuilder, action: MaterializeBagRelationAction): Unit = {
        action.tableScanRelationNames.foreach(tableName => {
            if (convertedSourceTableNameToVariableNameDict.contains(tableName)) {
                convertedSourceTableNameToVariableNameDict(tableName)
            } else {
                val sourceVariableName = sourceTableNameToVariableNameDict(tableName)
                val convertedVariableName = variableNameAssigner.getNewVariableName()
                indent(builder, 2).append("val ").append(convertedVariableName).append(" = ").append(sourceVariableName)
                    .append(".map(").append("arr => arr.map(x => x.asInstanceOf[Int])").append(").cache()").append("\n")
                indent(builder, 2).append(convertedVariableName).append(".count()").append("\n")

                convertedSourceTableNameToVariableNameDict(tableName) = convertedVariableName
            }
        })
        val tableScanRelationVariableNames = action.tableScanRelationNames.map(convertedSourceTableNameToVariableNameDict).mkString("Array(", ",", ")")

        val bagVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(bagVariableName).append(" = spark.sparkContext.lftj(")
            .append(tableScanRelationVariableNames).append(", ")
            .append(action.relationCount).append(", ")
            .append(action.variableCount).append(", ").append("\n")
        indent(builder, 4).append(action.sourceTableIndexToRelations).append(", ").append("\n")
        indent(builder, 4).append(action.redirects).append(", ").append("\n")
        indent(builder, 4).append(action.variableIndices).append(").cache()").append("\n")

        bagRelationIdToVariableNameDict(action.relationId) = bagVariableName
    }

    def generateMaterializeAggregatedRelationAction(builder: mutable.StringBuilder, action: MaterializeAggregatedRelationAction): Unit = {
        action.aggregateFunction match {
            case "COUNT" =>
                val fromVariableName = sourceTableNameToVariableNameDict(action.tableName)
                val groupFields = action.groupIndices.map(i => "fields(" + i + ")").mkString("(", ", ", ")")
                val variableName = variableNameAssigner.getNewVariableName()
                indent(builder, 2).append("val ").append(variableName).append(" = ").append(fromVariableName)
                    .append(".map(fields => (").append(groupFields).append(", 1L))")
                    .append(".reduceByKey(_ + _)")
                    .append(".map(x => Array[Any](" + (0 to action.groupIndices.length).map(i => "x._" + (i + 1)).mkString(", ") + ")).persist()").append("\n")
                indent(builder, 2).append(variableName).append(".count()").append("\n")
                aggregatedRelationIdToVariableNameDict(action.relationId) = variableName
            case _ => throw new UnsupportedOperationException
        }
    }

    def getKeyedVariableNameByRelationId(builder: mutable.StringBuilder, relationId: Int, keyIndices: List[Int], keyTypes: List[DataType]): String = {
        if (activeRelationRecord.contains(relationId)) {
            val optKeyedVariableName = activeRelationRecord.getKeyedVariableNameWithKeys(relationId, keyIndices)
            if (optKeyedVariableName.nonEmpty) {
                optKeyedVariableName.get
            } else {
                val activeVariableName = activeRelationRecord.getKeyedVariableNameWithAnyKey(relationId).get
                val newVariableName = variableNameAssigner.getNewVariableName()
                val keyByFunc = if (keyIndices.size == 1 && keyTypes.size == 1) {
                    val cast = keyTypes.head.castFromAny(s"x(${keyIndices.head})")
                    s"x => $cast"
                } else {
                    keyIndices.zip(keyTypes).map(t => {
                        val keyIndex = t._1
                        val dataType = t._2
                        dataType.castFromAny(s"x($keyIndex)")
                    }).mkString("x => (", ", ", ")")
                }

                indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                    .append(activeVariableName).append(".reKeyBy(").append(keyByFunc)
                    .append(")").append("\n")
                activeRelationRecord.addKeyedVariableName(relationId, keyIndices, newVariableName)
                newVariableName
            }
        } else {
            val relationInfo = relationIdToInfo(relationId)
            val sourceVariable = relationInfo.getRelation() match {
                case aggregatedRelation: AggregatedRelation =>
                    aggregatedRelationIdToVariableNameDict(aggregatedRelation.getRelationId())
                case tableScanRelation: TableScanRelation =>
                    sourceTableNameToVariableNameDict(tableScanRelation.getTableName())
                case auxiliaryRelation: AuxiliaryRelation =>
                    auxiliaryRelationIdToVariableNameDict(auxiliaryRelation.getRelationId())
                case bagRelation: BagRelation =>
                    bagRelationIdToVariableNameDict(bagRelation.getRelationId())
            }

            // check if the source relation is already keyed
            if (sourceRelationVariableToKeyedVariableNameDict.contains(sourceVariable, keyIndices)) {
                val result = sourceRelationVariableToKeyedVariableNameDict((sourceVariable, keyIndices))
                activeRelationRecord.addKeyedVariableName(relationId, keyIndices, result)
                result
            } else {
                val newVariableName = variableNameAssigner.getNewVariableName()
                val keyByFunc = if (keyIndices.size == 1 && keyTypes.size == 1) {
                    val cast = keyTypes.head.castFromAny(s"x(${keyIndices.head})")
                    s"x => $cast"
                } else {
                    keyIndices.zip(keyTypes).map(t => {
                        val keyIndex = t._1
                        val dataType = t._2
                        dataType.castFromAny(s"x($keyIndex)")
                    }).mkString("x => (", ", ", ")")
                }
                indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                    .append(sourceVariable).append(".keyBy(").append(keyByFunc).append(")").append("\n")
                sourceRelationVariableToKeyedVariableNameDict((sourceVariable, keyIndices)) = newVariableName
                activeRelationRecord.addKeyedVariableName(relationId, keyIndices, newVariableName)
                newVariableName
            }
        }
    }

    def getKeyedVariableNameWithAnyKeyByRelationId(relationId: Int): String = {
        assert(activeRelationRecord.contains(relationId))
        activeRelationRecord.getKeyedVariableNameWithAnyKey(relationId).get
    }

    def getGroupedVariableNameByRelationId(builder: mutable.StringBuilder, relationId: Int, keyIndices: List[Int], keyTypes: List[DataType]): String = {
        val optGroupedVariableName = activeRelationRecord.getGroupedVariableNameWithKeys(relationId, keyIndices)
        if (optGroupedVariableName.nonEmpty) {
            optGroupedVariableName.get
        } else {
            val keyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, keyIndices, keyTypes)
            val newVariableName = variableNameAssigner.getNewVariableName()
            indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                .append(keyedVariableName).append(".groupBy()").append("\n")
            activeRelationRecord.addGroupedVariableName(relationId, keyIndices, newVariableName)
            newVariableName
        }
    }

    private def generateEnumeration(builder: mutable.StringBuilder): String = {
        newLine(builder)

        // the enumerateActions must be a RootPrepareEnumerationAction followed by 0 or more other EnumerationActions
        assert(enumerateActions.head.isInstanceOf[RootPrepareEnumerationAction])

        val intermediateVariableName = enumerateActions.head match {
            case rootPrepareEnumerationAction: RootPrepareEnumerationAction =>
                generateRootPrepareEnumerationAction(builder, rootPrepareEnumerationAction)
            case _ => throw new RuntimeException("enumerateActions must be a RootPrepareEnumerationAction " +
                "followed by 0 or more other EnumerationActions")
        }

        enumerateActions.tail.foldLeft(intermediateVariableName)((v, ea) => {
            ea match {
                case enumerateWithoutComparisonAction: EnumerateWithoutComparisonAction =>
                    generateEnumerateWithoutComparisonAction(builder, enumerateWithoutComparisonAction, v)
                case enumerateWithOneComparisonAction: EnumerateWithOneComparisonAction =>
                    generateEnumerateWithOneComparisonAction(builder, enumerateWithOneComparisonAction, v)
                case enumerateWithTwoComparisonsAction: EnumerateWithTwoComparisonsAction =>
                    generateEnumerateWithTwoComparisonsAction(builder, enumerateWithTwoComparisonsAction, v)
                case enumerateWithMoreThanTwoComparisonsAction: EnumerateWithMoreThanTwoComparisonsAction =>
                    generateEnumerateWithMoreThanTwoComparisonsAction(builder, enumerateWithMoreThanTwoComparisonsAction, v)
                case _ =>
                    throw new RuntimeException("enumerateActions must be a RootPrepareEnumerationAction " +
                        "followed by 0 or more other EnumerationActions")
            }
        })
    }

    def generateRootPrepareEnumerationAction(builder: mutable.StringBuilder, action: RootPrepareEnumerationAction): String = {
        val relationId = action.relationId
        val joinKeyIndices = action.joinKeyIndices
        val joinKeyTypes = action.joinKeyTypes
        val extractIndicesInCurrent = action.extractIndicesInCurrent

        if (joinKeyIndices.nonEmpty) {
            val variableName = getKeyedVariableNameWithAnyKeyByRelationId(relationId)
            val newVariableName = variableNameAssigner.getNewVariableName()

            val newKey = if (joinKeyIndices.size == 1 && joinKeyTypes.size == 1) {
                joinKeyTypes.head.castFromAny(s"t._2(${joinKeyIndices.head})")
            } else {
                joinKeyIndices.zip(joinKeyTypes).map(t => {
                    val joinKeyIndex = t._1
                    val joinKeyType = t._2
                    joinKeyType.castFromAny(s"t._2($joinKeyIndex)")
                }).mkString("(", ", ", ")")
            }

            val func = extractIndicesInCurrent.map(i => s"t._2($i)").mkString(s"t => ($newKey, Array(", ",", "))")
            indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                .append(variableName).append(s".map($func)").append("\n")
            newVariableName
        } else {
            // joinKeyIndices is empty only when the root relation is the only output relation and there is no more enumeration
            // in this case, we simply return the variable name of the root relation
            val variableName = getKeyedVariableNameWithAnyKeyByRelationId(relationId)
            variableName
        }
    }

    def generateEnumerateWithoutComparisonAction(builder: mutable.StringBuilder, action: EnumerateWithoutComparisonAction,
                                                 intermediateResultVariableName: String): String = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyTypesInCurrent = action.joinKeyTypesInCurrent
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndicesTypes = action.optResultKeyIsInIntermediateResultAndIndicesTypes
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndicesTypes)
            .map(s => s", $s").getOrElse("")

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent, joinKeyTypesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(".enumerateWithoutComparison(")
            .append(groupedVariableName).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(resultKeySelector)
            .append(")").append("\n")
        newVariableName
    }

    def generateEnumerateWithOneComparisonAction(builder: StringBuilder, action: EnumerateWithOneComparisonAction,
                                                 intermediateResultVariableName: String): String = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyTypesInCurrent = action.joinKeyTypesInCurrent
        val compareKeyIndexInCurrent = action.compareKeyIndexInCurrent
        val compareKeyIndexInIntermediateResult = action.compareKeyIndexInIntermediateResult
        val func = action.func
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndicesTypes = action.optResultKeyIsInIntermediateResultAndIndicesTypes
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndicesTypes)
            .map(s => s", $s").getOrElse("")
        val typeParameters = action.typeParameters

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent, joinKeyTypesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(s".enumerateWithOneComparison[$typeParameters](")
            .append(groupedVariableName).append(", ")
            .append(compareKeyIndexInIntermediateResult).append(", ")
            .append(compareKeyIndexInCurrent).append(", ")
            .append(func).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(resultKeySelector)
            .append(")").append("\n")
        newVariableName
    }

    def generateEnumerateWithTwoComparisonsAction(builder: StringBuilder, action: EnumerateWithTwoComparisonsAction,
                                                  intermediateResultVariableName: String): String = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyTypesInCurrent = action.joinKeyTypesInCurrent
        val compareKeyIndexInIntermediateResult1 = action.compareKeyIndexInIntermediateResult1
        val compareKeyIndexInIntermediateResult2 = action.compareKeyIndexInIntermediateResult2
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndicesTypes = action.optResultKeyIsInIntermediateResultAndIndicesTypes
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndicesTypes)
            .map(s => s", $s").getOrElse("")
        val compareAndResultTypeParameters = action.compareAndResultTypeParameters

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent, joinKeyTypesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        val typeParameters = s"${treeLikeArrayVariableNameToTypeParametersDict(groupedVariableName)},$compareAndResultTypeParameters"
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(s".enumerateWithTwoComparisons[${typeParameters}](")
            .append(groupedVariableName).append(", ")
            .append(compareKeyIndexInIntermediateResult1).append(", ")
            .append(compareKeyIndexInIntermediateResult2).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(resultKeySelector)
            .append(")").append("\n")
        newVariableName
    }

    def generateEnumerateWithMoreThanTwoComparisonsAction(builder: StringBuilder, action: EnumerateWithMoreThanTwoComparisonsAction,
                                                          intermediateResultVariableName: String): String = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyTypesInCurrent = action.joinKeyTypesInCurrent
        val compareKeyIndexInCurrent = action.compareKeyIndexInCurrent
        val compareKeyIndexInIntermediateResult = action.compareKeyIndexInIntermediateResult
        val func = action.func
        val extraFilters = action.extraFilters
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndicesTypes = action.optResultKeyIsInIntermediateResultAndIndicesTypes
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndicesTypes)
            .map(s => s", $s").getOrElse("")
        val typeParameters = action.typeParameters

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent, joinKeyTypesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(s".enumerateWithMoreThanTwoComparisons[${typeParameters}](")
            .append(groupedVariableName).append(", ")
            .append(compareKeyIndexInIntermediateResult).append(", ")
            .append(compareKeyIndexInCurrent).append(", ")
            .append(func).append(", ")
            .append(extraFilters.mkString("(l, r) => (", " && ", ")")).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(resultKeySelector)
            .append(")").append("\n")
        newVariableName
    }

    def generateFormatResultAction(builder: StringBuilder, action: FormatResultAction,
                                   intermediateResultVariableName: String): Unit = {
        val formatters = action.formatters
        val mapFunc = formatters.indices.map(i => formatters(i).apply(s"x._2($i)")).mkString("x => Array(", ", ", ")")
        val newVariableName = variableNameAssigner.getNewVariableName()
        newLine(builder)
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(".map(").append(mapFunc).append(")").append("\n")
        indent(builder, 2).append(newVariableName).append(".take(20).map(r => r.mkString(\",\")).foreach(println)").append("\n")
        indent(builder, 2).append("println(\"only showing top 20 rows\")").append("\n")
    }

    def generateCountResultAction(builder: StringBuilder, intermediateResultVariableName: String): Unit = {
        if (getLogger.nonEmpty) {
            newLine(builder)
            indent(builder, 2).append("val ts1 = System.currentTimeMillis()").append("\n")
            indent(builder, 2).append("val cnt = ").append(intermediateResultVariableName).append(".count()").append("\n")
            indent(builder, 2).append("val ts2 = System.currentTimeMillis()").append("\n")
            indent(builder, 2).append("LOGGER.info(\"").append(getQueryName).append("-SparkSQLPlus cnt: \" + cnt)").append("\n")
            indent(builder, 2).append("LOGGER.info(\"").append(getQueryName).append("-SparkSQLPlus time: \" + (ts2 - ts1) / 1000f)").append("\n")
        } else {
            newLine(builder)
            indent(builder, 2).append(intermediateResultVariableName).append(".count()").append("\n")
        }
    }

    def generateFinalAction(builder: StringBuilder, finalVariableName: String, action: FinalAction): Unit = {
        action match {
            case f: FormatResultAction => generateFormatResultAction(builder, f, finalVariableName)
            case CountResultAction => generateCountResultAction(builder, finalVariableName)
        }
    }

    def getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndicesTypes: Option[List[(Boolean, Int, DataType)]]): Option[String] = {
        optResultKeyIsInIntermediateResultAndIndicesTypes.map(list => {
            val fields = list.map(t => {
                // t._1 indicates whether this index is in the intermediate result
                val isInIntermediateResult = t._1
                val index = t._2
                val dataType = t._3
                if (isInIntermediateResult) dataType.castFromAny(s"l($index)") else dataType.castFromAny(s"r($index)")
            })

            val selector = fields.mkString("(l, r) => (", ",", ")")
            selector
        })
    }
}

class ActiveRelationRecord {
    // relationId -> [groupByKeyIndex1 -> variableName1, (...) -> ..., ...]
    private val groupedDict: mutable.HashMap[Int, mutable.HashMap[List[Int], String]] = mutable.HashMap.empty

    // relationId -> [keyIndex1 -> variableName1, ...]
    private val keyedDict: mutable.HashMap[Int, mutable.HashMap[List[Int], String]] = mutable.HashMap.empty

    private val activeRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty

    def clean(relationId: Int): Unit = {
        groupedDict(relationId) = mutable.HashMap.empty
        keyedDict(relationId) = mutable.HashMap.empty
        activeRelationIds.remove(relationId)
    }

    def contains(relationId: Int): Boolean = {
        activeRelationIds.contains(relationId)
    }

    def getKeyedVariableNameWithKeys(relationId: Int, keyIndices: List[Int]): Option[String] = {
        keyedDict.get(relationId).flatMap(m => m.get(keyIndices))
    }

    def getKeyedVariableNameWithAnyKey(relationId: Int): Option[String] = {
        keyedDict.get(relationId).map(m => m.head._2)
    }

    def addKeyedVariableName(relationId: Int, keyIndices: List[Int], variableName: String): Unit = {
        keyedDict.getOrElseUpdate(relationId, mutable.HashMap.empty)(keyIndices) = variableName
        activeRelationIds.add(relationId)
    }

    def getGroupedVariableNameWithKeys(relationId: Int, keyIndices: List[Int]): Option[String] = {
        groupedDict.get(relationId).flatMap(m => m.get(keyIndices))
    }

    def addGroupedVariableName(relationId: Int, keyIndices: List[Int], variableName: String): Unit = {
        groupedDict.getOrElseUpdate(relationId, mutable.HashMap.empty)(keyIndices) = variableName
    }
}

class VariableNameAssigner {
    private var suffix = 1

    def getNewVariableName(): String = {
        val result = "v" + suffix.toString
        suffix += 1
        result
    }
}