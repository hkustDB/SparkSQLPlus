package sqlplus.codegen

import sqlplus.compile.{AppendCommonExtraColumnAction, AppendComparisonExtraColumnAction, ApplySelfComparisonAction, ApplySemiJoinAction, CreateCommonExtraColumnAction, CreateComparisonExtraColumnAction, CreateComputationExtraColumnAction, EndOfReductionAction, EnumerateAction, EnumerateWithOneComparisonAction, EnumerateWithTwoComparisonsAction, EnumerateWithoutComparisonAction, FinalOutputAction, ReduceAction, RelationInfo}
import sqlplus.expression.{ComparisonOperator, Variable}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable

import scala.collection.mutable

class SparkScalaCodeGenerator(val comparisonOperators: Set[ComparisonOperator], val sourceTables: Set[SqlPlusTable],
                              val aggregatedRelations: List[AggregatedRelation], val auxiliaryRelations: List[AuxiliaryRelation],
                              val bagRelations: List[BagRelation],
                              val relationIdToInfo: Map[Int, RelationInfo],
                              val reduceActions: List[ReduceAction], val enumerateActions: List[EnumerateAction],
                              val packageName: String, objectName: String)
    extends AbstractScalaCodeGenerator {
    val sourceTableNameToVariableNameDict = new mutable.HashMap[String, String]()
    val aggregatedRelationIdToVariableNameDict = new mutable.HashMap[Int, String]()
    val auxiliaryRelationIdToVariableNameDict = new mutable.HashMap[Int, String]()
    val bagRelationIdToVariableNameDict = new mutable.HashMap[Int, String]()

    val extraColumnVariableToVariableNameDict = new mutable.HashMap[Variable, String]()

    val sourceRelationVariableToKeyedVariableNameDict = new mutable.HashMap[(String, List[Int]), String]()

    val activeRelationRecord = new ActiveRelationRecord()

    var lastRelationId: Int = -1
    
    val variableNameAssigner = new VariableNameAssigner

    override def getPackageName: String = packageName

    override def getImports: List[String] = List(
        "sqlplus.helper.ImplicitConversions._",
        "org.apache.spark.sql.SparkSession",
        "org.apache.spark.{SparkConf, SparkContext}"
    )

    override def getType: String = "object"

    override def getName: String = objectName

    override def getConstructorParameters: List[Parameter] = List()

    override def getExtends: String = ""

    override def getSuperClassParameters: List[String] = List()

    override def generateBody(builder: StringBuilder): Unit = {
        generateExecuteMethod(builder)
    }

    private def generateExecuteMethod(builder: StringBuilder): Unit = {
        indent(builder, 1).append("def main(args: Array[String]): Unit = {").append("\n")

        generateSparkInit(builder)
        newLine(builder)

        generateCompareFunctionDefinitions(builder)
        newLine(builder)

        generateSourceTables(builder)
        generateAggregatedSourceTables(builder)
        generateBagSourceTables(builder)
        generateAuxiliarySourceTables(builder)
        newLine(builder)

        generateReduction(builder)
        newLine(builder)

        generateEnumeration(builder)
        newLine(builder)

        generateSparkClose(builder)
        indent(builder, 1).append("}").append("\n")
    }

    private def generateSparkInit(builder: StringBuilder): Unit = {
        indent(builder, 2).append("val conf = new SparkConf()").append("\n")
        indent(builder, 2).append("conf.setAppName(\"SparkSQLPlusExample\")").append("\n")
        indent(builder, 2).append("conf.setMaster(\"local\")").append("\n")
        indent(builder, 2).append("val sc = new SparkContext(conf)").append("\n")
        indent(builder, 2).append("sc.defaultParallelism").append("\n")
        indent(builder, 2).append("val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()").append("\n")
    }

    private def generateSparkClose(builder: StringBuilder): Unit = {
        indent(builder, 2).append("sparkSession.close()").append("\n")
    }

    private def generateCompareFunctionDefinitions(builder: StringBuilder): Unit = {
        for (op <- comparisonOperators)
            indent(builder, 2).append(op.getFuncDefinition()).append("\n")
    }

    private def generateSourceTables(builder: StringBuilder): Unit = {
        for (table <- sourceTables) {
            assert(!sourceTableNameToVariableNameDict.contains(table.getTableName))
            val variableName = variableNameAssigner.getNewVariableName()
            sourceTableNameToVariableNameDict(table.getTableName) = variableName

            val path = table.getTableProperties.get("path")
            indent(builder, 2).append("val ").append(variableName).append(" = sc.textFile(\"")
                .append(path).append("\").map(line => {").append("\n")
            indent(builder, 3).append("val fields = line.split(\"\\\\s+\")").append("\n")

            val fields = table.getTableColumns.zipWithIndex.map(columnAndIndex =>
                getFieldTypeConvertor("fields(" + columnAndIndex._2 + ")", columnAndIndex._1.getType))
            indent(builder, 3).append("Array[Any](").append(fields.mkString(", ")).append(")").append("\n")
            indent(builder, 2).append("})").append("\n")

            newLine(builder)
        }
    }

    private def generateAggregatedSourceTables(builder: StringBuilder): Unit = {
        for (relation <- aggregatedRelations) {
            relation.func match {
                case "COUNT" =>
                    val fromVariableName = sourceTableNameToVariableNameDict(relation.tableName)
                    val groupFields = relation.group.map(i => "fields(" + i + ")").mkString("(", ", ", ")")
                    val variableName = variableNameAssigner.getNewVariableName()
                    indent(builder, 2).append("val ").append(variableName).append(" = ").append(fromVariableName)
                        .append(".map(fields => (").append(groupFields).append(", 1))")
                        .append(".reduceByKey(_ + _)")
                        .append(".map(x => Array[Any](" + (0 to relation.group.length).map(i => "x._" + (i + 1)).mkString(", ") + "))").append("\n")
                    aggregatedRelationIdToVariableNameDict(relation.getRelationId()) = variableName
                case _ => throw new UnsupportedOperationException
            }
        }
    }

    private def generateBagSourceTables(builder: StringBuilder): Unit = {
        for (bagRelation <- bagRelations) {
            // only support triangle
            assert(bagRelation.getInternalRelation.size == 3)
            // should not put AuxiliaryRelation in BagRelation
            assert(bagRelation.getInternalRelation.forall(r => !r.isInstanceOf[AuxiliaryRelation]))
            // only support normal tables
            assert(bagRelation.getInternalRelation.forall(r => r.isInstanceOf[TableScanRelation]))
            val relation1 = bagRelation.getInternalRelation(0)
            val relation2 = bagRelation.getInternalRelation(1)
            val relation3 = bagRelation.getInternalRelation(2)
            // join relation1 and relation2
            val variableList1 = relation1.getVariableList()
            val variableList2 = relation2.getVariableList()
            val variableList3 = relation3.getVariableList()
            // keep all the variables
            val variableList12 = variableList1 ++ variableList2
            val variableList123 = variableList1 ++ variableList2 ++ variableList3

            val joinVariables12 = variableList1.toSet.intersect(variableList2.toSet).toList.sortBy(v => v.name)
            val joinVariables123 = variableList12.toSet.intersect(variableList3.toSet).toList.sortBy(v => v.name)

            val variableIndicesDict1 = relation1.getVariableList().zipWithIndex.toMap
            val variableIndicesDict2 = relation2.getVariableList().zipWithIndex.toMap
            val variableIndicesDict3 = relation3.getVariableList().zipWithIndex.toMap
            val variableIndicesDict12 = variableList12.zipWithIndex.toMap
            val variableIndicesDict123 = variableList123.zipWithIndex.toMap

            val joinIndices1 = joinVariables12.map(v => variableIndicesDict1(v))
            val joinIndices2 = joinVariables12.map(v => variableIndicesDict2(v))
            val joinIndices12 = joinVariables123.map(v => variableIndicesDict12(v))
            val joinIndices3 = joinVariables123.map(v => variableIndicesDict3(v))

            val variableName1 = sourceTableNameToVariableNameDict(relation1.asInstanceOf[TableScanRelation].tableName)
            val keyByFunc1 = if (joinIndices1.size == 1) s"x => Tuple1(x(${joinIndices1.head}))"
                else joinIndices1.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")")
            val variableName2 = sourceTableNameToVariableNameDict(relation2.asInstanceOf[TableScanRelation].tableName)
            val keyByFunc2 = if (joinIndices2.size == 1) s"x => Tuple1(x(${joinIndices2.head}))"
                else joinIndices2.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")")
            val keyByFunc12 = if (joinIndices12.size == 1) s"x => Tuple1(x(${joinIndices12.head}))"
                else joinIndices12.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")")

            val variableName3 = sourceTableNameToVariableNameDict(relation2.asInstanceOf[TableScanRelation].tableName)
            val keyByFunc3 = if (joinIndices3.size == 1) s"x => Tuple1(x(${joinIndices3.head}))"
                else joinIndices3.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")")
            val bagVariableName = variableNameAssigner.getNewVariableName()
            val resultIndices = bagRelation.getVariableList().map(v => variableIndicesDict123(v)).mkString("Array(", ",", ")")
            indent(builder, 2).append("val ").append(bagVariableName).append(" = bag(")
                .append(s"$variableName1, $keyByFunc1, $variableName2, $keyByFunc2, $keyByFunc12, $variableName3, $keyByFunc3, $resultIndices)")
                .append("\n")

            bagRelationIdToVariableNameDict(bagRelation.getRelationId()) = bagVariableName
        }
    }

    private def generateAuxiliarySourceTables(builder: StringBuilder): Unit = {
        for (relation <- auxiliaryRelations) {
            val sourceRelation = relation.sourceRelation
            sourceRelation match {
                case tableScanRelation: TableScanRelation =>
                    val sourceVariableList = tableScanRelation.getVariableList()
                    val sourceVariableIndicesMap = sourceVariableList.zipWithIndex.toMap
                    val expectedVariableList = relation.getVariableList()
                    val expectedVariableIndices = expectedVariableList.map(sourceVariableIndicesMap)
                    val fromVariableName = sourceTableNameToVariableNameDict(tableScanRelation.tableName)
                    val func = expectedVariableIndices.map(i => s"x($i)").mkString("x => Array(", ", ", ")")
                    val variableName = variableNameAssigner.getNewVariableName()
                    indent(builder, 2).append("val ").append(variableName).append(" = ").append(fromVariableName)
                        .append(".map(").append(func).append(")").append("\n")
                    auxiliaryRelationIdToVariableNameDict(relation.getRelationId()) = variableName
                case _ => throw new UnsupportedOperationException
            }
        }
    }

    private def generateReduction(builder: StringBuilder): Unit = {
        for (action <- reduceActions) {
            action match {
                case createCommonExtraColumnAction: CreateCommonExtraColumnAction =>
                    generateCreateCommonExtraColumnAction(builder, createCommonExtraColumnAction)
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
                case EndOfReductionAction(relationId) =>
                    lastRelationId = relationId
            }
        }
    }

    def generateCreateCommonExtraColumnAction(builder: StringBuilder, action: CreateCommonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val compareKeyIndex = action.compareKeyIndex
        val func = action.func

        val relationGroupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndices)
        val sortedVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(sortedVariableName).append(" = ")
            .append(relationGroupedVariableName).append(".sortValuesWith(").append(compareKeyIndex).append(", ")
            .append(func).append(")").append("\n")
        activeRelationRecord.addGroupedVariableName(relationId, joinKeyIndices, sortedVariableName)

        val extraColumnVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(extraColumnVariableName).append(" = ")
            .append(sortedVariableName).append(".extractFieldInHeadElement(").append(compareKeyIndex)
            .append(")").append("\n")
        extraColumnVariableToVariableNameDict(extraColumnVariable) = extraColumnVariableName
    }

    def generateCreateComparisonExtraColumnAction(builder: StringBuilder, action: CreateComparisonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val compareKeyIndex1 = action.compareKeyIndex1
        val compareKeyIndex2 = action.compareKeyIndex2
        val func1 = action.func1
        val func2 = action.func2

        val relationGroupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndices)
        val treeLikeArrayVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(treeLikeArrayVariableName).append(" = ")
            .append(relationGroupedVariableName).append(".constructTreeLikeArray(")
            .append(compareKeyIndex1).append(", ")
            .append(compareKeyIndex2).append(", ")
            .append(func1).append(", ")
            .append(func2).append(")").append("\n")
        activeRelationRecord.addGroupedVariableName(relationId, joinKeyIndices, treeLikeArrayVariableName)

        val extraColumnVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(extraColumnVariableName).append(" = ")
            .append(treeLikeArrayVariableName).append(".createDictionary()").append("\n")
        extraColumnVariableToVariableNameDict(extraColumnVariable) = extraColumnVariableName
    }

    def generateCreateComputationExtraColumnAction(builder: StringBuilder, action: CreateComputationExtraColumnAction): Unit = {
        val relationId = action.relationId
        val columnVariable = action.columnVariable
        val keyIndices = action.keyIndices
        val functionGenerator = action.functionGenerator

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, keyIndices)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".appendExtraColumn(")
            .append("x => ").append(functionGenerator("x")).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, keyIndices, newVariableName)
    }

    def generateAppendCommonExtraColumnAction(builder: StringBuilder, action: AppendCommonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, joinKeyIndices)
        val extraColumnVariableName = extraColumnVariableToVariableNameDict(extraColumnVariable)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".appendExtraColumn(")
            .append(extraColumnVariableName).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, joinKeyIndices, newVariableName)
    }

    def generateAppendComparisonExtraColumnAction(builder: StringBuilder, action: AppendComparisonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val compareKeyIndex = action.compareKeyIndex
        val func = action.func

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, joinKeyIndices)
        val extraColumnVariableName = extraColumnVariableToVariableNameDict(extraColumnVariable)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".appendExtraColumn(")
            .append(extraColumnVariableName).append(", ")
            .append(compareKeyIndex).append(", ")
            .append(func).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, joinKeyIndices, newVariableName)
    }

    def generateApplySelfComparisonAction(builder: StringBuilder, action: ApplySelfComparisonAction): Unit = {
        val relationId = action.relationId
        val keyIndices = action.keyIndices
        val functionGenerator = action.functionGenerator

        val relationKeyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, keyIndices)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(relationKeyedVariableName).append(".filter(")
            .append("x => ").append(functionGenerator("x._2")).append(")").append("\n")
        activeRelationRecord.clean(relationId)
        activeRelationRecord.addKeyedVariableName(relationId, keyIndices, newVariableName)
    }

    def generateApplySemiJoinAction(builder: StringBuilder, action: ApplySemiJoinAction): Unit = {
        val currentRelationId = action.currentRelationId
        val childRelationId = action.childRelationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyIndicesInChild = action.joinKeyIndicesInChild

        val currentKeyedVariableName = getKeyedVariableNameByRelationId(builder, currentRelationId, joinKeyIndicesInCurrent)
        val childKeyedVariableName = getKeyedVariableNameByRelationId(builder, childRelationId, joinKeyIndicesInChild)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(currentKeyedVariableName).append(".semiJoin(")
            .append(childKeyedVariableName).append(")").append("\n")
        activeRelationRecord.clean(currentRelationId)
        activeRelationRecord.addKeyedVariableName(currentRelationId, joinKeyIndicesInCurrent, newVariableName)
    }

    def getKeyedVariableNameByRelationId(builder: StringBuilder, relationId: Int, keyIndices: List[Int]): String = {
        if (activeRelationRecord.contains(relationId)) {
            val optKeyedVariableName = activeRelationRecord.getKeyedVariableNameWithKeys(relationId, keyIndices)
            if (optKeyedVariableName.nonEmpty) {
                optKeyedVariableName.get
            } else {
                val activeVariableName = activeRelationRecord.getKeyedVariableNameWithAnyKey(relationId).get
                val newVariableName = variableNameAssigner.getNewVariableName()
                val func = keyIndices.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")") // TODO: type
                indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                    .append(activeVariableName).append(".reKeyBy(").append(func)
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
                val func = keyIndices.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")") // TODO: type
                indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                    .append(sourceVariable).append(".keyBy(").append(func).append(")").append("\n")
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

    def getGroupedVariableNameByRelationId(builder: StringBuilder, relationId: Int, keyIndices: List[Int]): String = {
        val optGroupedVariableName = activeRelationRecord.getGroupedVariableNameWithKeys(relationId, keyIndices)
        if (optGroupedVariableName.nonEmpty) {
            optGroupedVariableName.get
        } else {
            val keyedVariableName = getKeyedVariableNameByRelationId(builder, relationId, keyIndices)
            val newVariableName = variableNameAssigner.getNewVariableName()
            indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                .append(keyedVariableName).append(".groupBy()").append("\n")
            activeRelationRecord.addGroupedVariableName(relationId, keyIndices, newVariableName)
            newVariableName
        }
    }

    private def generateEnumeration(builder: StringBuilder): Unit = {
        def processEnumerationActions(startVariableName: String, startKeyIndices: List[Int]): String = {
            enumerateActions.foldLeft((startVariableName, startKeyIndices))((tuple, action) => {
                val intermediateResultVariableName = tuple._1
                val intermediateResultKeyIndices = tuple._2
                action match {
                    case enumerateWithoutComparisonAction: EnumerateWithoutComparisonAction =>
                        generateEnumerateWithoutComparisonAction(builder, enumerateWithoutComparisonAction,
                            intermediateResultVariableName, intermediateResultKeyIndices)
                    case enumerateWithOneComparisonAction: EnumerateWithOneComparisonAction =>
                        generateEnumerateWithOneComparisonAction(builder, enumerateWithOneComparisonAction,
                            intermediateResultVariableName, intermediateResultKeyIndices)
                    case enumerateWithTwoComparisonsAction: EnumerateWithTwoComparisonsAction =>
                        generateEnumerateWithTwoComparisonsAction(builder, enumerateWithTwoComparisonsAction,
                            intermediateResultVariableName, intermediateResultKeyIndices)
                    case finalOutputAction: FinalOutputAction =>
                        generateFinalOutputAction(builder, finalOutputAction, intermediateResultVariableName)
                }
            })._1
        }

        def generateFinalCount(variableName: String): Unit = {
            indent(builder, 2).append(variableName).append(".count()").append("\n")
        }

        val finalVariableName: String = enumerateActions.head match {
            case enumerateWithoutComparisonAction: EnumerateWithoutComparisonAction =>
                val startKeyIndices = enumerateWithoutComparisonAction.joinKeyIndicesInIntermediateResult
                val startVariableName = getKeyedVariableNameByRelationId(builder, lastRelationId, startKeyIndices)
                processEnumerationActions(startVariableName, startKeyIndices)
            case enumerateWithOneComparisonAction: EnumerateWithOneComparisonAction =>
                val startKeyIndices = enumerateWithOneComparisonAction.joinKeyIndicesInIntermediateResult
                val startVariableName = getKeyedVariableNameByRelationId(builder, lastRelationId, startKeyIndices)
                processEnumerationActions(startVariableName, startKeyIndices)
            case enumerateWithTwoComparisonsAction: EnumerateWithTwoComparisonsAction =>
                val startKeyIndices = enumerateWithTwoComparisonsAction.joinKeyIndicesInIntermediateResult
                val startVariableName = getKeyedVariableNameByRelationId(builder, lastRelationId, startKeyIndices)
                processEnumerationActions(startVariableName, startKeyIndices)
            case finalOutputAction: FinalOutputAction => // no more EnumerationActions
                val (variableName, _) = generateFinalOutputAction(builder, finalOutputAction, getKeyedVariableNameWithAnyKeyByRelationId(lastRelationId))
                variableName
        }

        generateFinalCount(finalVariableName)
    }

    def generateEnumerateWithoutComparisonAction(builder: StringBuilder, action: EnumerateWithoutComparisonAction,
                                                 intermediateResultVariableName: String, intermediateResultKeyIndices: List[Int]): (String, List[Int]) = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyIndicesInIntermediateResult = action.joinKeyIndicesInIntermediateResult
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult

        val variableName = reKeyIntermediateResultIfNeeded(builder, intermediateResultVariableName,
            intermediateResultKeyIndices, joinKeyIndicesInIntermediateResult)
        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(variableName).append(".enumerate(")
            .append(groupedVariableName).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(")").append("\n")
        val newKeyIndices = getNewKeyIndexInIntermediateResult(extractIndicesInIntermediateResult, joinKeyIndicesInIntermediateResult)
        (newVariableName, newKeyIndices)
    }

    def generateEnumerateWithOneComparisonAction(builder: StringBuilder, action: EnumerateWithOneComparisonAction,
                                                 intermediateResultVariableName: String, intermediateResultKeyIndices: List[Int]): (String, List[Int]) = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyIndicesInIntermediateResult = action.joinKeyIndicesInIntermediateResult
        val compareKeyIndexInCurrent = action.compareKeyIndexInCurrent
        val compareKeyIndexInIntermediateResult = action.compareKeyIndexInIntermediateResult
        val func = action.func
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult

        val variableName = reKeyIntermediateResultIfNeeded(builder, intermediateResultVariableName,
            intermediateResultKeyIndices, joinKeyIndicesInIntermediateResult)
        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(variableName).append(".enumerate(")
            .append(groupedVariableName).append(", ")
            .append(compareKeyIndexInIntermediateResult).append(", ")
            .append(compareKeyIndexInCurrent).append(", ")
            .append(func).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(")").append("\n")
        val newKeyIndices = getNewKeyIndexInIntermediateResult(extractIndicesInIntermediateResult, joinKeyIndicesInIntermediateResult)
        (newVariableName, newKeyIndices)
    }

    def generateEnumerateWithTwoComparisonsAction(builder: StringBuilder, action: EnumerateWithTwoComparisonsAction,
                                                  intermediateResultVariableName: String, intermediateResultKeyIndices: List[Int]): (String, List[Int]) = {
        val relationId = action.relationId
        val joinKeyIndicesInCurrent = action.joinKeyIndicesInCurrent
        val joinKeyIndicesInIntermediateResult = action.joinKeyIndicesInIntermediateResult
        val compareKeyIndexInIntermediateResult1 = action.compareKeyIndexInIntermediateResult1
        val compareKeyIndexInIntermediateResult2 = action.compareKeyIndexInIntermediateResult2
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult

        val variableName = reKeyIntermediateResultIfNeeded(builder, intermediateResultVariableName,
            intermediateResultKeyIndices, joinKeyIndicesInIntermediateResult)
        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(variableName).append(".enumerate(")
            .append(groupedVariableName).append(", ")
            .append(compareKeyIndexInIntermediateResult1).append(", ")
            .append(compareKeyIndexInIntermediateResult2).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(")").append("\n")
        val newKeyIndices = getNewKeyIndexInIntermediateResult(extractIndicesInIntermediateResult, joinKeyIndicesInIntermediateResult)
        (newVariableName, newKeyIndices)
    }

    def generateFinalOutputAction(builder: StringBuilder, action: FinalOutputAction,
                                  intermediateResultVariableName: String): (String, List[Int]) = {
        val extracts = action.outputVariableIndices.map(i => s"t._2($i)")
        val func = s"t => ${extracts.mkString("Array(", ", ", ")")}"
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(".map(").append(func).append(")").append("\n")
        (newVariableName, List(0))
    }

    def reKeyIntermediateResultIfNeeded(builder: StringBuilder, intermediateResultVariableName: String,
                                        intermediateResultKeyIndices: List[Int], expectedKeyIndices: List[Int]): String = {
        if (intermediateResultKeyIndices == expectedKeyIndices) {
            intermediateResultVariableName
        } else {
            val newVariableName = variableNameAssigner.getNewVariableName()
            val func = expectedKeyIndices.map(i => s"x($i).asInstanceOf[Int]").mkString("x => (", ", ", ")")
            indent(builder, 2).append("val ").append(newVariableName).append(" = ")
                .append(intermediateResultVariableName).append(".reKeyBy(")
                .append(func).append(")").append("\n")
            newVariableName
        }
    }

    /**
     * Get the new key index. The key index may change due to the extractIndices are not full.
     * e.g., assuming the intermediate result has columns [var1, var2, var3, var4],
     * the key index of intermediate result is 2. So each row looks like (var3, (var1, var2, var3, var4))
     * Suppose the extractIndices for intermediate result is Array(0,2,3),
     * then var2 is not in the new intermediate result but the key is still var3.
     * Now the new key index is 1 since the new intermediate result has columns [var1, var3, var4, var*...]
     *
     * case 1: if the key column is preserved after the extraction,
     * The new key index can be calculated by counting the indices in extractIndices that is strictly smaller than
     * the original key index. Here only index = 1 satisfy (i less than 2), so the new key index is 1. This approach comes the
     * fact that the indices must be in an ascending order.
     *
     * case 2: if the key column is dropped after the extraction,
     * The new key index will be set to -1 to indicate that the key is no longer in the content.
     *
     * @param extractIndices
     * @param keyIndices
     * @return
     */
    def getNewKeyIndexInIntermediateResult(extractIndices: List[Int], keyIndices: List[Int]): List[Int] = {
        def fun(keyIndex: Int): Int = {
            if (extractIndices.contains(keyIndex))
                extractIndices.takeWhile(i => i < keyIndex).size
            else
                -1
        }

        keyIndices.map(fun)
    }

    // TODO: replace with a type component
    def getFieldTypeConvertor(f: String, t: String): String = t match {
        case "INTEGER" | "INT" => f + ".toInt"
        case _ => throw new UnsupportedOperationException
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