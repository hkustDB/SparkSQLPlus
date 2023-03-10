package sqlplus.codegen

import sqlplus.compile.{AppendCommonExtraColumnAction, AppendComparisonExtraColumnAction, ApplySelfComparisonAction, ApplySemiJoinAction, CreateCommonExtraColumnAction, CreateComparisonExtraColumnAction, CreateComputationExtraColumnAction, CreateTransparentCommonExtraColumnAction, EndOfReductionAction, EnumerateAction, EnumerateWithOneComparisonAction, EnumerateWithTwoComparisonsAction, EnumerateWithoutComparisonAction, ReduceAction, RelationInfo, RootPrepareEnumerationAction}
import sqlplus.expression.{ComparisonOperator, Variable}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable
import sqlplus.types.IntDataType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    val convertedSourceTableNameToVariableNameDict = new mutable.HashMap[String, String]()

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

    override def generateBody(builder: mutable.StringBuilder): Unit = {
        generateExecuteMethod(builder)
    }

    private def generateExecuteMethod(builder: mutable.StringBuilder): Unit = {
        indent(builder, 1).append("def main(args: Array[String]): Unit = {").append("\n")

        generateSparkInit(builder)
        newLine(builder)

        generateCompareFunctionDefinitions(builder)
        newLine(builder)

        generateSourceTables(builder)
        newLine(builder)
        generateAggregatedSourceTables(builder)
        newLine(builder)
        generateBagSourceTables(builder)
        newLine(builder)
        generateAuxiliarySourceTables(builder)
        newLine(builder)

        generateReduction(builder)
        newLine(builder)

        generateEnumeration(builder)
        newLine(builder)

        generateSparkClose(builder)
        indent(builder, 1).append("}").append("\n")
    }

    private def generateSparkInit(builder: mutable.StringBuilder): Unit = {
        indent(builder, 2).append("val conf = new SparkConf()").append("\n")
        indent(builder, 2).append("conf.setAppName(\"SparkSQLPlusExample\")").append("\n")
        indent(builder, 2).append("conf.setMaster(\"local\")").append("\n")
        indent(builder, 2).append("val sc = new SparkContext(conf)").append("\n")
        indent(builder, 2).append("val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()").append("\n")
    }

    private def generateSparkClose(builder: mutable.StringBuilder): Unit = {
        indent(builder, 2).append("sparkSession.close()").append("\n")
    }

    private def generateCompareFunctionDefinitions(builder: mutable.StringBuilder): Unit = {
        for (op <- comparisonOperators)
            indent(builder, 2).append(op.getFuncDefinition()).append("\n")
    }

    private def generateSourceTables(builder: mutable.StringBuilder): Unit = {
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
            indent(builder, 2).append("}).persist()").append("\n")

            indent(builder, 2).append(variableName).append(".count()")

            newLine(builder)
        }
    }

    private def generateAggregatedSourceTables(builder: mutable.StringBuilder): Unit = {
        for (relation <- aggregatedRelations) {
            relation.func match {
                case "COUNT" =>
                    val fromVariableName = sourceTableNameToVariableNameDict(relation.tableName)
                    val groupFields = relation.group.map(i => "fields(" + i + ")").mkString("(", ", ", ")")
                    val variableName = variableNameAssigner.getNewVariableName()
                    indent(builder, 2).append("val ").append(variableName).append(" = ").append(fromVariableName)
                        .append(".map(fields => (").append(groupFields).append(", 1))")
                        .append(".reduceByKey(_ + _)")
                        .append(".map(x => Array[Any](" + (0 to relation.group.length).map(i => "x._" + (i + 1)).mkString(", ") + ")).persist()").append("\n")
                    indent(builder, 2).append(variableName).append(".count()").append("\n")
                    aggregatedRelationIdToVariableNameDict(relation.getRelationId()) = variableName
                case _ => throw new UnsupportedOperationException
            }
        }
    }

    private def generateBagSourceTables(builder: mutable.StringBuilder): Unit = {
        for (bagRelation <- bagRelations) {
            // TODO: support not only triangle
            assert(bagRelation.getInternalRelations.size == 3)
            // should not put AuxiliaryRelation in BagRelation
            assert(bagRelation.getInternalRelations.forall(r => !r.isInstanceOf[AuxiliaryRelation]))
            // only support normal tables
            assert(bagRelation.getInternalRelations.forall(r => r.isInstanceOf[TableScanRelation]))
            // currently all columns must be INT type
            assert(bagRelation.getInternalRelations.forall(r => r.getVariableList().forall(v => v.dataType == IntDataType)))

            // currently, the input relations to LFTJ must be Array[Int]
            // can be removed if we support arbitrary types
            for (internalRelation <- bagRelation.getInternalRelations) {
                val tableName = internalRelation.asInstanceOf[TableScanRelation].tableName
                if (convertedSourceTableNameToVariableNameDict.contains(tableName)) {
                    convertedSourceTableNameToVariableNameDict(tableName)
                } else {
                    val sourceVariableName = sourceTableNameToVariableNameDict(tableName)
                    val size = internalRelation.getVariableList().size
                    val createArray = (0 until size).map(i => s"fields($i).asInstanceOf[Int]").mkString("fields => Array(", ",", ")")
                    val convertedVariableName = variableNameAssigner.getNewVariableName()
                    indent(builder, 2).append("val ").append(convertedVariableName).append(" = ").append(sourceVariableName)
                        .append(".map(").append(createArray).append(").cache()").append("\n")
                    newLine(builder)
                    indent(builder, 2).append(convertedVariableName).append(".count()").append("\n")

                    convertedSourceTableNameToVariableNameDict(tableName) = convertedVariableName
                }
            }

            // generate lftj calls
            val involvedVariables = bagRelation.getInternalRelations.flatMap(r => r.getVariableList()).distinct.sortBy(v => v.name)
            val involvedVariableToIndexDict = involvedVariables.map(v => v.name).zipWithIndex.toMap
            val sortedRelations = bagRelation.getInternalRelations.sortBy(r => r.getRelationId())
            val relationIdToIndexDict = sortedRelations.map(r => r.getRelationId()).zipWithIndex.toMap
            val groups = bagRelation.getInternalRelations.groupBy(r => r.asInstanceOf[TableScanRelation].tableName)
                .mapValues(l => l.sortBy(r => relationIdToIndexDict(r.getRelationId())))
            val sourceTableNames = groups.keys.toList

            // argument 1
            val sourceTableVariableNames = sourceTableNames.map(n => convertedSourceTableNameToVariableNameDict(n)).mkString("Array(", ",", ")")

            // argument 2
            val relationCount = bagRelation.getInternalRelations.size

            // argument 3
            val variableCount = involvedVariables.size

            // argument 4
            val sourceTableIndexToRelations = sourceTableNames.indices.map(i => {
                val sourceTableName = sourceTableNames(i)
                val group = groups(sourceTableName)
                val relationIndices = group.map(r => relationIdToIndexDict(r.getRelationId()))
                relationIndices.mkString("Array(", ",", ")")
            }).mkString("Array(", ",", ")")

            // argument 5&6
            val redirectBuffer = ListBuffer.empty[String]
            val variableIndicesBuffer = ListBuffer.empty[String]
            for (relation <- sortedRelations) {
                val redirect = relation.getVariableList().zipWithIndex.map(t => (t._2, involvedVariableToIndexDict(t._1.name)))
                    .map(t => s"(${t._1},${t._2})").mkString("Array(", ",", ")")
                redirectBuffer.append(redirect)

                // build the own view of this relation. e.g., in the view of relation S(C,A), the source table schema is (C,A)
                // however, assuming the total order of variables is A,B,C, the tuples of S should be arrange in order (A,C)
                val view = relation.getVariableList().map(v => v.name).zipWithIndex.toMap
                val variableIndices = relation.getVariableList().map(v => v.name)
                    .sortBy(involvedVariableToIndexDict).map(view).mkString("Array(", ",", ")")
                variableIndicesBuffer.append(variableIndices)
            }
            val redirects = redirectBuffer.mkString("Array(", ",", ")")
            val variableIndices = variableIndicesBuffer.mkString("Array(", ",", ")")

            val bagVariableName = variableNameAssigner.getNewVariableName()
            indent(builder, 2).append("val ").append(bagVariableName).append(" = sc.lftj(")
                .append(sourceTableVariableNames).append(", ")
                .append(relationCount).append(", ")
                .append(variableCount).append(", ").append("\n")
            indent(builder, 4).append(sourceTableIndexToRelations).append(", ").append("\n")
            indent(builder, 4).append(redirects).append(", ").append("\n")
            indent(builder, 4).append(variableIndices).append(").cache()").append("\n")

            bagRelationIdToVariableNameDict(bagRelation.getRelationId()) = bagVariableName
        }
    }

    private def generateAuxiliarySourceTables(builder: mutable.StringBuilder): Unit = {
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
                // TODO: support non-full queries over bag relations
            }
        }
    }

    private def generateReduction(builder: mutable.StringBuilder): Unit = {
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
                case EndOfReductionAction(relationId) =>
                    lastRelationId = relationId
            }
        }
    }

    def generateCreateCommonExtraColumnAction(builder: mutable.StringBuilder, action: CreateCommonExtraColumnAction): Unit = {
        val relationId = action.relationId
        val extraColumnVariable = action.extraColumnVariable
        val joinKeyIndices = action.joinKeyIndices
        val compareKeyIndex = action.compareKeyIndex
        val func = action.func

        val relationGroupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndices)
        val sortedVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(sortedVariableName).append(" = ")
            .append(relationGroupedVariableName).append(".sortValuesWith(").append(compareKeyIndex).append(", ")
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

        // the join key with parent must be the 1st field
        assert(joinKeyIndices.size == 1 && joinKeyIndices.head == 0)

        // we issue CreateTransparentCommonExtraColumnAction only for AggregatedRelation
        assert(aggregatedRelationIdToVariableNameDict.contains(relationId))
        val rawVariableName = aggregatedRelationIdToVariableNameDict(relationId)

        val extraColumnVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(extraColumnVariableName).append(" = ")
            .append(rawVariableName).append(".map(x => (x(0).asInstanceOf[Int], x(1)))").append("\n")
        extraColumnVariableToVariableNameDict(extraColumnVariable) = extraColumnVariableName
    }

    def generateCreateComparisonExtraColumnAction(builder: mutable.StringBuilder, action: CreateComparisonExtraColumnAction): Unit = {
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

    def generateCreateComputationExtraColumnAction(builder: mutable.StringBuilder, action: CreateComputationExtraColumnAction): Unit = {
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

    def generateAppendCommonExtraColumnAction(builder: mutable.StringBuilder, action: AppendCommonExtraColumnAction): Unit = {
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

    def generateAppendComparisonExtraColumnAction(builder: mutable.StringBuilder, action: AppendComparisonExtraColumnAction): Unit = {
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

    def generateApplySelfComparisonAction(builder: mutable.StringBuilder, action: ApplySelfComparisonAction): Unit = {
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

    def generateApplySemiJoinAction(builder: mutable.StringBuilder, action: ApplySemiJoinAction): Unit = {
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

    def getKeyedVariableNameByRelationId(builder: mutable.StringBuilder, relationId: Int, keyIndices: List[Int]): String = {
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

    def getGroupedVariableNameByRelationId(builder: mutable.StringBuilder, relationId: Int, keyIndices: List[Int]): String = {
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

    private def generateEnumeration(builder: mutable.StringBuilder): Unit = {
        // the enumerateActions must be a RootPrepareEnumerationAction followed by 0 or more other EnumerationActions
        assert(enumerateActions.head.isInstanceOf[RootPrepareEnumerationAction])

        val intermediateVariableName = enumerateActions.head match {
            case rootPrepareEnumerationAction: RootPrepareEnumerationAction =>
                generateRootPrepareEnumerationAction(builder, rootPrepareEnumerationAction)
            case _ => throw new RuntimeException("enumerateActions must be a RootPrepareEnumerationAction " +
                "followed by 0 or more other EnumerationActions")
        }

        val finalVariableName: String = enumerateActions.tail.foldLeft(intermediateVariableName)((v, ea) => {
            ea match {
                case enumerateWithoutComparisonAction: EnumerateWithoutComparisonAction =>
                    generateEnumerateWithoutComparisonAction(builder, enumerateWithoutComparisonAction, v)
                case enumerateWithOneComparisonAction: EnumerateWithOneComparisonAction =>
                    generateEnumerateWithOneComparisonAction(builder, enumerateWithOneComparisonAction, v)
                case enumerateWithTwoComparisonsAction: EnumerateWithTwoComparisonsAction =>
                    generateEnumerateWithTwoComparisonsAction(builder, enumerateWithTwoComparisonsAction, v)
                case _ =>
                    throw new RuntimeException("enumerateActions must be a RootPrepareEnumerationAction " +
                        "followed by 0 or more other EnumerationActions")
            }
        })

        def generateFinalCount(variableName: String): Unit = {
            indent(builder, 2).append(variableName).append(".count()").append("\n")
        }

        generateFinalCount(finalVariableName)
    }

    def generateRootPrepareEnumerationAction(builder: mutable.StringBuilder, action: RootPrepareEnumerationAction): String = {
        val relationId = action.relationId
        val joinKeyIndices = action.joinKeyIndices
        val extractIndicesInCurrent = action.extractIndicesInCurrent

        if (joinKeyIndices.nonEmpty) {
            val variableName = getKeyedVariableNameWithAnyKeyByRelationId(relationId)
            val newVariableName = variableNameAssigner.getNewVariableName()
            val keyByTuple = joinKeyIndices.map(i => s"t._2($i).asInstanceOf[Int]").mkString("(", ", ", ")")
            val func = extractIndicesInCurrent.map(i => s"t._2($i)").mkString(s"t => ($keyByTuple, Array(", ", ", "))")
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
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndices = action.optResultKeyIsInIntermediateResultAndIndices
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndices)
            .map(s => s", $s").getOrElse("")

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent)
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
        val compareKeyIndexInCurrent = action.compareKeyIndexInCurrent
        val compareKeyIndexInIntermediateResult = action.compareKeyIndexInIntermediateResult
        val func = action.func
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndices = action.optResultKeyIsInIntermediateResultAndIndices
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndices)
            .map(s => s", $s").getOrElse("")

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(".enumerateWithOneComparison(")
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
        val compareKeyIndexInIntermediateResult1 = action.compareKeyIndexInIntermediateResult1
        val compareKeyIndexInIntermediateResult2 = action.compareKeyIndexInIntermediateResult2
        val extractIndicesInCurrent = action.extractIndicesInCurrent
        val extractIndicesInIntermediateResult = action.extractIndicesInIntermediateResult
        val optResultKeyIsInIntermediateResultAndIndices = action.optResultKeyIsInIntermediateResultAndIndices
        val resultKeySelector = getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndices)
            .map(s => s", $s").getOrElse("")

        val groupedVariableName = getGroupedVariableNameByRelationId(builder, relationId, joinKeyIndicesInCurrent)
        val newVariableName = variableNameAssigner.getNewVariableName()
        indent(builder, 2).append("val ").append(newVariableName).append(" = ")
            .append(intermediateResultVariableName).append(".enumerateWithTwoComparisons(")
            .append(groupedVariableName).append(", ")
            .append(compareKeyIndexInIntermediateResult1).append(", ")
            .append(compareKeyIndexInIntermediateResult2).append(", ")
            .append(extractIndicesInIntermediateResult.mkString("Array(", ",", ")")).append(", ")
            .append(extractIndicesInCurrent.mkString("Array(", ",", ")"))
            .append(resultKeySelector)
            .append(")").append("\n")
        newVariableName
    }

    // TODO: replace with a type component
    def getFieldTypeConvertor(f: String, t: String): String = t match {
        case "INTEGER" | "INT" => f + ".toInt"
        case _ => throw new UnsupportedOperationException
    }

    def getResultKeySelectorInEnumerations(optResultKeyIsInIntermediateResultAndIndices: Option[List[(Boolean, Int)]]): Option[String] = {
        optResultKeyIsInIntermediateResultAndIndices.map(list => {
            val fields = list.map(t => {
                // t._1 indicates whether this index is in the intermediate result
                if (t._1) s"l(${t._2}).asInstanceOf[Int]" else s"r(${t._2}).asInstanceOf[Int]"
            })

            val selector = fields.mkString("(l, r) => (", ", ", ")")
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