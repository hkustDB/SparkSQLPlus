package sqlplus.compile

import sqlplus.expression.VariableOrdering._
import org.apache.calcite.rel.RelNode
import sqlplus.catalog.CatalogManager
import sqlplus.codegen.SparkScalaCodeGenerator
import sqlplus.convert.{ConvertResult, LogicalPlanConverter}
import sqlplus.expression.{ComparisonOperator, Expression, SingleVariableExpression, Variable, VariableManager}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, Comparison, Relation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SqlPlusCompiler(val variableManager: VariableManager) {
    def compile(catalogManager: CatalogManager, convertResult: ConvertResult, packageName: String, objectName: String): String = {
        val aggregatedRelations: List[AggregatedRelation] = convertResult.relations
            .filter(r => r.isInstanceOf[AggregatedRelation])
            .map(r => r.asInstanceOf[AggregatedRelation])

        val auxiliaryRelations: List[AuxiliaryRelation] = convertResult.relations
            .filter(r => r.isInstanceOf[AuxiliaryRelation])
            .map(r => r.asInstanceOf[AuxiliaryRelation])

        val bagRelations: List[BagRelation] = convertResult.relations
            .filter(r => r.isInstanceOf[BagRelation])
            .map(r => r.asInstanceOf[BagRelation])

        val sourceTableNames = convertResult.relations.filter(r => r.isInstanceOf[TableScanRelation]).map(r => r.getTableName()).toSet
        val tables = sourceTableNames.map(tableName => catalogManager.getSchema.getTable(tableName, false).getTable)
        assert(tables.forall(t => t.isInstanceOf[SqlPlusTable]))
        val sourceTables: Set[SqlPlusTable] = tables.map(t => t.asInstanceOf[SqlPlusTable])

        val finalOutputVariables: List[Variable] = convertResult.outputVariables

        printDebugInfo(sourceTables, convertResult.comparisonHyperGraph.getEdges(), convertResult.relations, finalOutputVariables)

        // relationId to relationInfo dictionary
        val relationIdToInfo: Map[Int, RelationInfo] = convertResult.relations.map(r => (r.getRelationId(), new RelationInfo(r))).toMap

        // relationId to parentRelationId
        val relationIdToParentId: mutable.HashMap[Int, Int] = mutable.HashMap.empty

        val validRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
        convertResult.relations.foreach(r => validRelationIds.add(r.getRelationId()))
        // construct adjacent matrix for join tree(view as a undirected graph)
        val matrix = mutable.HashMap.empty[(Int, Int), Boolean].withDefaultValue(false)
        convertResult.joinTree.getEdges().foreach(edge => {
            if (edge.getSrc != null && edge.getDst != null) {
                matrix((edge.getSrc.getRelationId(), edge.getDst.getRelationId())) = true
                matrix((edge.getDst.getRelationId(), edge.getSrc.getRelationId())) = true
            }
        })

        // comparison info
        val comparisonIdToInfo: Map[Int, ComparisonInfo] = convertResult.comparisonHyperGraph.getEdges()
            .map(c => (c.getComparisonId(), new ComparisonInfo(c))).toMap

        // track the existing relations. relationIds will be removed in reduction.
        val existingRelationIds = mutable.HashSet.empty[Int]
        relationIdToInfo.keySet.foreach(id => existingRelationIds.add(id))

        val reduceActions = ListBuffer.empty[ReduceAction]
        val reduceOrderBuffer = ListBuffer.empty[RelationInfo]
        while (existingRelationIds.size > 1) {
            val connectedRelations = existingRelationIds.map(id => (id, getConnectedRelations(matrix, validRelationIds, id)))
            // only leaf relations(connects with only 1 relation) can be candidate for reduction
            val leafRelationAndParents = connectedRelations
                .filter(t => t._2.size == 1)
                .map(t => (t._1, t._2.head))    // convert from [(id, Set(parent))] to [(id, parent)]

            // check the incident comparisons.
            val candidates = leafRelationAndParents.filter(t => {
                // get incident comparisons
                val comparisons = comparisonIdToInfo.values.filter(info => info.getIncidentRelationIds().contains(t._1))
                // a comparison is long if more than 2 relations are involved
                val longComparisonCount = comparisons.count(info => info.getIncidentRelationIds().size > 2)
                // if the longComparisonCount of a leaf relation < 2, it is a candidate
                // the root of join tree is irreducible
                longComparisonCount < 2 && t._1 != convertResult.joinTree.root.getRelationId()
            })

            // pick the first reducible relation from candidates
            val (reducibleRelationId, parentId) = candidates.head
            relationIdToParentId(reducibleRelationId) = parentId

            val incidentComparisonsInfo = comparisonIdToInfo.values.filter(info => info.getIncidentRelationIds().contains(reducibleRelationId)).toList

            val reducibleRelationInfo = relationIdToInfo(reducibleRelationId)
            val parentRelationInfo = relationIdToInfo(parentId)
            val joinVariables = reducibleRelationInfo.getCurrentVariables().toSet
                .intersect(parentRelationInfo.getCurrentVariables().toSet).toList.sorted    // sort joinVariables by name

            // reduce the reducible relation
            val actions = reduceRelation(reducibleRelationInfo, Some(parentRelationInfo), joinVariables, incidentComparisonsInfo)
            reduceActions.appendAll(actions)
            reduceOrderBuffer.append(reducibleRelationInfo)
            existingRelationIds.remove(reducibleRelationId)
            removeRelation(matrix, validRelationIds, reducibleRelationId)
        }

        // now we have the last relation remains in existingRelationIds
        val lastRelationId = existingRelationIds.head
        val lastRelationInfo = relationIdToInfo(lastRelationId)
        val lastIncidentComparisonsInfo = comparisonIdToInfo.values.filter(info => info.getIncidentRelationIds().contains(lastRelationId)).toList

        val lastReduceActions = reduceRelation(lastRelationInfo, None, List(), lastIncidentComparisonsInfo)
        reduceActions.appendAll(lastReduceActions)
        reduceActions.append(EndOfReductionAction(lastRelationId))

        // start to handle enumeration
        val reduceOrderList = reduceOrderBuffer.toList

        // apply enumeration only on relations in connex subset
        val subsetRelationIds = convertResult.joinTree.subset.map(r => r.getRelationId())
        val enumerationList = reduceOrderList.filter(info => subsetRelationIds.contains(info.getRelation().getRelationId()))

        val (finalVariables, _, enumerationActions) = enumerationList
            .foldRight((lastRelationInfo.getCurrentVariables(), finalOutputVariables, ListBuffer.empty[EnumerateAction]))((relationInfo, tuple) =>
                enumerateRelation(relationInfo, tuple._1, tuple._2, tuple._3))

        // handleLastEnumerateAction(enumerationActions, finalOutputVariables, finalVariables)
        if (enumerationList.nonEmpty) {
            handleLastEnumerateAction(enumerationActions, finalOutputVariables, finalVariables)
        } else {
            handleLastEnumerateAction(enumerationActions, finalOutputVariables, lastRelationInfo.getCurrentVariables())
        }

        val appearingComparisonOperators = comparisonIdToInfo.values.map(info => info.getOperator()).toSet
        val codeGenerator = new SparkScalaCodeGenerator(appearingComparisonOperators, sourceTables, aggregatedRelations,
            auxiliaryRelations, bagRelations,
            relationIdToInfo, reduceActions.toList, enumerationActions.toList, packageName, objectName)
        val builder = new StringBuilder()
        codeGenerator.generate(builder)

        builder.toString()
    }

    def reduceRelation(currentRelationInfo: RelationInfo, optParentRelationInfo: Option[RelationInfo], joinVariables: List[Variable],
                       incidentComparisonsInfo: List[ComparisonInfo]): List[ReduceAction] = {
        val buffer: ListBuffer[ReduceAction] = ListBuffer.empty
        val currentRelationId = currentRelationInfo.getRelationId()

        // append extra columns to current relation
        currentRelationInfo.getExtraColumns().foreach(extraColumn => {
            val currentVariables = currentRelationInfo.getCurrentVariables()
            extraColumn match {
                case CommonExtraColumn(columnVariable, joinVariables) =>
                    val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    buffer.append(AppendCommonExtraColumnAction(currentRelationId, columnVariable, joinVariableIndices))
                case ComparisonExtraColumn(columnVariable, joinVariables, comparisonInfo) =>
                    val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val (expression, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonInfo)
                    val (compareVariable, compareVariableIndex, optAction) = createComputationColumnIfNeeded(currentRelationInfo, expression, joinVariableIndices)
                    if (optAction.nonEmpty) {
                        buffer.append(optAction.get)
                        comparisonInfo.replaceHead(SingleVariableExpression(compareVariable), isLeft)
                    }
                    buffer.append(AppendComparisonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, compareVariableIndex, comparisonInfo.getOperator().getFuncExpression(isReverse = isLeft)))
            }
        })

        // apply semi join to reduce dangling tuples
        currentRelationInfo.getSemiJoinTasks().foreach(task => {
            val currentVariables = currentRelationInfo.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val joinVariableIndicesInCurrent = task.joinVariables.map(currentVariableIndicesMap)
            buffer.append(ApplySemiJoinAction(currentRelationId, task.childRelationId, joinVariableIndicesInCurrent, task.joinKeyIndicesInChild))
        })

        // apply self-comparison if any
        val selfComparisons = incidentComparisonsInfo.filter(info => info.getIncidentRelationIds().size == 1)
        selfComparisons.foreach(comparisonInfo => {
            val currentVariables = currentRelationInfo.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val keyIndices = if (joinVariables.nonEmpty) joinVariables.map(currentVariableIndicesMap) else List(0)
            val (leftVariable, leftIndex, optLeftAction) = createComputationColumnIfNeeded(currentRelationInfo, comparisonInfo.getLeft(), keyIndices)
            val (rightVariable, rightIndex, optRightAction) = createComputationColumnIfNeeded(currentRelationInfo, comparisonInfo.getRight(), keyIndices)
            if (optLeftAction.nonEmpty) {
                buffer.append(optLeftAction.get)
                comparisonInfo.replaceHead(SingleVariableExpression(leftVariable), true)
            }
            if (optRightAction.nonEmpty) {
                buffer.append(optRightAction.get)
                comparisonInfo.replaceHead(SingleVariableExpression(rightVariable), false)
            }
            val functionGenerator: String => String = arr => {
                val leftExpr = s"$arr($leftIndex).asInstanceOf[Int]"
                val rightExpr = s"$arr($rightIndex).asInstanceOf[Int]"
                comparisonInfo.getOperator().applyTo(leftExpr, rightExpr)
            }
            buffer.append(ApplySelfComparisonAction(currentRelationId, keyIndices, functionGenerator))
        })

        // create extra column for parent relation if current relation is not the last relation
        if (optParentRelationInfo.nonEmpty && joinVariables.nonEmpty) {
            val parentRelationInfo = optParentRelationInfo.get
            val currentVariables = currentRelationInfo.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val nonSelfComparisons = incidentComparisonsInfo.filter(info => info.getIncidentRelationIds().size > 1)
            nonSelfComparisons.size match {
                case 0 =>
                    parentRelationInfo.addSemiJoinTask(SemiJoinTask(currentRelationId, joinVariables.map(currentVariableIndicesMap), joinVariables))
                    currentRelationInfo.setEnumerationInfo(EnumerationInfo(joinVariables, List()))
                case 1 =>
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val onlyComparisonInfo = nonSelfComparisons.head
                    val (expression, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, onlyComparisonInfo)
                    val (compareVariable, compareVariableIndex, optAction) = createComputationColumnIfNeeded(currentRelationInfo, expression, joinVariableIndices)
                    if (optAction.nonEmpty) {
                        buffer.append(optAction.get)
                        onlyComparisonInfo.replaceHead(SingleVariableExpression(compareVariable), isLeft)
                    }

                    if (currentRelationInfo.getRelation().isInstanceOf[AggregatedRelation]) {
                        val aggregatedRelation = currentRelationInfo.getRelation().asInstanceOf[AggregatedRelation]
                        // currently, only support AggregatedRelations with the 'GROUP BY' field as the 1st field,
                        // and AGG field as the 2nd field
                        assert(aggregatedRelation.getVariableList().size == 2)
                        val aggregatedColumnVariable: Variable = aggregatedRelation.getVariableList()(1)
                        if (joinVariables.size == 1 && joinVariables.head == aggregatedRelation.getVariableList().head) {
                            // special case such as '(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2' and
                            // 'C2.src = P3.dst' in the WHERE clause.
                            // we don't need to create an new variable since there is only one row for each C2.src
                            val func = onlyComparisonInfo.getOperator().getFuncExpression(isReverse = !isLeft)
                            onlyComparisonInfo.removeIncidentRelationId(currentRelationId)
                            parentRelationInfo.addExtraColumn(CommonExtraColumn(aggregatedColumnVariable, joinVariables))
                            buffer.append(CreateTransparentCommonExtraColumnAction(currentRelationId, aggregatedColumnVariable, joinVariableIndices))
                        } else {
                            // other cases, fall back to general approach
                            // TODO: use general approach
                            throw new UnsupportedOperationException()
                        }
                    } else {
                        // not an AggregatedRelation, use general approach
                        val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                        val func = onlyComparisonInfo.getOperator().getFuncExpression(isReverse = !isLeft)
                        onlyComparisonInfo.update(SingleVariableExpression(columnVariable), isLeft)
                        onlyComparisonInfo.removeIncidentRelationId(currentRelationId)
                        parentRelationInfo.addExtraColumn(CommonExtraColumn(columnVariable, joinVariables))
                        currentRelationInfo.setEnumerationInfo(EnumerationInfo(joinVariables, List((onlyComparisonInfo, isLeft))))
                        buffer.append(CreateCommonExtraColumnAction(currentRelationId, columnVariable, joinVariableIndices, compareVariableIndex, func))
                    }
                case 2 =>
                    // at most 1 long comparison
                    assert(nonSelfComparisons.count(c => c.getIncidentRelationIds().size > 2) <= 1)
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val nonSelfComparisonsSorted = nonSelfComparisons.sortBy(c => c.getIncidentRelationIds().size)
                    // the shorter one
                    val comparisonInfo1 = nonSelfComparisonsSorted(0)
                    // the longer one(may be another short comparison)
                    val comparisonInfo2 = nonSelfComparisonsSorted(1)

                    val (expression1, isLeft1) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonInfo1)
                    val (compareVariable1, compareVariableIndex1, optAction1) = createComputationColumnIfNeeded(currentRelationInfo, expression1, joinVariableIndices)
                    if (optAction1.nonEmpty) {
                        buffer.append(optAction1.get)
                        comparisonInfo1.replaceHead(SingleVariableExpression(compareVariable1), isLeft1)
                    }

                    val (expression2, isLeft2) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonInfo2)
                    val (compareVariable2, compareVariableIndex2, optAction2) = createComputationColumnIfNeeded(currentRelationInfo, expression2, joinVariableIndices)
                    if (optAction2.nonEmpty) {
                        buffer.append(optAction2.get)
                        comparisonInfo2.replaceHead(SingleVariableExpression(compareVariable2), isLeft2)
                    }

                    val columnVariable = variableManager.getNewVariable(compareVariable2.dataType)
                    val func1 = comparisonInfo1.getOperator().getFuncExpression(isReverse = !isLeft1)
                    val func2 = comparisonInfo2.getOperator().getFuncExpression(isReverse = !isLeft2)

                    comparisonInfo1.removeIncidentRelationId(currentRelationId)
                    comparisonInfo1.removeIncidentRelationId(parentRelationInfo.getRelationId())

                    // NOTE: update only the longer comparison
                    comparisonInfo2.update(SingleVariableExpression(columnVariable), isLeft2)
                    comparisonInfo2.removeIncidentRelationId(currentRelationId)
                    parentRelationInfo.addExtraColumn(ComparisonExtraColumn(columnVariable, joinVariables, comparisonInfo1))
                    currentRelationInfo.setEnumerationInfo(EnumerationInfo(joinVariables, List((comparisonInfo1, isLeft1), (comparisonInfo2, isLeft2))))
                    buffer.append(CreateComparisonExtraColumnAction(currentRelationId, columnVariable, joinVariableIndices, compareVariableIndex1, compareVariableIndex2, func1, func2))
                case _ =>
                    throw new UnsupportedOperationException
            }
        }

        buffer.toList
    }

    def getCurrentExpressionInComparison(currentVariables: Set[Variable], comparisonInfo: ComparisonInfo): (Expression, Boolean) = {
        if (comparisonInfo.getLeftVariables().exists(v => currentVariables.contains(v))) {
            assert(comparisonInfo.getRightVariables().forall(v => !currentVariables.contains(v)))
            (comparisonInfo.getLeft(), true)
        } else {
            assert(comparisonInfo.getRightVariables().exists(v => currentVariables.contains(v)))
            (comparisonInfo.getRight(), false)
        }
    }

    def createComputationColumnIfNeeded(relationInfo: RelationInfo, expression: Expression, keyIndices: List[Int]): (Variable, Int, Option[CreateComputationExtraColumnAction]) = {
        val currentVariables = relationInfo.getCurrentVariables()
        expression match {
            case SingleVariableExpression(variable) =>
                (variable, currentVariables.indexOf(variable), None)
            case _ =>
                val newColumnVariable = variableManager.getNewVariable(expression.getType())
                val functionGenerator = expression.createFunctionGenerator(currentVariables)
                val action = CreateComputationExtraColumnAction(relationInfo.getRelationId(), newColumnVariable, keyIndices, functionGenerator)
                relationInfo.addComputationColumn(newColumnVariable)
                (newColumnVariable, currentVariables.size, Some(action))
        }
    }

    def enumerateRelation(relationInfo: RelationInfo, intermediateResultVariables: List[Variable],
                          finalOutputVariables: List[Variable], buffer: ListBuffer[EnumerateAction]): (List[Variable], List[Variable], ListBuffer[EnumerateAction]) = {
        if (finalOutputVariables.toSet.subsetOf(intermediateResultVariables.toSet)) {
            // we have all the output variables in the intermediate result, skip the following enumerations
            (intermediateResultVariables, finalOutputVariables, buffer)
        } else {
            val currentRelationVariables = relationInfo.getCurrentVariables()
            val enumerationInfo = relationInfo.getEnumerationInfo()
            val joinVariables = enumerationInfo.joinVariables

            val intermediateResultVariableIndicesMap = intermediateResultVariables.zipWithIndex.toMap
            val currentRelationVariableVariableIndicesMap = currentRelationVariables.zipWithIndex.toMap
            val joinVariableIndicesInIntermediateResult = joinVariables.map(intermediateResultVariableIndicesMap)
            val joinVariableIndicesInCurrentRelation = joinVariables.map(currentRelationVariableVariableIndicesMap)
            enumerationInfo.incidentComparisonsAndIsInLeftSide.size match {
                case 0 =>
                    val (intermediateResultIndices, currentRelationIndices, newIntermediateResultVariables) =
                        getExtractIndices(enumerationInfo, intermediateResultVariables, currentRelationVariables)
                    buffer.append(EnumerateWithoutComparisonAction(relationInfo.getRelationId(), joinVariableIndicesInCurrentRelation,
                        joinVariableIndicesInIntermediateResult, currentRelationIndices, intermediateResultIndices))
                    (newIntermediateResultVariables, finalOutputVariables, buffer)
                case 1 =>
                    val (intermediateResultIndices, currentRelationIndices, newIntermediateResultVariables) =
                        getExtractIndices(enumerationInfo, intermediateResultVariables, currentRelationVariables)
                    val (onlyComparisonInfo, isLeft) = enumerationInfo.incidentComparisonsAndIsInLeftSide.head
                    val currentRelationCompareVariable = onlyComparisonInfo.getHead(isLeft).getVariables().head
                    val intermediateResultCompareVariable = onlyComparisonInfo.getHead(!isLeft).getVariables().head
                    val currentRelationCompareVariableIndex = currentRelationVariables.indexOf(currentRelationCompareVariable)
                    val intermediateResultCompareVariableIndex = intermediateResultVariables.indexOf(intermediateResultCompareVariable)
                    val func = onlyComparisonInfo.getOperator().getFuncExpression(isReverse = isLeft)
                    buffer.append(EnumerateWithOneComparisonAction(relationInfo.getRelationId(), joinVariableIndicesInCurrentRelation,
                        joinVariableIndicesInIntermediateResult, currentRelationCompareVariableIndex, intermediateResultCompareVariableIndex,
                        func, currentRelationIndices, intermediateResultIndices))
                    (newIntermediateResultVariables, finalOutputVariables, buffer)
                case 2 =>
                    val (intermediateResultIndices, currentRelationIndices, newIntermediateResultVariables) =
                        getExtractIndices(enumerationInfo, intermediateResultVariables, currentRelationVariables)
                    val (comparisonInfo1, isLeft1) = enumerationInfo.incidentComparisonsAndIsInLeftSide(0)
                    val (comparisonInfo2, isLeft2) = enumerationInfo.incidentComparisonsAndIsInLeftSide(1)
                    assert(comparisonInfo1.getHead(!isLeft1).getVariables().size == 1)
                    val intermediateResultCompareVariable1 = comparisonInfo1.getHead(!isLeft1).getVariables().head
                    assert(comparisonInfo2.getHead(!isLeft2).getVariables().size == 1)
                    val intermediateResultCompareVariable2 = comparisonInfo2.getHead(!isLeft2).getVariables().head
                    val intermediateResultCompareVariableIndex1 = intermediateResultVariables.indexOf(intermediateResultCompareVariable1)
                    val intermediateResultCompareVariableIndex2 = intermediateResultVariables.indexOf(intermediateResultCompareVariable2)
                    buffer.append(EnumerateWithTwoComparisonsAction(relationInfo.getRelationId(), joinVariableIndicesInCurrentRelation,
                        joinVariableIndicesInIntermediateResult, intermediateResultCompareVariableIndex1, intermediateResultCompareVariableIndex2,
                        currentRelationIndices, intermediateResultIndices))
                    (newIntermediateResultVariables, finalOutputVariables, buffer)
                case _ =>
                    throw new UnsupportedOperationException
            }
        }
    }

    /**
     * Get the ExtractIndices for both the current relation and the intermediate result.
     * Consider the example in Figure 3 in the paper. The only comparison is updated to mf1 <= mf2.
     * Suppose we decide to enumerate the left child first. In this enumeration, we can safely drop
     * variable(column) mf1 since we will get variable x1 in the schema of our intermediate result.
     * And the next enumeration will use x1 as the compare varible instead of mf1. So the rule here
     * is that we always drop the head element in the comparison on the same side with the current
     * relation(in this case, the current relation is the left relation and the side is therefore LEFT).
     * By dropping the variable mf1, the comparison is now updated to x1 <= mf2 and waiting for the next
     * enumeration.
     *
     * @param enumerationInfo the EnumerationInfo stored in the current relation
     * @param intermediateResultVariables variable list in the intermediate result
     * @param currentRelationVariables variable list in the current relation
     * @return (intermediateResultExtractIndices, currentRelationExtractIndices, newIntermediateResultVariables)
     */
    def getExtractIndices(enumerationInfo: EnumerationInfo, intermediateResultVariables: List[Variable],
                          currentRelationVariables: List[Variable]): (List[Int], List[Int], List[Variable]) = {
        val incidentComparisonsAndIsInLeftSide = enumerationInfo.incidentComparisonsAndIsInLeftSide
        val optLastComparisonAndIsInLeftSide = incidentComparisonsAndIsInLeftSide.lastOption
        val optDropVariable: Option[Variable] = optLastComparisonAndIsInLeftSide.map(t => {
            val comparisonInfo = t._1
            val isLeft = t._2
            val dropped = comparisonInfo.dropHead(isLeft)
            assert(dropped.isInstanceOf[SingleVariableExpression])
            dropped.asInstanceOf[SingleVariableExpression].variable
        })

        val intermediateResultBlackList: Set[Variable] = optDropVariable.toSet
        val intermediateResultExtracts = intermediateResultVariables.zipWithIndex
            .filter(t => !intermediateResultBlackList.contains(t._1))
        val intermediateResultExtractVariables = intermediateResultExtracts.map(t => t._1)
        val intermediateResultExtractIndices = intermediateResultExtracts.map(t => t._2)

        val currentRelationBlackList: Set[Variable] = intermediateResultBlackList ++ intermediateResultExtractVariables
        val currentRelationExtracts = currentRelationVariables.zipWithIndex
            .filter(t => !currentRelationBlackList.contains(t._1))
        val currentRelationExtractVariables = currentRelationExtracts.map(t => t._1)
        val currentRelationExtractIndices = currentRelationExtracts.map(t => t._2)
        val newIntermediateResultVariables = intermediateResultExtractVariables ++ currentRelationExtractVariables
        (intermediateResultExtractIndices, currentRelationExtractIndices, newIntermediateResultVariables)
    }

    /**
     * Append a FinalOutputAction to the end of buffer that contains EnumerationActions. This action will
     * 1. remove the key of each row(e.g., (1, (1,2,3)) => (1,2,3))
     * 2. drop the variables that are not in output variables
     * 3. reorder the variables to match the order in input query
     * @param buffer
     * @param targetFinalVariables
     * @param currentFinalVariables
     */
    def handleLastEnumerateAction(buffer: ListBuffer[EnumerateAction], targetFinalVariables: List[Variable],
                                  currentFinalVariables: List[Variable]): Unit = {
        val dict = currentFinalVariables.zipWithIndex.toMap
        val targetFinalVariableIndices = targetFinalVariables.map(dict)
        val action = FinalOutputAction(targetFinalVariableIndices)
        buffer.append(action)
    }

    private def printDebugInfo(sourceTables: Set[SqlPlusTable], comparisons: Set[Comparison],
                               relations: List[Relation], outputVariables: List[Variable]): Unit = {
        println("Source Tables:")
        sourceTables.foreach(t => println(t.getTableName))
        println()

        println("Comparisons:")
        comparisons.foreach(println)
        println()

        println("Relations:")
        relations.foreach(println)
        println()

        println("Output Variables:")
        outputVariables.foreach(println)
    }

    private def getConnectedRelations(matrix: mutable.Map[(Int, Int), Boolean], validRelationIds: mutable.HashSet[Int], relationId: Int): Set[Int] = {
        validRelationIds.filter(id => id != relationId && matrix(relationId, id)).toSet
    }

    private def removeRelation(matrix: mutable.Map[(Int, Int), Boolean], validRelationIds: mutable.HashSet[Int], relationId: Int): Unit = {
        for (i <- validRelationIds) {
            matrix.remove((relationId, i))
            matrix.remove((i, relationId))
        }
        validRelationIds.remove(relationId)
    }
}

class RelationInfo(private val relation: Relation) {
    private val extraColumns: ListBuffer[ExtraColumn] = ListBuffer.empty
    private val currentVariables: ListBuffer[Variable] = ListBuffer.empty
    currentVariables.appendAll(relation.getVariableList())
    private var enumerationInfo: EnumerationInfo = null
    private val semiJoinTasks: ListBuffer[SemiJoinTask] = ListBuffer.empty[SemiJoinTask]

    def getRelationId(): Int =
        relation.getRelationId()

    def getRelation(): Relation =
        relation

    def addExtraColumn(extraColumn: ExtraColumn): Unit = {
        extraColumns.append(extraColumn)
        currentVariables.append(extraColumn.getVariable())
    }

    def getExtraColumns(): List[ExtraColumn] =
        extraColumns.toList

    def getCurrentVariables(): List[Variable] =
        currentVariables.toList

    def setEnumerationInfo(info: EnumerationInfo): Unit = {
        assert(enumerationInfo == null)
        enumerationInfo = info
    }

    def getEnumerationInfo(): EnumerationInfo =
        enumerationInfo

    def addSemiJoinTask(task: SemiJoinTask): Unit =
        semiJoinTasks.append(task)

    def getSemiJoinTasks(): List[SemiJoinTask] =
        semiJoinTasks.toList

    def addComputationColumn(columnVariable: Variable): Unit =
        currentVariables.append(columnVariable)
}

class ComparisonInfo(private val comparison: Comparison) {
    private val op = comparison.op
    private var left: List[Expression] = List(comparison.left)
    private var right: List[Expression] = List(comparison.right)

    val incidentRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty
    comparison.getNodes().flatMap(e => Set(e.getSrc, e.getDst)).foreach(r => incidentRelationIds.add(r.getRelationId()))

    def getIncidentRelationIds(): Set[Int] = incidentRelationIds.toSet

    def getHead(isLeft: Boolean): Expression =
        if (isLeft) getLeft() else getRight()

    def getLeft(): Expression = left.head

    def getRight(): Expression = right.head

    def update(updated: Expression, isLeft: Boolean): Unit = {
        if (isLeft)
            left = updated :: left
        else
            right = updated :: right
    }

    def replaceHead(newHead: Expression, isLeft: Boolean): Unit = {
        if (isLeft)
            left = newHead :: left.tail
        else
            right = newHead :: right.tail
    }

    def dropHead(isLeft: Boolean): Expression = {
        if (isLeft) {
            val result = left.head
            left = left.tail
            result
        } else {
            val result = right.head
            right = right.tail
            result
        }
    }

    def removeIncidentRelationId(id: Int): Unit = {
        incidentRelationIds.remove(id)
    }

    def getLeftExpressions(): List[Expression] = left
    def getRightExpressions(): List[Expression] = right

    def getLeftVariables(): List[Variable] = left.flatMap(e => e.getVariables())
    def getRightVariables(): List[Variable] = right.flatMap(e => e.getVariables())

    def getOperator(): ComparisonOperator = op
}

case class EnumerationInfo(joinVariables: List[Variable], incidentComparisonsAndIsInLeftSide: List[(ComparisonInfo, Boolean)])

case class SemiJoinTask(childRelationId: Int, joinKeyIndicesInChild: List[Int], joinVariables: List[Variable])