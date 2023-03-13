package sqlplus.compile

import sqlplus.expression.VariableOrdering._
import sqlplus.catalog.CatalogManager
import sqlplus.codegen.SparkScalaCodeGenerator
import sqlplus.convert.ConvertResult
import sqlplus.expression.{BinaryOperator, ComputeExpression, Expression, LiteralExpression, Operator, SingleVariableExpression, UnaryOperator, Variable, VariableManager}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, Comparison, Relation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable
import sqlplus.types.DataType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SqlPlusCompiler(val variableManager: VariableManager) {
    def compile(catalogManager: CatalogManager, convertResult: ConvertResult, packageName: String, objectName: String, countFinalResult: Boolean): String = {
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
        // assign an new Variable for expressions like (v2 + v5)
        // this is necessary because when we reduce a relation with comparison (v1 + v2) < (v3 + v4),
        // the other side is not a single variable and thus can not make a snapshot(ComparisonInstance)
        comparisonIdToInfo.values.foreach(c => c.assignVariablesForNonTrivialExpressions(variableManager))

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
        val subsetReduceOrderList = reduceOrderList.filter(info => subsetRelationIds.contains(info.getRelation().getRelationId()))

        val enumerationActions = enumerate(lastRelationInfo, subsetReduceOrderList, finalOutputVariables, countFinalResult)

        val appearingComparisonOperators = comparisonIdToInfo.values.map(info => info.getOperator()).toSet
        val codeGenerator = new SparkScalaCodeGenerator(appearingComparisonOperators, sourceTables, aggregatedRelations,
            auxiliaryRelations, bagRelations,
            relationIdToInfo, reduceActions.toList, enumerationActions, packageName, objectName)
        val builder = new mutable.StringBuilder()
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
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    buffer.append(AppendCommonExtraColumnAction(currentRelationId, columnVariable, joinVariableIndices, joinVariableTypes))
                case ComparisonExtraColumn(columnVariable, joinVariables, comparisonInfo) =>
                    val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonInfo)
                    val (compareVariable, compareVariableIndex, optAction) =
                        createComputationColumnIfNeeded(currentRelationInfo, expression, variable, joinVariableIndices, joinVariableTypes)
                    if (optAction.nonEmpty) {
                        buffer.append(optAction.get)
                        comparisonInfo.update(SingleVariableExpression(compareVariable), isLeft)
                    }
                    buffer.append(AppendComparisonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, joinVariableTypes, compareVariableIndex, comparisonInfo.getOperator().getFuncLiteral(isReverse = isLeft)))
            }
        })

        // apply semi join to reduce dangling tuples
        currentRelationInfo.getSemiJoinTasks().foreach(task => {
            val currentVariables = currentRelationInfo.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val joinVariableIndicesInCurrent = task.joinVariables.map(currentVariableIndicesMap)
            val joinVariableTypesInCurrent = task.joinVariables.map(v => v.dataType)
            // join variables in child should have the same data type
            val joinVariableTypesInChild = joinVariableTypesInCurrent
            buffer.append(ApplySemiJoinAction(currentRelationId, task.childRelationId, joinVariableIndicesInCurrent, joinVariableTypesInCurrent,
                task.joinKeyIndicesInChild, joinVariableTypesInChild))
        })

        // apply self-comparison if any
        val selfComparisons = incidentComparisonsInfo.filter(info => info.getIncidentRelationIds().size == 1)
        selfComparisons.foreach(comparisonInfo => {
            val currentVariables = currentRelationInfo.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val keyIndices = if (joinVariables.nonEmpty) joinVariables.map(currentVariableIndicesMap) else List(0)
            val keyTypes = if (joinVariables.nonEmpty) joinVariables.map(v => v.dataType) else List(currentVariables.head.dataType)

            comparisonInfo.getOperator() match {
                case unaryOperator: UnaryOperator =>
                    // for UnaryOperator, the only operand is on the left
                    val (leftVariable, leftIndex, optLeftAction) = createComputationColumnIfNeeded(currentRelationInfo,
                        comparisonInfo.getLeft(),
                        comparisonInfo.getAssignedVariable(true).getOrElse(comparisonInfo.getLeft().asInstanceOf[SingleVariableExpression].variable),
                        keyIndices, keyTypes)
                    if (optLeftAction.nonEmpty) {
                        buffer.append(optLeftAction.get)
                        comparisonInfo.update(SingleVariableExpression(leftVariable), true)
                    }
                    val functionGenerator: String => String = x => {
                        val leftExpr = leftVariable.dataType.castFromAny(s"$x($leftIndex)")
                        unaryOperator.apply(leftExpr)
                    }
                    buffer.append(ApplySelfComparisonAction(currentRelationId, keyIndices, keyTypes, functionGenerator))
                case binaryOperator: BinaryOperator =>
                    val (leftVariable, leftIndex, optLeftAction) = createComputationColumnIfNeeded(currentRelationInfo,
                        comparisonInfo.getLeft(),
                        comparisonInfo.getAssignedVariable(true).getOrElse(comparisonInfo.getLeft().asInstanceOf[SingleVariableExpression].variable), keyIndices, keyTypes)
                    val (rightVariable, rightIndex, optRightAction) = createComputationColumnIfNeeded(currentRelationInfo,
                        comparisonInfo.getRight(),
                        comparisonInfo.getAssignedVariable(false).getOrElse(comparisonInfo.getRight().asInstanceOf[SingleVariableExpression].variable), keyIndices, keyTypes)
                    if (optLeftAction.nonEmpty) {
                        buffer.append(optLeftAction.get)
                        comparisonInfo.update(SingleVariableExpression(leftVariable), true)
                    }
                    if (optRightAction.nonEmpty) {
                        buffer.append(optRightAction.get)
                        comparisonInfo.update(SingleVariableExpression(rightVariable), false)
                    }
                    val functionGenerator: String => String = arr => {
                        val leftExpr = leftVariable.dataType.castFromAny(s"$arr($leftIndex)")
                        val rightExpr = rightVariable.dataType.castFromAny(s"$arr($rightIndex)")
                        binaryOperator.apply(leftExpr, rightExpr)
                    }
                    buffer.append(ApplySelfComparisonAction(currentRelationId, keyIndices, keyTypes, functionGenerator))
            }
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
                    currentRelationInfo.setEnumerationInfo(EnumerationInfo(joinVariables, List(), 0, List()))
                case 1 =>
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val onlyComparisonInfo = nonSelfComparisons.head
                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, onlyComparisonInfo)
                    val (compareVariable, compareVariableIndex, optAction) =
                        createComputationColumnIfNeeded(currentRelationInfo, expression, variable, joinVariableIndices, joinVariableTypes)
                    if (optAction.nonEmpty) {
                        buffer.append(optAction.get)
                        onlyComparisonInfo.update(SingleVariableExpression(compareVariable), isLeft)
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
                            onlyComparisonInfo.removeIncidentRelationId(currentRelationId)
                            parentRelationInfo.addExtraColumn(CommonExtraColumn(aggregatedColumnVariable, joinVariables))
                            buffer.append(CreateTransparentCommonExtraColumnAction(currentRelationId, aggregatedColumnVariable, joinVariableIndices, joinVariableTypes))
                        } else {
                            // other cases, fall back to general approach
                            // TODO: use general approach
                            throw new UnsupportedOperationException()
                        }
                    } else {
                        // not an AggregatedRelation, use general approach
                        // make a snapshot of comparison before updating
                        val enumerationInfo = EnumerationInfo(joinVariables, List((onlyComparisonInfo.mkSnapshot(), isLeft)), 1, List())
                        val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                        val func = onlyComparisonInfo.getOperator().getFuncLiteral(isReverse = !isLeft)
                        onlyComparisonInfo.update(SingleVariableExpression(columnVariable), isLeft)
                        onlyComparisonInfo.removeIncidentRelationId(currentRelationId)
                        parentRelationInfo.addExtraColumn(CommonExtraColumn(columnVariable, joinVariables))
                        currentRelationInfo.setEnumerationInfo(enumerationInfo)
                        buffer.append(CreateCommonExtraColumnAction(currentRelationId, columnVariable,
                            joinVariableIndices, joinVariableTypes, compareVariableIndex, func))
                    }
                case 2 =>
                    // at most 1 long comparison
                    assert(nonSelfComparisons.count(c => c.getIncidentRelationIds().size > 2) <= 1)
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val nonSelfComparisonsSorted = nonSelfComparisons.sortBy(c => c.getIncidentRelationIds().size)
                    // the shorter one
                    val comparisonInfo1 = nonSelfComparisonsSorted(0)
                    // the longer one(may be another short comparison)
                    val comparisonInfo2 = nonSelfComparisonsSorted(1)

                    val (expression1, variable1, isLeft1) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonInfo1)
                    val (compareVariable1, compareVariableIndex1, optAction1) =
                        createComputationColumnIfNeeded(currentRelationInfo, expression1, variable1, joinVariableIndices, joinVariableTypes)
                    if (optAction1.nonEmpty) {
                        buffer.append(optAction1.get)
                        comparisonInfo1.update(SingleVariableExpression(compareVariable1), isLeft1)
                    }

                    val (expression2, variable2, isLeft2) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonInfo2)
                    val (compareVariable2, compareVariableIndex2, optAction2) =
                        createComputationColumnIfNeeded(currentRelationInfo, expression2, variable2, joinVariableIndices, joinVariableTypes)
                    if (optAction2.nonEmpty) {
                        buffer.append(optAction2.get)
                        comparisonInfo2.update(SingleVariableExpression(compareVariable2), isLeft2)
                    }

                    val enumerationInfo = EnumerationInfo(joinVariables,
                        List((comparisonInfo1.mkSnapshot(), isLeft1), (comparisonInfo2.mkSnapshot(), isLeft2)), 2, List())

                    val columnVariable = variableManager.getNewVariable(compareVariable2.dataType)
                    val func1 = comparisonInfo1.getOperator().getFuncLiteral(isReverse = !isLeft1)
                    val func2 = comparisonInfo2.getOperator().getFuncLiteral(isReverse = !isLeft2)

                    comparisonInfo1.removeIncidentRelationId(currentRelationId)
                    comparisonInfo1.removeIncidentRelationId(parentRelationInfo.getRelationId())

                    // NOTE: update only the longer comparison
                    comparisonInfo2.update(SingleVariableExpression(columnVariable), isLeft2)
                    comparisonInfo2.removeIncidentRelationId(currentRelationId)
                    parentRelationInfo.addExtraColumn(ComparisonExtraColumn(columnVariable, joinVariables, comparisonInfo1))
                    currentRelationInfo.setEnumerationInfo(enumerationInfo)
                    buffer.append(CreateComparisonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, joinVariableTypes, compareVariableIndex1, compareVariableIndex2, func1, func2))
                case _ =>
                    // at most 1 long comparison
                    assert(nonSelfComparisons.count(c => c.getIncidentRelationIds().size > 2) <= 1)
                    // as mentioned in the CQC paper, we reduce by only one comparison and delay the other comparisons to the enumeration
                    val optLongComparison = nonSelfComparisons.find(c => c.getIncidentRelationIds().size > 2)
                    // select the only long comparison, or arbitrarily select one if no long comparison
                    val selectedComparison = optLongComparison.getOrElse(nonSelfComparisons.head)
                    val remainingComparisons = nonSelfComparisons.filterNot(c => c == selectedComparison)

                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, selectedComparison)
                    val (compareVariable, compareVariableIndex, optAction) =
                        createComputationColumnIfNeeded(currentRelationInfo, expression, variable, joinVariableIndices, joinVariableTypes)
                    if (optAction.nonEmpty) {
                        buffer.append(optAction.get)
                        selectedComparison.update(SingleVariableExpression(compareVariable), isLeft)
                    }

                    val extraFilterFunctionBuffer = ListBuffer.empty[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)]
                    for (remainingComparison <- remainingComparisons) {
                        // ivs = intermediateVariables, cvs = currentVariables, during enumeration compilation
                        extraFilterFunctionBuffer.append((ivs, cvs) => {
                            // during enumeration execution, l = intermediateVariables, r = currentVariables
                            (l, r) => {
                                // this is a non-self short comparison. If the LHS of the comparison is contained in currentVariables,
                                // then the RHS should be contained in intermediateResultVariables, vice versa.
                                val isLeftContainedInCurrent = remainingComparison.getLeft().getVariables().subsetOf(currentVariables.toSet)

                                val op = remainingComparison.getOperator().asInstanceOf[BinaryOperator]
                                val left = remainingComparison.getLeft() match {
                                    case compute: ComputeExpression =>
                                        compute.getComputeFunction(if (isLeftContainedInCurrent) cvs else ivs.toList, true)(if (isLeftContainedInCurrent) r else l)
                                    case literal: LiteralExpression =>
                                        literal.getLiteral()
                                }
                                val right = remainingComparison.getRight() match {
                                    case compute: ComputeExpression =>
                                        compute.getComputeFunction(if (!isLeftContainedInCurrent) cvs else ivs.toList, true)(if (!isLeftContainedInCurrent) r else l)
                                    case literal: LiteralExpression =>
                                        literal.getLiteral()
                                }
                                op.apply(left, right)
                            }
                        })
                        remainingComparison.removeIncidentRelationId(currentRelationId)
                        remainingComparison.removeIncidentRelationId(parentRelationInfo.getRelationId())
                    }
                    val enumerationInfo = EnumerationInfo(joinVariables, List((selectedComparison.mkSnapshot(), isLeft)),
                        nonSelfComparisons.size, extraFilterFunctionBuffer.toList)
                    val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                    val func = selectedComparison.getOperator().getFuncLiteral(isReverse = !isLeft)
                    selectedComparison.update(SingleVariableExpression(columnVariable), isLeft)
                    selectedComparison.removeIncidentRelationId(currentRelationId)
                    parentRelationInfo.addExtraColumn(CommonExtraColumn(columnVariable, joinVariables))
                    currentRelationInfo.setEnumerationInfo(enumerationInfo)
                    buffer.append(CreateCommonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, joinVariableTypes, compareVariableIndex, func))
            }
        }

        buffer.toList
    }

    def getCurrentExpressionInComparison(currentVariables: Set[Variable], comparisonInfo: ComparisonInfo): (Expression, Variable, Boolean) = {
        val leftVariables = comparisonInfo.getLeft().getVariables()
        val rightVariables = comparisonInfo.getRight().getVariables()
        if (leftVariables.subsetOf(currentVariables)) {
            comparisonInfo.getLeft() match {
                case SingleVariableExpression(variable) =>
                    (comparisonInfo.getLeft(), variable, true)
                case _ =>
                    (comparisonInfo.getLeft(), comparisonInfo.getAssignedVariable(true).get, true)
            }
        } else {
            comparisonInfo.getRight() match {
                case SingleVariableExpression(variable) =>
                    (comparisonInfo.getRight(), variable, false)
                case _ =>
                    (comparisonInfo.getRight(), comparisonInfo.getAssignedVariable(false).get, false)
            }
        }
    }

    // TODO: refactor createComputationColumnIfNeeded
    def createComputationColumnIfNeeded(relationInfo: RelationInfo, expression: Expression, variable: Variable,
                                        keyIndices: List[Int], keyTypes: List[DataType]): (Variable, Int, Option[CreateComputationExtraColumnAction]) = {
        val currentVariables = relationInfo.getCurrentVariables()
        expression match {
            case SingleVariableExpression(variable) =>
                (variable, currentVariables.indexOf(variable), None)
            case ce: ComputeExpression =>
                val functionGenerator = ce.getComputeFunction(currentVariables, false)
                val action = CreateComputationExtraColumnAction(relationInfo.getRelationId(), variable, keyIndices, keyTypes, functionGenerator)
                relationInfo.addComputationColumn(variable)
                (variable, currentVariables.size, Some(action))
            case le: LiteralExpression =>
                throw new RuntimeException("can not createComputationColumn for LiteralExpressions.")
        }
    }

    /**
     * Preprocess the enumeration list to determine the variables keep in intermediate result,
     * and keyBy index after enumeration.
     *
     * The process is done in two phases.
     * Phase 1: top-down. Compute the maximum set of variables that a enumeration can have(basically it is
     * the union of the variables in intermediate result and in current relation). This phase terminates when
     * we have all the variables in the final output variables.
     *
     * Phase 2: bottom-up. We fix the output variables of the last enumeration as the final output variables.
     * Then we trace back towards the head of enumeration list. If a variable appears in both the intermediate result
     * and the current relation, we use the one from current relation.
     *
     * After Phase 2 is done, all the relations and intermediate results know which variables
     * they should keep after enumeration. This info can be used in the actual enumeration process to compute the
     * key indices.
     *
     */
    def preprocessEnumerations(lastRelation: RelationInfo, enumerationList: List[RelationInfo],
                               finalOutputVariables: List[Variable]): (List[RelationInfo], Map[Int, Set[Variable]], Map[Int, List[Variable]], Set[Variable], List[Variable]) = {
        // phase 1
        var lastMaximumVariableSet = lastRelation.getCurrentVariables().toSet

        var index = -1
        val actualEnumerateRelationList = ListBuffer.empty[RelationInfo]
        while (!finalOutputVariables.toSet.subsetOf(lastMaximumVariableSet)) {
            index += 1
            if (index < enumerationList.size) {
                val relationInfo = enumerationList(index)
                if (relationInfo.getCurrentVariables().toSet.subsetOf(lastMaximumVariableSet)) {
                    // we already have all the variables of the current relation, skip it
                    // this happens when the current relation is a aggregate sub-query
                } else {
                    lastMaximumVariableSet = lastMaximumVariableSet.union(relationInfo.getCurrentVariables().toSet)
                    actualEnumerateRelationList.append(relationInfo)
                }
            } else {
                throw new RuntimeException("Not all the variables are covered after enumeration terminates")
            }
        }

        // phase 2
        // we use the following data structures to keep track of the variables
        // the set of variables that should appears in the intermediate result after enumerating a relation
        val keepVariables = mutable.HashMap.empty[Int, Set[Variable]]

        // the list of variables that should act as key by fields after enumeration(for further enumeration)
        val keyByVariables = mutable.HashMap.empty[Int, List[Variable]]

        val requireVariables = mutable.HashSet.empty[Variable]
        val requireJoinVariables = ListBuffer.empty[Variable]

        // the last enumeration must keep all the output variables
        for (v <- finalOutputVariables)
            requireVariables.add(v)

        actualEnumerateRelationList.reverse.foreach(relationInfo => {
            // the current relation must keep variables in 'requireVariables' to fulfill the requirements from later enumerations
            keepVariables.put(relationInfo.getRelationId(), requireVariables.toSet)

            // set the key by fields to join variables with the next relation
            // NOTE that we foreach actualEnumerateRelationList in a reverse order
            if (requireJoinVariables.nonEmpty)
                keyByVariables.put(relationInfo.getRelationId(), requireJoinVariables.toList)

            // for each v in the current relation, there is no need for the intermediate result to keep it
            for (v <- relationInfo.getCurrentVariables())
                requireVariables.remove(v)

            for (t <- relationInfo.getEnumerationInfo().incidentComparisonsAndIsInLeftSide) {
                val (comparisonInstance, isLeft) = t
                // if current contains the variable on the left, then the intermediate result after enumerating
                // the previous relation should contains the variable on the right; vise versa
                requireVariables.add(if (isLeft) comparisonInstance.right else comparisonInstance.left)
            }

            for (v <- requireJoinVariables) {
                if (!relationInfo.getCurrentVariables().contains(v)) {
                    // current relation does not have the variable to join with the next relation
                    // then the previous enumerations must keep this join variable
                    // this happens when R x S x T, suppose S is the root, the reduce order is R then T
                    // the intermediate result of (R x S) should be key by JOIN_KEY(S, T). However, this variable
                    // may not appear in R. In this case, R require S to provide this variable for joining with T.
                    requireVariables.add(v)
                }
            }

            requireJoinVariables.clear()
            for (v <- relationInfo.getEnumerationInfo().joinVariables)
                requireJoinVariables.append(v)
        })

        // the last 2 components are requirements for the root relation
        (actualEnumerateRelationList.toList, keepVariables.toMap, keyByVariables.toMap,
            requireVariables.toSet, requireJoinVariables.toList)
    }

    def enumerate(lastRelationInfo: RelationInfo, subsetReduceOrderList: List[RelationInfo],
                  finalOutputVariables: List[Variable], countFinalResult: Boolean): List[EnumerateAction] = {
        // preprocess the enumeration list
        // reverse the subsetReduceOrderList since we preprocess in a top-down and then bottom-up manner
        val (actualEnumerateRelationList, keepVariables, keyByVariables, rootKeepVariables, rootKeyByVariables)
            = preprocessEnumerations(lastRelationInfo, subsetReduceOrderList.reverse, finalOutputVariables)

        val enumerateActions = ListBuffer.empty[EnumerateAction]
        val intermediateResultVariables = ListBuffer.empty[Variable]

        // root relation prepares for enumeration
        val rootRelationVariablesDict = lastRelationInfo.getCurrentVariables().zipWithIndex.toMap
        val rootKeepVariableList = lastRelationInfo.getCurrentVariables().filter(v => rootKeepVariables.contains(v))
        intermediateResultVariables.appendAll(rootKeepVariableList)
        enumerateActions.append(RootPrepareEnumerationAction(
            lastRelationInfo.getRelationId(),
            rootKeyByVariables.map(rootRelationVariablesDict), rootKeyByVariables.map(v => v.dataType), rootKeepVariableList.map(rootRelationVariablesDict)))


        for (index <- actualEnumerateRelationList.indices) {
            val relationInfo = actualEnumerateRelationList(index)
            val currentRelationVariables = relationInfo.getCurrentVariables()
            val enumerationInfo = relationInfo.getEnumerationInfo()
            val joinVariables = enumerationInfo.joinVariables

            val currentRelationVariablesDict = currentRelationVariables.zipWithIndex.toMap
            val intermediateResultVariablesDict = intermediateResultVariables.toList.zipWithIndex.toMap
            val currentKeepVariables = keepVariables(relationInfo.getRelationId())
            val (currentRelationIndices, intermediateResultIndices, newIntermediateVariables) =
                getExtractIndices(currentRelationVariables, intermediateResultVariables.toList, currentKeepVariables)

            val optResultKeyIsInIntermediateResultAndIndicesTypes = keyByVariables.get(relationInfo.getRelationId())
                .map(list => list.map(v => {
                if (currentRelationVariables.contains(v))
                    (false, currentRelationVariablesDict(v), v.dataType)
                else
                    (true, intermediateResultVariablesDict(v), v.dataType)
            }))

            enumerationInfo.incidentComparisonsSize match {
                case 0 =>
                    val action = EnumerateWithoutComparisonAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                case 1 =>
                    val (onlyComparison, isLeft) = enumerationInfo.incidentComparisonsAndIsInLeftSide.head
                    val compareVariableInCurrent = if (isLeft) onlyComparison.left else onlyComparison.right
                    val compareVariableInIntermediate = if (isLeft) onlyComparison.right else onlyComparison.left
                    val compareKeyIndexInCurrent = currentRelationVariablesDict(compareVariableInCurrent)
                    val compareKeyIndexInIntermediate = intermediateResultVariablesDict(compareVariableInIntermediate)
                    val action = EnumerateWithOneComparisonAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        compareKeyIndexInCurrent, compareKeyIndexInIntermediate, onlyComparison.op.getFuncLiteral(isReverse = isLeft),
                        currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                case 2 =>
                    val (comparison1, isLeft1) = enumerationInfo.incidentComparisonsAndIsInLeftSide(0)
                    val (comparison2, isLeft2) = enumerationInfo.incidentComparisonsAndIsInLeftSide(1)
                    val intermediateResultCompareVariable1 = if (isLeft1) comparison1.right else comparison1.left
                    val intermediateResultCompareVariable2 = if (isLeft2) comparison2.right else comparison2.left
                    val intermediateResultCompareVariableIndex1 = intermediateResultVariables.toList.indexOf(intermediateResultCompareVariable1)
                    val intermediateResultCompareVariableIndex2 = intermediateResultVariables.toList.indexOf(intermediateResultCompareVariable2)
                    val action = EnumerateWithTwoComparisonsAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        intermediateResultCompareVariableIndex1, intermediateResultCompareVariableIndex2,
                        currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                case _ =>
                    // we have more than 2 incident comparisons
                    val (headComparison, isLeft) = enumerationInfo.incidentComparisonsAndIsInLeftSide.head
                    val compareVariableInCurrent = if (isLeft) headComparison.left else headComparison.right
                    val compareVariableInIntermediate = if (isLeft) headComparison.right else headComparison.left
                    val compareKeyIndexInCurrent = currentRelationVariablesDict(compareVariableInCurrent)
                    val compareKeyIndexInIntermediate = intermediateResultVariablesDict(compareVariableInIntermediate)

                    val extraFilters = enumerationInfo.extraFilterFunctions.map(f => f(intermediateResultVariables, currentRelationVariables)("l", "r"))

                    val action = EnumerateWithMoreThanTwoComparisonsAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        compareKeyIndexInCurrent, compareKeyIndexInIntermediate, headComparison.op.getFuncLiteral(isReverse = isLeft),
                        extraFilters, currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
            }
        }

        if (countFinalResult) {
            enumerateActions.append(CountResultAction)
        } else {
            val finalVariableFormatters = finalOutputVariables.map(v => v.dataType.format(_))
            enumerateActions.append(FormatResultAction(finalVariableFormatters))
        }

        enumerateActions.toList
    }

    def getExtractIndices(currentRelationVariables: List[Variable], intermediateResultVariables: List[Variable],
                          keepVariables: Set[Variable]): (List[Int], List[Int], List[Variable]) = {
        val extractInCurrent = currentRelationVariables.filter(v => keepVariables.contains(v))
        // extract variables from current relation first
        val remainingKeepVariables = (keepVariables -- extractInCurrent)
        val extractInIntermediate = intermediateResultVariables.filter(v => remainingKeepVariables.contains(v))

        // assert no missing variable
        assert(extractInIntermediate.toSet.union(extractInCurrent.toSet) == keepVariables)

        val currentRelationVariablesDict = currentRelationVariables.zipWithIndex.toMap
        val intermediateResultVariablesDict = intermediateResultVariables.zipWithIndex.toMap

        val extractIndicesInCurrent = extractInCurrent.map(currentRelationVariablesDict)
        val extractIndicesInIntermediate = extractInIntermediate.map(intermediateResultVariablesDict)

        // the 3rd component should be in this order because the variables in intermediate result always goes first in enumerate
        (extractIndicesInCurrent, extractIndicesInIntermediate, (extractInIntermediate ++ extractInCurrent))
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
    private var left: Expression = comparison.left
    private var right: Expression = comparison.right

    val incidentRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty
    comparison.getNodes().flatMap(e => Set(e.getSrc, e.getDst)).foreach(r => incidentRelationIds.add(r.getRelationId()))

    private var optAssignedVariableLeft: Option[Variable] = None
    private var optAssignedVariableRight: Option[Variable] = None

    def getIncidentRelationIds(): Set[Int] = incidentRelationIds.toSet

    def getLeft(): Expression = left

    def getRight(): Expression = right

    def update(updated: Expression, isLeft: Boolean): Unit = {
        if (isLeft)
            this.left = updated
        else
            this.right = updated
    }

    def removeIncidentRelationId(id: Int): Unit = {
        incidentRelationIds.remove(id)
    }

    def getOperator(): Operator = op

    def mkSnapshot(): ComparisonInstance = {
        val leftVariable = left match {
            case SingleVariableExpression(variable) => variable
            case _ => optAssignedVariableLeft.get
        }

        val rightVariable = right match {
            case SingleVariableExpression(variable) => variable
            case _ => optAssignedVariableRight.get
        }

        ComparisonInstance(leftVariable, rightVariable, op)
    }

    def assignVariablesForNonTrivialExpressions(manager: VariableManager): Unit = {
        this.left match {
            case expr: Expression if !expr.isInstanceOf[SingleVariableExpression] =>
                assert(optAssignedVariableLeft.isEmpty)
                optAssignedVariableLeft = Some(manager.getNewVariable(expr.getType()))
            case _ =>
        }

        this.right match {
            case expr: Expression if !expr.isInstanceOf[SingleVariableExpression] =>
                assert(optAssignedVariableRight.isEmpty)
                optAssignedVariableRight = Some(manager.getNewVariable(expr.getType()))
            case _ =>
        }
    }

    def getAssignedVariable(isLeft: Boolean): Option[Variable] =
        if (isLeft) optAssignedVariableLeft else optAssignedVariableRight
}

case class ComparisonInstance(left: Variable, right: Variable, op: Operator)

/**
 * Store the necessary info for enumeration.
 * @param joinVariables the list of variables that this relation joins with its parent
 * @param incidentComparisonsAndIsInLeftSide a list of incident relations and an indicator whether the current relation
 *                                           contains the LHS or RHS of the comparison
 * @param incidentComparisonsSize actual count of incident comparisons. This value is NOT the size of
 *                                incidentComparisonsAndIsInLeftSide when we have more than 2 incident comparisons.
 * @param extraFilterFunctions used only when we have more than 2 incident comparisons
 */
case class EnumerationInfo(joinVariables: List[Variable], incidentComparisonsAndIsInLeftSide: List[(ComparisonInstance, Boolean)],
                           incidentComparisonsSize: Int,
                           extraFilterFunctions: List[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)])

case class SemiJoinTask(childRelationId: Int, joinKeyIndicesInChild: List[Int], joinVariables: List[Variable])