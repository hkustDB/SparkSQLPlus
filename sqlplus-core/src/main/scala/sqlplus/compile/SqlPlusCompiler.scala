package sqlplus.compile

import sqlplus.expression.VariableOrdering._
import sqlplus.catalog.CatalogManager
import sqlplus.convert.ConvertResult
import sqlplus.expression.{BinaryOperator, ComputeExpression, Expression, LiteralExpression, Operator, SingleVariableExpression, UnaryOperator, Variable, VariableManager}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, Comparison, Relation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable
import sqlplus.types.{DataType, IntDataType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SqlPlusCompiler(val variableManager: VariableManager) {
    def compile(catalogManager: CatalogManager, convertResult: ConvertResult): CompileResult = {
        val relations = convertResult.joinTree.getEdges().flatMap(e => List(e.getSrc, e.getDst)).toList

        val aggregatedRelations: List[AggregatedRelation] = relations
            .filter(r => r.isInstanceOf[AggregatedRelation])
            .map(r => r.asInstanceOf[AggregatedRelation])

        val bagRelations: List[BagRelation] = relations
            .filter(r => r.isInstanceOf[BagRelation])
            .map(r => r.asInstanceOf[BagRelation])

        val tableScanRelations: List[TableScanRelation] = relations
            .filter(r => r.isInstanceOf[TableScanRelation])
            .map(r => r.asInstanceOf[TableScanRelation])

        val sourceTableNames =
            (tableScanRelations.map(r => r.getTableName())
                ++ aggregatedRelations.map(r => r.getTableName())
                ++ bagRelations.flatMap(r => r.getInternalRelations
                .filter(ir => ir.isInstanceOf[TableScanRelation] || ir.isInstanceOf[AggregatedRelation])).map(r => r.getTableName())).toSet

        val tables = sourceTableNames.map(tableName => catalogManager.getSchema.getTable(tableName, false).getTable)
        assert(tables.forall(t => t.isInstanceOf[SqlPlusTable]))
        val sourceTables: Set[SqlPlusTable] = tables.map(t => t.asInstanceOf[SqlPlusTable])

        val finalOutputVariables: List[Variable] = convertResult.outputVariables

        // relationId to relationInfo dictionary
        val relationIdToInfo: Map[Int, RelationInfo] = relations.map(r => (r.getRelationId(), new RelationInfo(r))).toMap

        // relationId to parentRelationId
        val relationIdToParentId: mutable.HashMap[Int, Int] = mutable.HashMap.empty

        val validRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
        relations.foreach(r => validRelationIds.add(r.getRelationId()))

        val matrix = mutable.HashMap.empty[(Int, Int), Boolean].withDefaultValue(false)
        convertResult.joinTree.getEdges().foreach(edge => {
            if (edge.getSrc != null && edge.getDst != null) {
                matrix((edge.getSrc.getRelationId(), edge.getDst.getRelationId())) = true
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
            val parentAndChildren = existingRelationIds.map(id => (id, getChildrenRelations(matrix, validRelationIds, id)))
            // only leaf relations(connects with only 1 relation) can be candidate for reduction
            val leafRelationAndParents = parentAndChildren
                .filter(t => t._2.isEmpty)
                .map(t => (t._1, getParentRelation(matrix, validRelationIds, t._1)))

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
            val actions = reduceRelation(reducibleRelationInfo, Some(parentRelationInfo), joinVariables, incidentComparisonsInfo, relationIdToInfo)
            reduceActions.appendAll(actions)
            reduceOrderBuffer.append(reducibleRelationInfo)
            existingRelationIds.remove(reducibleRelationId)
            removeRelation(matrix, validRelationIds, reducibleRelationId)
        }

        // now we have the last relation remains in existingRelationIds
        val lastRelationId = existingRelationIds.head
        val lastRelationInfo = relationIdToInfo(lastRelationId)
        val lastIncidentComparisonsInfo = comparisonIdToInfo.values.filter(info => info.getIncidentRelationIds().contains(lastRelationId)).toList

        val lastReduceActions = reduceRelation(lastRelationInfo, None, List(), lastIncidentComparisonsInfo, relationIdToInfo)
        reduceActions.appendAll(lastReduceActions)
        reduceActions.append(EndOfReductionAction(lastRelationId))

        // start to handle enumeration
        val reduceOrderList = reduceOrderBuffer.toList

        // apply enumeration only on relations in connex subset
        val subsetRelationIds = convertResult.joinTree.subset.map(r => r.getRelationId())
        val subsetReduceOrderList = reduceOrderList.filter(info => subsetRelationIds.contains(info.getRelation().getRelationId()))

        val enumerateResult = enumerate(lastRelationInfo, subsetReduceOrderList, finalOutputVariables)

        val comparisonOperators = comparisonIdToInfo.values.map(info => info.getOperator()).toSet

        CompileResult(comparisonOperators, sourceTables, relationIdToInfo, reduceActions.toList, enumerateResult._1, enumerateResult._2)
    }

    def materializeRelationIfNeeded(relationInfo: RelationInfo,
                                    relationIdToInfo: Map[Int, RelationInfo]): Option[ReduceAction] = {
        relationInfo.getRelation() match {
            case relation: AggregatedRelation =>
                val relationId = relation.getRelationId()
                val tableName = relation.tableName
                val groupIndices = relation.group
                val aggregateFunction = relation.func
                Some(MaterializeAggregatedRelationAction(relationId, tableName, groupIndices, aggregateFunction))
            case relation: AuxiliaryRelation =>
                val relationId = relation.getRelationId()
                val supportingRelationId = relation.supportingRelation.getRelationId()
                val supportingRelationInfo = relationIdToInfo(supportingRelationId)
                val supportingRelationVariableIndicesMap = supportingRelationInfo.getCurrentVariables().zipWithIndex.toMap
                val projectVariables = relationInfo.getRelation().getVariableList()
                val projectIndices = projectVariables.map(supportingRelationVariableIndicesMap)
                val projectTypes = projectVariables.map(v => v.dataType)
                Some(MaterializeAuxiliaryRelationAction(relationId, supportingRelationId, projectIndices, projectTypes))
            case relation: BagRelation =>
                val relationId = relation.getRelationId()
                // currently all columns must be INT type
                // currently, the input relations to LFTJ must be Array[Int]
                // can be removed if we support arbitrary types
                assert(relation.getInternalRelations.forall(r => r.getVariableList().forall(v => v.dataType == IntDataType)))

                assert(relation.getInternalRelations.size == 3)

                // generate lftj calls
                val involvedVariables = relation.getInternalRelations.flatMap(r => r.getVariableList()).distinct.sortBy(v => v.name)
                val involvedVariableToIndexDict = involvedVariables.map(v => v.name).zipWithIndex.toMap
                val sortedRelations = relation.getInternalRelations.sortBy(r => r.getRelationId())
                val relationIdToIndexDict = sortedRelations.map(r => r.getRelationId()).zipWithIndex.toMap

                // currently, only TableScanRelations are supported in bag
                assert(sortedRelations.forall(r => r.isInstanceOf[TableScanRelation]))

                val tableScanRelations = sortedRelations
                    .filter(r => r.isInstanceOf[TableScanRelation]).map(r => r.asInstanceOf[TableScanRelation])

                val groups = tableScanRelations.groupBy(r => r.tableName)
                    .mapValues(l => l.sortBy(r => relationIdToIndexDict(r.getRelationId())))
                val sourceTableNames = groups.keys.toList

                // argument 1
                val tableScanRelationNames = sourceTableNames

                // argument 2
                val relationCount = relation.getInternalRelations.size

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

                Some(MaterializeBagRelationAction(relationId, tableScanRelationNames, relationCount,
                    variableCount, sourceTableIndexToRelations, redirects, variableIndices))
            case relation: TableScanRelation =>
                // TableScanRelations are already materialized at the beginning
                None
            case _ => throw new UnsupportedOperationException()
        }
    }

    def reduceRelation(currentRelationInfo: RelationInfo, optParentRelationInfo: Option[RelationInfo], joinVariables: List[Variable],
                       incidentComparisonsInfo: List[ComparisonInfo], relationIdToInfo: Map[Int, RelationInfo]): List[ReduceAction] = {
        val buffer: ListBuffer[ReduceAction] = ListBuffer.empty
        val currentRelationId = currentRelationInfo.getRelationId()

        // if current relation is not a table scan relation, compute(materialize) it before further execution
        // the computation is always valid since the relation to be reduced must have no children now
        // that means we have all the information needed to compute this relation
        val optMaterializationAction = materializeRelationIfNeeded(currentRelationInfo, relationIdToInfo)
        if (optMaterializationAction.nonEmpty)
            buffer.append(optMaterializationAction.get)

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

                    val typeParameter = compareVariable.dataType.getScalaTypeName
                    buffer.append(AppendComparisonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, joinVariableTypes, compareVariableIndex, comparisonInfo.getOperator().getFuncLiteral(isReverse = isLeft), typeParameter))
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
                    currentRelationInfo.setEnumerationInfo(new EnumerationInfo(joinVariables, List(), 0, List()))
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
                        val enumerationInfo = new EnumerationInfo(joinVariables, List((onlyComparisonInfo.mkSnapshot(isLeft), isLeft)), 1, List())
                        val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                        val func = onlyComparisonInfo.getOperator().getFuncLiteral(isReverse = !isLeft)
                        onlyComparisonInfo.update(SingleVariableExpression(columnVariable), isLeft)
                        onlyComparisonInfo.removeIncidentRelationId(currentRelationId)
                        parentRelationInfo.addExtraColumn(CommonExtraColumn(columnVariable, joinVariables))
                        currentRelationInfo.setEnumerationInfo(enumerationInfo)

                        // creating a common extra column requires sorting. The op must be binary operator.
                        assert(onlyComparisonInfo.getOperator().isInstanceOf[BinaryOperator])
                        val op = onlyComparisonInfo.getOperator().asInstanceOf[BinaryOperator]
                        val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${columnVariable.dataType.getScalaTypeName},${columnVariable.dataType.getScalaTypeName}"
                        buffer.append(CreateCommonExtraColumnAction(currentRelationId, columnVariable,
                            joinVariableIndices, joinVariableTypes, compareVariableIndex, func, typeParameters))
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

                    val enumerationInfo = new EnumerationInfo(joinVariables,
                        List((comparisonInfo1.mkSnapshot(isLeft1), isLeft1), (comparisonInfo2.mkSnapshot(isLeft2), isLeft2)), 2, List())

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

                    assert(comparisonInfo1.getOperator().isInstanceOf[BinaryOperator])
                    val op1 = comparisonInfo1.getOperator().asInstanceOf[BinaryOperator]
                    assert(comparisonInfo2.getOperator().isInstanceOf[BinaryOperator])
                    val op2 = comparisonInfo2.getOperator().asInstanceOf[BinaryOperator]
                    val typeParameters = s"${op1.leftTypeName},${op2.rightTypeName},${compareVariable1.dataType.getScalaTypeName},${compareVariable2.dataType.getScalaTypeName}"

                    buffer.append(CreateComparisonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, joinVariableTypes, compareVariableIndex1, compareVariableIndex2, func1, func2, typeParameters))
                case _ =>
                    // at most 1 long comparison
                    assert(nonSelfComparisons.count(c => c.getIncidentRelationIds().size > 2) <= 1)
                    // as mentioned in the CQC paper, we reduce by only one comparison and delay the other comparisons to the enumeration
                    val optLongComparison = nonSelfComparisons.find(c => c.getIncidentRelationIds().size > 2)
                    // select the only long comparison, or arbitrarily select one if no long comparison
                    val selectedComparisonInfo = optLongComparison.getOrElse(nonSelfComparisons.head)
                    val remainingComparisonInfos = nonSelfComparisons.filterNot(c => c == selectedComparisonInfo)

                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, selectedComparisonInfo)
                    val (compareVariable, compareVariableIndex, optAction) =
                        createComputationColumnIfNeeded(currentRelationInfo, expression, variable, joinVariableIndices, joinVariableTypes)
                    if (optAction.nonEmpty) {
                        buffer.append(optAction.get)
                        selectedComparisonInfo.update(SingleVariableExpression(compareVariable), isLeft)
                    }

                    val extraFilterFunctionBuffer = ListBuffer.empty[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)]
                    for (remainingComparisonInfo <- remainingComparisonInfos) {
                        // ivs = intermediateVariables, cvs = currentVariables, during enumeration compilation
                        extraFilterFunctionBuffer.append((ivs, cvs) => {
                            // during enumeration execution, l = intermediateVariables, r = currentVariables
                            (l, r) => {
                                // this is a non-self short comparison. If the LHS of the comparison is contained in currentVariables,
                                // then the RHS should be contained in intermediateResultVariables, vice versa.
                                val isLeftContainedInCurrent = remainingComparisonInfo.getLeft().getVariables().subsetOf(currentVariables.toSet)

                                val op = remainingComparisonInfo.getOperator().asInstanceOf[BinaryOperator]
                                val left = remainingComparisonInfo.getLeft() match {
                                    case compute: ComputeExpression =>
                                        compute.getComputeFunction(if (isLeftContainedInCurrent) cvs else ivs.toList, true)(if (isLeftContainedInCurrent) r else l)
                                    case literal: LiteralExpression =>
                                        literal.getLiteral()
                                }
                                val right = remainingComparisonInfo.getRight() match {
                                    case compute: ComputeExpression =>
                                        compute.getComputeFunction(if (!isLeftContainedInCurrent) cvs else ivs.toList, true)(if (!isLeftContainedInCurrent) r else l)
                                    case literal: LiteralExpression =>
                                        literal.getLiteral()
                                }
                                op.apply(left, right)
                            }
                        })
                        remainingComparisonInfo.removeIncidentRelationId(currentRelationId)
                        remainingComparisonInfo.removeIncidentRelationId(parentRelationInfo.getRelationId())
                    }
                    val enumerationInfo = new EnumerationInfo(joinVariables, List((selectedComparisonInfo.mkSnapshot(isLeft), isLeft)),
                        nonSelfComparisons.size, extraFilterFunctionBuffer.toList)
                    val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                    val func = selectedComparisonInfo.getOperator().getFuncLiteral(isReverse = !isLeft)
                    selectedComparisonInfo.update(SingleVariableExpression(columnVariable), isLeft)
                    selectedComparisonInfo.removeIncidentRelationId(currentRelationId)
                    parentRelationInfo.addExtraColumn(CommonExtraColumn(columnVariable, joinVariables))
                    currentRelationInfo.setEnumerationInfo(enumerationInfo)

                    assert(selectedComparisonInfo.getOperator().isInstanceOf[BinaryOperator])
                    val op = selectedComparisonInfo.getOperator().asInstanceOf[BinaryOperator]
                    val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${columnVariable.dataType.getScalaTypeName},${columnVariable.dataType.getScalaTypeName}"
                    buffer.append(CreateCommonExtraColumnAction(currentRelationId, columnVariable,
                        joinVariableIndices, joinVariableTypes, compareVariableIndex, func, typeParameters))
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
        val comparisonVariableSet = mutable.HashMap.empty[Int, Set[Variable]]
        val joinVariableList = mutable.HashMap.empty[Int, List[Variable]]
        val maximumVariableSet = mutable.HashMap.empty[Int, Set[Variable]]

        // for the root, the MaximumVariableSet is its CurrentVariables
        maximumVariableSet.put(lastRelation.getRelationId(), lastMaximumVariableSet)

        val actualEnumerateRelationList = ListBuffer.empty[RelationInfo]
        for (relationInfo <- enumerationList) {
            if (relationInfo.getCurrentVariables().toSet.subsetOf(lastMaximumVariableSet)) {
                // we already have all the variables of the current relation, skip it
                // this happens when the current relation is a aggregate sub-query
            } else {
                lastMaximumVariableSet = lastMaximumVariableSet.union(relationInfo.getCurrentVariables().toSet)
                actualEnumerateRelationList.append(relationInfo)

                val incidentComparisonsAndIsInLeftSides = relationInfo.getEnumerationInfo().incidentComparisonsAndIsInLeftSide

                val comparisonsInEnumeration = incidentComparisonsAndIsInLeftSides.map(t => {
                    val op = t._1.comparisonInfo.getOperator()
                    val isLeft = t._2
                    // get the variable in the opposite side in comparisons
                    // note that oppositeVariable may be different with the oppositeVariables when reducing this relation
                    // this may happen when the query is non-full
                    (op, t._1.variable, t._1.comparisonInfo.getValidOldestVariable(!t._2, lastMaximumVariableSet), isLeft)
                })
                relationInfo.getEnumerationInfo().setComparisonsInEnumeration(comparisonsInEnumeration)

                val oppositeVariables = comparisonsInEnumeration.map(t => t._3)
                comparisonVariableSet.put(relationInfo.getRelationId(), oppositeVariables.toSet)
                joinVariableList.put(relationInfo.getRelationId(), relationInfo.getEnumerationInfo().joinVariables)
                maximumVariableSet.put(relationInfo.getRelationId(), lastMaximumVariableSet)
            }
        }

        // phase 2
        // the variables required by the descendants
        // all the finalOutputVariables are required after the enumeration
        var requireVariables = finalOutputVariables.toSet

        val keepVariables = mutable.HashMap.empty[Int, Set[Variable]]
        val keyByVariables = mutable.HashMap.empty[Int, List[Variable]]
        var previousRelationId = -1

        // bottom-up
        for (relationInfo <- (lastRelation :: actualEnumerateRelationList.toList).reverse) {
            // we should be able to keep all the variables we need
            assert(requireVariables.subsetOf(maximumVariableSet(relationInfo.getRelationId())))

            if (previousRelationId < 0) {
                // this is the last step in enumeration. no need to keyBy
                keyByVariables.remove(relationInfo.getRelationId())
                keepVariables.put(relationInfo.getRelationId(), requireVariables)
            } else {
                keyByVariables.put(relationInfo.getRelationId(), joinVariableList(previousRelationId))
                keepVariables.put(relationInfo.getRelationId(), requireVariables)
            }

            if (relationInfo.getRelationId() != lastRelation.getRelationId()) {
                // update requireVariables only when current relation is not the root
                requireVariables = keepVariables(relationInfo.getRelationId()) ++ keyByVariables.getOrElse(relationInfo.getRelationId(), Set.empty) ++
                    comparisonVariableSet(relationInfo.getRelationId()) -- relationInfo.getCurrentVariables().toSet

                previousRelationId = relationInfo.getRelationId()
            }
        }

        (actualEnumerateRelationList.toList, keepVariables.toMap, keyByVariables.toMap,
            keepVariables(lastRelation.getRelationId()), keyByVariables.getOrElse(lastRelation.getRelationId(), List.empty))
    }

    def enumerate(lastRelationInfo: RelationInfo, subsetReduceOrderList: List[RelationInfo],
                  finalOutputVariables: List[Variable]): (List[EnumerateAction], FormatResultAction) = {
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

        var previousStepKeyByVariables = rootKeyByVariables
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

            val optKeyByVariables = keyByVariables.get(relationInfo.getRelationId())
            val currentStepKeyByVariables = optKeyByVariables.getOrElse(previousStepKeyByVariables)
            val optResultKeyIsInIntermediateResultAndIndicesTypes = optKeyByVariables
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
                    val (operator, localVariables, oppositeVariables, isLeft) = enumerationInfo.getComparisonsInEnumeration().head
                    val compareVariableInCurrent = localVariables
                    val compareVariableInIntermediate = oppositeVariables
                    val compareKeyIndexInCurrent = currentRelationVariablesDict(compareVariableInCurrent)
                    val compareKeyIndexInIntermediate = intermediateResultVariablesDict(compareVariableInIntermediate)

                    assert(operator.isInstanceOf[BinaryOperator])
                    val op = operator.asInstanceOf[BinaryOperator]
                    val resultKeyByVariableTypeParameters =
                        if (currentStepKeyByVariables.size == 1)
                            currentStepKeyByVariables.head.dataType.getScalaTypeName
                        else
                            currentStepKeyByVariables.map(v => v.dataType.getScalaTypeName).mkString("(",",",")")
                    val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${compareVariableInIntermediate.dataType.getScalaTypeName}," +
                        s"${compareVariableInCurrent.dataType.getScalaTypeName},$resultKeyByVariableTypeParameters"
                    val action = EnumerateWithOneComparisonAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        compareKeyIndexInCurrent, compareKeyIndexInIntermediate, op.getFuncLiteral(isReverse = isLeft),
                        currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes, typeParameters)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                case 2 =>
                    val (operator1, localVariables1, oppositeVariables1, isLeft1) = enumerationInfo.getComparisonsInEnumeration()(0)
                    val (operator2, localVariables2, oppositeVariables2, isLeft2) = enumerationInfo.getComparisonsInEnumeration()(1)
                    val intermediateResultCompareVariable1 = oppositeVariables1
                    val intermediateResultCompareVariable2 = oppositeVariables2
                    val intermediateResultCompareVariableIndex1 = intermediateResultVariables.toList.indexOf(intermediateResultCompareVariable1)
                    val intermediateResultCompareVariableIndex2 = intermediateResultVariables.toList.indexOf(intermediateResultCompareVariable2)

                    val resultKeyByVariableTypeParameters =
                        if (currentStepKeyByVariables.size == 1)
                            currentStepKeyByVariables.head.dataType.getScalaTypeName
                        else
                            currentStepKeyByVariables.map(v => v.dataType.getScalaTypeName).mkString("(", ",", ")")
                    val compareAndResultTypeParameters = s"${intermediateResultCompareVariable1.dataType.getScalaTypeName}," +
                        s"${intermediateResultCompareVariable2.dataType.getScalaTypeName},$resultKeyByVariableTypeParameters"
                    val action = EnumerateWithTwoComparisonsAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        intermediateResultCompareVariableIndex1, intermediateResultCompareVariableIndex2,
                        currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes,
                        compareAndResultTypeParameters)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                case _ =>
                    // we have more than 2 incident comparisons
                    val (operator, localVariables, oppositeVariables, isLeft) = enumerationInfo.getComparisonsInEnumeration().head
                    val compareVariableInCurrent = localVariables
                    val compareVariableInIntermediate = oppositeVariables
                    val compareKeyIndexInCurrent = currentRelationVariablesDict(compareVariableInCurrent)
                    val compareKeyIndexInIntermediate = intermediateResultVariablesDict(compareVariableInIntermediate)

                    val extraFilters = enumerationInfo.extraFilterFunctions.map(f => f(intermediateResultVariables, currentRelationVariables)("l", "r"))

                    assert(operator.isInstanceOf[BinaryOperator])
                    val op = operator.asInstanceOf[BinaryOperator]
                    val resultKeyByVariableTypeParameters =
                        if (currentStepKeyByVariables.size == 1)
                            currentStepKeyByVariables.head.dataType.getScalaTypeName
                        else
                            currentStepKeyByVariables.map(v => v.dataType.getScalaTypeName).mkString("(", ",", ")")
                    val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${compareVariableInIntermediate.dataType.getScalaTypeName}," +
                        s"${compareVariableInCurrent.dataType.getScalaTypeName},$resultKeyByVariableTypeParameters"
                    val action = EnumerateWithMoreThanTwoComparisonsAction(relationInfo.getRelationId(),
                        joinVariables.map(currentRelationVariablesDict), joinVariables.map(v => v.dataType),
                        compareKeyIndexInCurrent, compareKeyIndexInIntermediate, op.getFuncLiteral(isReverse = isLeft),
                        extraFilters, currentRelationIndices, intermediateResultIndices, optResultKeyIsInIntermediateResultAndIndicesTypes, typeParameters)
                    enumerateActions.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
            }

            // save the keyByVariables. The last enumerate() need this info as the key type parameter.
            previousStepKeyByVariables = currentStepKeyByVariables
        }

        val finalVariableFormatters = finalOutputVariables.map(v => v.dataType.format(_))

        (enumerateActions.toList, FormatResultAction(finalVariableFormatters))
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

    private def getChildrenRelations(matrix: mutable.Map[(Int, Int), Boolean], validRelationIds: mutable.HashSet[Int], relationId: Int): Set[Int] = {
        validRelationIds.filter(id => id != relationId && matrix(relationId, id)).toSet
    }

    private def getParentRelation(matrix: mutable.Map[(Int, Int), Boolean], validRelationIds: mutable.HashSet[Int], relationId: Int): Int = {
        val s = validRelationIds.filter(id => id != relationId && matrix(id, relationId)).toSet
        assert(s.size == 1)
        s.head
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

    private val leftHistory: ListBuffer[Expression] = ListBuffer(comparison.left)
    private val rightHistory: ListBuffer[Expression] = ListBuffer(comparison.right)

    val incidentRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty
    comparison.getNodes().flatMap(e => Set(e.getSrc, e.getDst)).foreach(r => incidentRelationIds.add(r.getRelationId()))

    private var optAssignedVariableLeft: Option[Variable] = None
    private var optAssignedVariableRight: Option[Variable] = None

    def getIncidentRelationIds(): Set[Int] = incidentRelationIds.toSet

    def getLeft(): Expression = left

    def getRight(): Expression = right

    def update(updated: Expression, isLeft: Boolean): Unit = {
        if (isLeft) {
            this.left = updated
            this.leftHistory.append(updated)
        } else {
            this.right = updated
            this.rightHistory.append(updated)
        }
    }

    def removeIncidentRelationId(id: Int): Unit = {
        incidentRelationIds.remove(id)
    }

    def getOperator(): Operator = op

    def mkSnapshot(isLeft: Boolean): ComparisonInstance = {
        val variable = if (isLeft) {
            left match {
                case SingleVariableExpression(variable) => variable
                case _ => optAssignedVariableLeft.get
            }
        } else {
            right match {
                case SingleVariableExpression(variable) => variable
                case _ => optAssignedVariableRight.get
            }
        }
        ComparisonInstance(variable, this)
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

    def getValidOldestVariable(isLeft: Boolean, validVariables: Set[Variable]): Variable = {
        if (isLeft) {
            leftHistory.filter(e => e.isInstanceOf[SingleVariableExpression])
                .map(e => e.asInstanceOf[SingleVariableExpression].variable)
                .filter(v => validVariables.contains(v))
                .head
        } else {
            rightHistory.filter(e => e.isInstanceOf[SingleVariableExpression])
                .map(e => e.asInstanceOf[SingleVariableExpression].variable)
                .filter(v => validVariables.contains(v))
                .head
        }
    }
}

case class ComparisonInstance(variable: Variable, comparisonInfo: ComparisonInfo)

/**
 * Store the necessary info for enumeration.
 * @param joinVariables the list of variables that this relation joins with its parent
 * @param incidentComparisonsAndIsInLeftSide a list of incident relations and an indicator whether the current relation
 *                                           contains the LHS or RHS of the comparison
 * @param incidentComparisonsSize actual count of incident comparisons. This value is NOT the size of
 *                                incidentComparisonsAndIsInLeftSide when we have more than 2 incident comparisons.
 * @param extraFilterFunctions used only when we have more than 2 incident comparisons
 */
class EnumerationInfo(val joinVariables: List[Variable], val incidentComparisonsAndIsInLeftSide: List[(ComparisonInstance, Boolean)],
                      val incidentComparisonsSize: Int,
                      val extraFilterFunctions: List[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)]) {

    private var comparisonsInEnumeration: List[(Operator, Variable, Variable, Boolean)] = List.empty

    def setComparisonsInEnumeration(comparisonsInEnumeration: List[(Operator, Variable, Variable, Boolean)]): Unit = {
        this.comparisonsInEnumeration = comparisonsInEnumeration
    }

    // (operator, localVariable, oppositeVariable, isLeft)
    def getComparisonsInEnumeration(): List[(Operator, Variable, Variable, Boolean)] =
        this.comparisonsInEnumeration
}

case class SemiJoinTask(childRelationId: Int, joinKeyIndicesInChild: List[Int], joinVariables: List[Variable])

case class CompileResult(comparisonOperators: Set[Operator], sourceTables: Set[SqlPlusTable],
                         relationIdToInfo: Map[Int, RelationInfo], reduceActions: List[ReduceAction],
                         enumerateActions: List[EnumerateAction], formatResultAction: FormatResultAction)