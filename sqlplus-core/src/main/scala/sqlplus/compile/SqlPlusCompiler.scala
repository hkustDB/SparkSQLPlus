package sqlplus.compile

import sqlplus.expression.VariableOrdering._
import sqlplus.catalog.CatalogManager
import sqlplus.convert.RunResult
import sqlplus.expression.{BinaryOperator, ComputeExpression, Expression, LiteralExpression, Operator, SingleVariableExpression, UnaryOperator, Variable, VariableManager}
import sqlplus.graph.{AggregatedRelation, AuxiliaryRelation, BagRelation, Comparison, Relation, TableScanRelation}
import sqlplus.plan.table.SqlPlusTable
import sqlplus.types.{DataType, IntDataType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SqlPlusCompiler(val variableManager: VariableManager) {
    def compile(catalogManager: CatalogManager, runResult: RunResult, formatResult: Boolean): CompileResult = {
        // TODO: compilation and codegen for aggregation is unsupported yet.
        assert(runResult.aggregations.isEmpty)
        assert(runResult.candidates.size == 1)

        val joinTree = runResult.candidates.head._1
        val comparisonHyperGraph = runResult.candidates.head._2

        val manager = new RelationVariableNamesManager
        val assigner = new VariableNameAssigner
        val relations = joinTree.getEdges().flatMap(e => List(e.getSrc, e.getDst)).toList

        val sourceTableNames = getSourceTableNames(relations)

        val tables = sourceTableNames.map(tableName => catalogManager.getSchema.getTable(tableName, false).getTable).map(t => t.asInstanceOf[SqlPlusTable])
        val setupActions = ListBuffer.empty[SetupAction]
        tables.distinct.foreach(t => {
            val tableVariableName = assigner.newVariableName()
            val columns = t.getTableColumns
            setupActions.append(ReadSourceTableAction(tableVariableName, t.getTableProperties.get("path"),
                columns.indices.toList, columns.map(c => DataType.fromTypeName(c.getType)).toList))
            manager.setSourceRelation(t.getTableName, tableVariableName)
        })

        val materializedAggregatedRelationVariableNames = mutable.HashMap.empty[(String, String, List[Int]), String]
        relations.filter(r => r.isInstanceOf[AggregatedRelation]).map(r => r.asInstanceOf[AggregatedRelation]).foreach(r => {
            val relationId = r.getRelationId()
            val groupIndices = r.group
            val aggregateFunction = r.func
            if (materializedAggregatedRelationVariableNames.contains((r.tableName, r.func, groupIndices))) {
                val variableName = materializedAggregatedRelationVariableNames((r.tableName, r.func, groupIndices))
                manager.setRawRelation(relationId, variableName)
            } else {
                val fromVariableName = manager.getSourceRelation(r.tableName)
                val variableName = assigner.newVariableName()
                setupActions.append(ComputeAggregatedRelationAction(variableName, fromVariableName, groupIndices, aggregateFunction))
                manager.setRawRelation(relationId, variableName)
                materializedAggregatedRelationVariableNames((r.tableName, r.func, groupIndices)) = variableName
            }
        })

        val finalOutputVariables: List[Variable] = runResult.outputVariables
        val relationStates: Map[Int, RelationState] = relations.map(r => (r.getRelationId(), new RelationState(r))).toMap
        val relationIdToParentId: mutable.HashMap[Int, Int] = mutable.HashMap.empty
        val validRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
        relations.foreach(r => validRelationIds.add(r.getRelationId()))
        val matrix = mutable.HashMap.empty[(Int, Int), Boolean].withDefaultValue(false)
        joinTree.getEdges().foreach(edge => {
            if (edge.getSrc != null && edge.getDst != null) {
                matrix((edge.getSrc.getRelationId(), edge.getDst.getRelationId())) = true
            }
        })

        val comparisonStates: Map[Int, ComparisonState] = comparisonHyperGraph.getEdges()
            .map(c => (c.getComparisonId(), new ComparisonState(c, variableManager))).toMap
        comparisonStates.values.map(cs => cs.getOperator()).toSet.foreach((op: Operator) => {
            setupActions.append(FunctionDefinitionAction(op.getFuncDefinition()))
        })

        val existingRelationIds = mutable.HashSet.empty[Int]
        relationStates.keySet.foreach(id => existingRelationIds.add(id))

        // for treeLikeArrays only, variableName -> typeParameters
        val treeLikeArrayTypeParameters = mutable.HashMap.empty[String, String]

        val reduceActions = ListBuffer.empty[ReduceAction]
        val reduceOrder = ListBuffer.empty[RelationState]
        while (existingRelationIds.size > 1) {
            val parentAndChildren = existingRelationIds.map(id => (id, getChildrenRelations(matrix, validRelationIds, id)))
            // only leaf relations(connects with only 1 relation) can be candidate for reduction
            val leafRelationAndParents = parentAndChildren
                .filter(t => t._2.isEmpty)
                .map(t => (t._1, getParentRelation(matrix, validRelationIds, t._1)))

            // check the incident comparisons.
            val candidates = leafRelationAndParents.filter(t => {
                // get incident comparisons
                val comparisons = comparisonStates.values.filter(cs => cs.getIncidentRelationIds().contains(t._1))
                // a comparison is long if more than 2 relations are involved
                val longComparisonCount = comparisons.count(cs => cs.getIncidentRelationIds().size > 2)
                // if the longComparisonCount of a leaf relation < 2, it is a candidate
                // the root of join tree is irreducible
                longComparisonCount < 2 && t._1 != joinTree.root.getRelationId()
            })

            // pick the first reducible relation from candidates
            val (reducibleRelationId, parentId) = candidates.head
            relationIdToParentId(reducibleRelationId) = parentId

            val incidentComparisonStates = comparisonStates.values.filter(cs => cs.getIncidentRelationIds().contains(reducibleRelationId)).toList

            val reducibleRelationState = relationStates(reducibleRelationId)
            val parentRelationState = relationStates(parentId)
            val joinVariables = reducibleRelationState.getCurrentVariables().toSet
                .intersect(parentRelationState.getCurrentVariables().toSet).toList.sorted

            // reduce the reducible relation
            reduceRelation(reducibleRelationState, Some(parentRelationState), joinVariables, incidentComparisonStates, relationStates,
                joinTree.subset, treeLikeArrayTypeParameters, reduceActions, manager, assigner)
            reduceOrder.append(reducibleRelationState)
            existingRelationIds.remove(reducibleRelationId)
            removeRelation(matrix, validRelationIds, reducibleRelationId)
        }

        // now we have the last relation remains in existingRelationIds
        val lastRelationId = existingRelationIds.head
        val lastRelationState = relationStates(lastRelationId)
        val lastIncidentComparisonStates = comparisonStates.values.filter(cs => cs.getIncidentRelationIds().contains(lastRelationId)).toList

        reduceRelation(lastRelationState, None, List(), lastIncidentComparisonStates, relationStates,
            joinTree.subset, treeLikeArrayTypeParameters, reduceActions, manager, assigner)

        // apply enumeration only on relations in connex subset
        val subsetRelationIds = joinTree.subset.map(r => r.getRelationId())
        val subsetReduceOrder = reduceOrder.toList.filter(rs => subsetRelationIds.contains(rs.getRelation().getRelationId()))

        val enumerateActions = ListBuffer.empty[EnumerateAction]
        enumerate(lastRelationState, subsetReduceOrder, finalOutputVariables, formatResult, treeLikeArrayTypeParameters, enumerateActions, manager, assigner)

        CompileResult(setupActions.toList, reduceActions.toList, enumerateActions.toList)
    }

    def materializeRelation(relationState: RelationState,
                            relationStates: Map[Int, RelationState], buffer: ListBuffer[ReduceAction],
                            manager: RelationVariableNamesManager, assigner: VariableNameAssigner): Unit = {
        relationState.getRelation() match {
            case relation: AuxiliaryRelation =>
                val relationId = relation.getRelationId()
                val supportingRelationId = relation.supportingRelation.getRelationId()
                val supportingRelationState = relationStates(supportingRelationId)
                val supportingRelationVariableIndicesMap = supportingRelationState.getCurrentVariables().zipWithIndex.toMap
                val projectVariables = relationState.getRelation().getVariableList()
                val projectIndices = projectVariables.map(supportingRelationVariableIndicesMap)
                val projectTypes = projectVariables.map(v => v.dataType)
                val supportingVariableName = getKeyByRelationVariableName(supportingRelationId, projectIndices, projectTypes, buffer, manager, assigner)
                val variableName = assigner.newVariableName()
                manager.setKeyByRelation(relationId, projectIndices, variableName)
                buffer.append(MaterializeAuxiliaryRelationAction(variableName, supportingVariableName, projectIndices))
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
                    // TODO: remove this type conversion after allowing more types. Currently, only Int.
                    .map(t => manager.getSourceRelation(t) + ".map(a => a.map(i => i.asInstanceOf[Int]))")
                    .mkString("Array(", ",", ")")

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

                val variableName = assigner.newVariableName()
                manager.setRawRelation(relationId, variableName)
                buffer.append(MaterializeBagRelationAction(variableName, tableScanRelationNames, relationCount,
                    variableCount, sourceTableIndexToRelations, redirects, variableIndices))
            case relation: TableScanRelation =>
                // just mark source relation as raw relation. TableScanRelations are already materialized
                manager.setRawRelation(relation.relationId, manager.getSourceRelation(relation.getTableName()))
            case relation: AggregatedRelation =>
                // do nothing, AggregatedRelations are already materialized
            case _ => throw new UnsupportedOperationException()
        }
    }

    def reduceRelation(currentRelationState: RelationState, optParentRelationState: Option[RelationState],
                       joinVariablesWithParent: List[Variable], incidentComparisonStates: List[ComparisonState],
                       relationStates: Map[Int, RelationState], subset: Set[Relation],
                       treeLikeArrayTypeParameters: mutable.HashMap[String, String],
                       buffer: ListBuffer[ReduceAction],
                       manager: RelationVariableNamesManager,
                       assigner: VariableNameAssigner): Unit = {
        val currentRelationId = currentRelationState.getRelationId()

        // materialize Relation if needed
        materializeRelation(currentRelationState, relationStates, buffer, manager, assigner)

        // append extra columns to current relation
        currentRelationState.getExtraColumns().foreach(extraColumn => {
            val currentVariables = currentRelationState.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap

            extraColumn match {
                case CommonExtraColumn(columnVariableName, joinVariables) =>
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val fromVariableName = getKeyByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                    val appendedVariableName = assigner.newVariableName()
                    manager.setKeyByRelation(currentRelationId, joinVariableIndices, appendedVariableName)
                    buffer.append(AppendCommonExtraColumnAction(appendedVariableName, fromVariableName, columnVariableName))
                case ComparisonExtraColumn(columnVariableName, joinVariables, comparisonState) =>
                    val joinVariableIndices = joinVariables.map(currentVariableIndicesMap)
                    val joinVariableTypes = joinVariables.map(v => v.dataType)
                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonState)
                    val (compareVariable, compareVariableIndex) =
                        createComputationColumnIfNeeded(currentRelationState, expression, variable,
                            joinVariableIndices, joinVariableTypes, comparisonState, isLeft, buffer, manager, assigner)
                    val fromVariableName = getKeyByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                    val appendedVariableName = assigner.newVariableName()
                    val typeParameters = s"${treeLikeArrayTypeParameters(columnVariableName)},${compareVariable.dataType.getScalaTypeName}"
                    manager.setKeyByRelation(currentRelationId, joinVariableIndices, appendedVariableName)
                    buffer.append(AppendComparisonExtraColumnAction(appendedVariableName, fromVariableName, columnVariableName,
                        compareVariableIndex, comparisonState.getOperator().getFuncLiteral(isReverse = isLeft), typeParameters))
            }
        })

        // apply semi join to reduce dangling tuples
        currentRelationState.getSemiJoinTasks().foreach(task => {
            val currentVariables = currentRelationState.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val joinVariableIndicesInCurrent = task.joinVariables.map(currentVariableIndicesMap)
            val joinVariableTypesInCurrent = task.joinVariables.map(v => v.dataType)

            val childVariableName = task.childVariableName
            val fromVariableName = getKeyByRelationVariableName(currentRelationId, joinVariableIndicesInCurrent, joinVariableTypesInCurrent, buffer, manager, assigner)
            val semiJoinedVariableName = assigner.newVariableName()
            manager.setKeyByRelation(currentRelationId, joinVariableIndicesInCurrent, semiJoinedVariableName)
            buffer.append(ApplySemiJoinAction(semiJoinedVariableName, fromVariableName, childVariableName))
        })

        // apply self-comparison if any
        val selfComparisons = incidentComparisonStates.filter(cs => cs.getIncidentRelationIds().size == 1)
        selfComparisons.foreach(comparisonState => {
            val currentVariables = currentRelationState.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val keyIndices = if (joinVariablesWithParent.nonEmpty) joinVariablesWithParent.map(currentVariableIndicesMap) else List(0)
            val keyTypes = if (joinVariablesWithParent.nonEmpty) joinVariablesWithParent.map(v => v.dataType) else List(currentVariables.head.dataType)

            val generator = comparisonState.getOperator() match {
                case unaryOperator: UnaryOperator =>
                    // for UnaryOperator, the only operand is on the left
                    val (leftVariable, leftIndex) = createComputationColumnIfNeeded(currentRelationState,
                        comparisonState.getLeft(),
                        comparisonState.getAssignedVariable(true).getOrElse(comparisonState.getLeft().asInstanceOf[SingleVariableExpression].variable),
                        keyIndices, keyTypes, comparisonState, true, buffer, manager, assigner)
                    val functionGenerator: String => String = x => {
                        val leftExpr = leftVariable.dataType.castFromAny(s"$x($leftIndex)")
                        unaryOperator.apply(leftExpr)
                    }
                    functionGenerator
                case binaryOperator: BinaryOperator =>
                    val (leftVariable, leftIndex) = createComputationColumnIfNeeded(currentRelationState,
                        comparisonState.getLeft(),
                        comparisonState.getAssignedVariable(true).getOrElse(comparisonState.getLeft().asInstanceOf[SingleVariableExpression].variable),
                        keyIndices, keyTypes, comparisonState, true, buffer, manager, assigner)
                    val (rightVariable, rightIndex) = createComputationColumnIfNeeded(currentRelationState,
                        comparisonState.getRight(),
                        comparisonState.getAssignedVariable(false).getOrElse(comparisonState.getRight().asInstanceOf[SingleVariableExpression].variable),
                        keyIndices, keyTypes, comparisonState, false, buffer, manager, assigner)
                    val functionGenerator: String => String = arr => {
                        val leftExpr = leftVariable.dataType.castFromAny(s"$arr($leftIndex)")
                        val rightExpr = rightVariable.dataType.castFromAny(s"$arr($rightIndex)")
                        binaryOperator.apply(leftExpr, rightExpr)
                    }
                    functionGenerator
            }

            val fromVariableName = getKeyByRelationVariableName(currentRelationId, keyIndices, keyTypes, buffer, manager, assigner)
            val filteredVariableName = assigner.newVariableName()
            manager.setKeyByRelation(currentRelationId, keyIndices, filteredVariableName)
            buffer.append(ApplySelfComparisonAction(filteredVariableName, fromVariableName, generator))
        })

        // create extra column for parent relation if current relation is not the last relation
        if (optParentRelationState.nonEmpty && joinVariablesWithParent.nonEmpty) {
            val parentRelationState = optParentRelationState.get
            val currentVariables = currentRelationState.getCurrentVariables()
            val currentVariableIndicesMap = currentVariables.zipWithIndex.toMap
            val joinVariableIndices = joinVariablesWithParent.map(currentVariableIndicesMap)
            val joinVariableTypes = joinVariablesWithParent.map(v => v.dataType)
            val nonSelfComparisons = incidentComparisonStates.filter(cs => cs.getIncidentRelationIds().size > 1)

            nonSelfComparisons.size match {
                case 0 =>
                    // we need to keyBy the current relation anyway
                    val childVariableName = getKeyByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                    parentRelationState.addSemiJoinTask(SemiJoinTask(childVariableName, joinVariablesWithParent))

                    if (subset.contains(currentRelationState.getRelation())) {
                        // groupBy is needed only when current relation is in connex-subset
                        // otherwise, no enumeration will be applied on this relation
                        val enumerateVariableName = getGroupByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                        currentRelationState.setEnumerationState(joinVariablesWithParent, List(), List(), enumerateVariableName)
                    }
                case 1 =>
                    val onlyComparisonState = nonSelfComparisons.head
                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, onlyComparisonState)
                    val (compareVariable, compareVariableIndex) =
                        createComputationColumnIfNeeded(currentRelationState, expression, variable, joinVariableIndices, joinVariableTypes,
                            onlyComparisonState, isLeft, buffer, manager, assigner)
                    val comparisonInstances = List(onlyComparisonState.mkSnapshot(isLeft))

                    if (currentRelationState.getRelation().isInstanceOf[AggregatedRelation]) {
                        val aggregatedRelation = currentRelationState.getRelation().asInstanceOf[AggregatedRelation]
                        // currently, only support AggregatedRelations with the 'GROUP BY' field as the 1st field,
                        // and AGG field as the 2nd field
                        assert(aggregatedRelation.getVariableList().size == 2)
                        val aggregatedColumnVariable: Variable = aggregatedRelation.getVariableList()(1)
                        if (joinVariablesWithParent.size == 1 && joinVariablesWithParent.head == aggregatedRelation.getVariableList().head) {
                            // special case such as '(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2' and
                            // 'C2.src = P3.dst' in the WHERE clause.
                            // we don't need to create an new variable since there is only one row for each C2.src
                            onlyComparisonState.removeIncidentRelationId(currentRelationId)
                            val fromVariableName = manager.getRawRelation(aggregatedRelation.relationId)
                            val columnVariableName = assigner.newVariableName()
                            parentRelationState.addExtraColumn(CommonExtraColumn(columnVariableName, joinVariablesWithParent), aggregatedColumnVariable)
                            manager.setGroupByRelation(aggregatedRelation.relationId, List(0), columnVariableName)
                            buffer.append(CreateTransparentCommonExtraColumnAction(columnVariableName, fromVariableName,
                                joinVariableIndices, joinVariableTypes.map(t => (s: String) => t.castFromAny(s))))
                        } else {
                            // other cases, fall back to general approach
                            // TODO: use general approach
                            throw new UnsupportedOperationException()
                        }
                    } else {
                        // not an AggregatedRelation, use general approach
                        val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                        val func = onlyComparisonState.getOperator().getFuncLiteral(isReverse = !isLeft)
                        onlyComparisonState.update(SingleVariableExpression(columnVariable), isLeft)
                        onlyComparisonState.removeIncidentRelationId(currentRelationId)

                        val fromVariableName = getGroupByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                        val sortedVariableName = assigner.newVariableName()
                        val columnVariableName = assigner.newVariableName()

                        parentRelationState.addExtraColumn(CommonExtraColumn(columnVariableName, joinVariablesWithParent), columnVariable)
                        currentRelationState.setEnumerationState(joinVariablesWithParent, comparisonInstances, List(), sortedVariableName)

                        // creating a common extra column requires sorting. The op must be binary operator.
                        assert(onlyComparisonState.getOperator().isInstanceOf[BinaryOperator])
                        val op = onlyComparisonState.getOperator().asInstanceOf[BinaryOperator]
                        val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${columnVariable.dataType.getScalaTypeName},${columnVariable.dataType.getScalaTypeName}"

                        manager.setGroupByRelation(currentRelationId, joinVariableIndices, sortedVariableName)
                        buffer.append(CreateCommonExtraColumnAction(columnVariableName, sortedVariableName, fromVariableName,
                            compareVariableIndex, func, typeParameters))
                    }
                case 2 =>
                    // at most 1 long comparison
                    assert(nonSelfComparisons.count(c => c.getIncidentRelationIds().size > 2) <= 1)
                    val nonSelfComparisonsSorted = nonSelfComparisons.sortBy(c => c.getIncidentRelationIds().size)
                    // the shorter one
                    val comparisonState1 = nonSelfComparisonsSorted(0)
                    // the longer one(may be another short comparison)
                    val comparisonState2 = nonSelfComparisonsSorted(1)

                    val (expression1, variable1, isLeft1) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonState1)
                    val (compareVariable1, compareVariableIndex1) =
                        createComputationColumnIfNeeded(currentRelationState, expression1, variable1, joinVariableIndices, joinVariableTypes,
                            comparisonState1, isLeft1, buffer, manager, assigner)

                    val (expression2, variable2, isLeft2) = getCurrentExpressionInComparison(currentVariables.toSet, comparisonState2)
                    val (compareVariable2, compareVariableIndex2) =
                        createComputationColumnIfNeeded(currentRelationState, expression2, variable2, joinVariableIndices, joinVariableTypes,
                            comparisonState2, isLeft2, buffer, manager, assigner)

                    val comparisonInstances = List(comparisonState1.mkSnapshot(isLeft1), comparisonState2.mkSnapshot(isLeft2))

                    val columnVariable = variableManager.getNewVariable(compareVariable2.dataType)
                    val func1 = comparisonState1.getOperator().getFuncLiteral(isReverse = !isLeft1)
                    val func2 = comparisonState2.getOperator().getFuncLiteral(isReverse = !isLeft2)

                    comparisonState1.removeIncidentRelationId(currentRelationId)
                    comparisonState1.removeIncidentRelationId(parentRelationState.getRelationId())

                    // NOTE: update only the longer comparison
                    comparisonState2.update(SingleVariableExpression(columnVariable), isLeft2)
                    comparisonState2.removeIncidentRelationId(currentRelationId)
                    val fromVariableName = getGroupByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                    val treeLikeArrayVariableName = assigner.newVariableName()
                    val columnVariableName = assigner.newVariableName()
                    parentRelationState.addExtraColumn(ComparisonExtraColumn(columnVariableName, joinVariablesWithParent, comparisonState1), columnVariable)
                    currentRelationState.setEnumerationState(joinVariablesWithParent, comparisonInstances, List(), treeLikeArrayVariableName)

                    assert(comparisonState1.getOperator().isInstanceOf[BinaryOperator])
                    val op1 = comparisonState1.getOperator().asInstanceOf[BinaryOperator]
                    assert(comparisonState2.getOperator().isInstanceOf[BinaryOperator])
                    val op2 = comparisonState2.getOperator().asInstanceOf[BinaryOperator]
                    val typeParameters = s"${op1.leftTypeName},${op2.rightTypeName},${compareVariable1.dataType.getScalaTypeName},${compareVariable2.dataType.getScalaTypeName}"

                    // we need to store the typeParameters for both treeLikeArrayVariableName(used in appendExtraColumn)
                    // and columnVariableName(used in enumerateWithTwoComparisons)
                    treeLikeArrayTypeParameters.put(treeLikeArrayVariableName, typeParameters)
                    treeLikeArrayTypeParameters.put(columnVariableName, typeParameters)

                    manager.setGroupByRelation(currentRelationId, joinVariableIndices, treeLikeArrayVariableName)
                    buffer.append(CreateComparisonExtraColumnAction(columnVariableName, treeLikeArrayVariableName, fromVariableName,
                        compareVariableIndex1, compareVariableIndex2, func1, func2, typeParameters))
                case _ =>
                    // at most 1 long comparison
                    assert(nonSelfComparisons.count(c => c.getIncidentRelationIds().size > 2) <= 1)
                    // as mentioned in the CQC paper, we reduce by only one comparison and delay the other comparisons to the enumeration
                    val optLongComparison = nonSelfComparisons.find(c => c.getIncidentRelationIds().size > 2)
                    // select the only long comparison, or arbitrarily select one if no long comparison
                    val selectedComparisonState = optLongComparison.getOrElse(nonSelfComparisons.head)
                    val remainingComparisonStates = nonSelfComparisons.filterNot(c => c == selectedComparisonState)

                    val (expression, variable, isLeft) = getCurrentExpressionInComparison(currentVariables.toSet, selectedComparisonState)
                    val (compareVariable, compareVariableIndex) =
                        createComputationColumnIfNeeded(currentRelationState, expression, variable, joinVariableIndices, joinVariableTypes,
                            selectedComparisonState, isLeft, buffer, manager, assigner)

                    val comparisonInstances = List(selectedComparisonState.mkSnapshot(isLeft))

                    val extraFilterFunctionBuffer = ListBuffer.empty[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)]
                    for (remainingComparisonState <- remainingComparisonStates) {
                        // ivs = intermediateVariables, cvs = currentVariables, during enumeration compilation
                        extraFilterFunctionBuffer.append((ivs, cvs) => {
                            // during enumeration execution, l = intermediateVariables, r = currentVariables
                            (l, r) => {
                                // this is a non-self short comparison. If the LHS of the comparison is contained in currentVariables,
                                // then the RHS should be contained in intermediateResultVariables, vice versa.
                                val isLeftContainedInCurrent = remainingComparisonState.getLeft().getVariables().subsetOf(currentVariables.toSet)

                                val op = remainingComparisonState.getOperator().asInstanceOf[BinaryOperator]
                                val left = remainingComparisonState.getLeft() match {
                                    case compute: ComputeExpression =>
                                        compute.getComputeFunction(if (isLeftContainedInCurrent) cvs else ivs.toList, true)(if (isLeftContainedInCurrent) r else l)
                                    case literal: LiteralExpression =>
                                        literal.getLiteral()
                                }
                                val right = remainingComparisonState.getRight() match {
                                    case compute: ComputeExpression =>
                                        compute.getComputeFunction(if (!isLeftContainedInCurrent) cvs else ivs.toList, true)(if (!isLeftContainedInCurrent) r else l)
                                    case literal: LiteralExpression =>
                                        literal.getLiteral()
                                }
                                op.apply(left, right)
                            }
                        })
                        remainingComparisonState.removeIncidentRelationId(currentRelationId)
                        remainingComparisonState.removeIncidentRelationId(parentRelationState.getRelationId())
                    }
                    val columnVariable = variableManager.getNewVariable(compareVariable.dataType)
                    val func = selectedComparisonState.getOperator().getFuncLiteral(isReverse = !isLeft)
                    selectedComparisonState.update(SingleVariableExpression(columnVariable), isLeft)
                    selectedComparisonState.removeIncidentRelationId(currentRelationId)
                    val fromVariableName = getGroupByRelationVariableName(currentRelationId, joinVariableIndices, joinVariableTypes, buffer, manager, assigner)
                    val sortedVariableName = assigner.newVariableName()
                    val columnVariableName = assigner.newVariableName()
                    parentRelationState.addExtraColumn(CommonExtraColumn(columnVariableName, joinVariablesWithParent), columnVariable)
                    currentRelationState.setEnumerationState(joinVariablesWithParent, comparisonInstances, extraFilterFunctionBuffer.toList, sortedVariableName)

                    assert(selectedComparisonState.getOperator().isInstanceOf[BinaryOperator])
                    val op = selectedComparisonState.getOperator().asInstanceOf[BinaryOperator]
                    val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${columnVariable.dataType.getScalaTypeName},${columnVariable.dataType.getScalaTypeName}"

                    manager.setGroupByRelation(currentRelationId, joinVariableIndices, sortedVariableName)
                    buffer.append(CreateCommonExtraColumnAction(columnVariableName, sortedVariableName, fromVariableName,
                        compareVariableIndex, func, typeParameters))
            }
        }
    }

    def getCurrentExpressionInComparison(currentVariables: Set[Variable], comparisonState: ComparisonState): (Expression, Variable, Boolean) = {
        val leftVariables = comparisonState.getLeft().getVariables()
        if (leftVariables.subsetOf(currentVariables)) {
            comparisonState.getLeft() match {
                case SingleVariableExpression(variable) =>
                    (comparisonState.getLeft(), variable, true)
                case _ =>
                    (comparisonState.getLeft(), comparisonState.getAssignedVariable(true).get, true)
            }
        } else {
            comparisonState.getRight() match {
                case SingleVariableExpression(variable) =>
                    (comparisonState.getRight(), variable, false)
                case _ =>
                    (comparisonState.getRight(), comparisonState.getAssignedVariable(false).get, false)
            }
        }
    }

    // TODO: refactor createComputationColumnIfNeeded
    def createComputationColumnIfNeeded(relationState: RelationState, expression: Expression, variable: Variable,
                                        keyIndices: List[Int], keyTypes: List[DataType],
                                        comparisonState: ComparisonState, isLeft: Boolean,
                                        buffer: ListBuffer[ReduceAction],
                                        manager: RelationVariableNamesManager, assigner: VariableNameAssigner): (Variable, Int) = {
        val currentVariables = relationState.getCurrentVariables()
        expression match {
            case SingleVariableExpression(variable) =>
                (variable, currentVariables.indexOf(variable))
            case ce: ComputeExpression =>
                val functionGenerator = ce.getComputeFunction(currentVariables, false)
                val fromVariableName = getKeyByRelationVariableName(relationState.getRelationId(), keyIndices, keyTypes, buffer, manager, assigner)
                val resultVariableName = assigner.newVariableName()
                val action = CreateComputationExtraColumnAction(resultVariableName, fromVariableName, functionGenerator)
                relationState.addComputationColumn(variable)
                comparisonState.update(SingleVariableExpression(variable), isLeft)
                manager.updateKeyByRelation(relationState.getRelationId(), resultVariableName)
                buffer.append(action)
                (variable, currentVariables.size)
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
    def preprocessEnumerations(lastRelationState: RelationState, enumerationList: List[RelationState],
                               finalOutputVariables: List[Variable]): (List[RelationState], Map[Int, Set[Variable]], Map[Int, List[Variable]], Set[Variable], List[Variable]) = {
        // phase 1
        var lastMaximumVariableSet = lastRelationState.getCurrentVariables().toSet
        val comparisonVariableSet = mutable.HashMap.empty[Int, Set[Variable]]
        val joinVariableList = mutable.HashMap.empty[Int, List[Variable]]
        val maximumVariableSet = mutable.HashMap.empty[Int, Set[Variable]]

        // for the root, the MaximumVariableSet is its CurrentVariables
        maximumVariableSet.put(lastRelationState.getRelationId(), lastMaximumVariableSet)

        val actualEnumerateRelationList = ListBuffer.empty[RelationState]
        for (relationState <- enumerationList) {
            if (relationState.getCurrentVariables().toSet.subsetOf(lastMaximumVariableSet)) {
                // we already have all the variables of the current relation, skip it
                // this happens when the current relation is a aggregate sub-query
            } else {
                lastMaximumVariableSet = lastMaximumVariableSet.union(relationState.getCurrentVariables().toSet)
                actualEnumerateRelationList.append(relationState)

                val comparisonInstances = relationState.getComparisonInstances()

                val comparisonsInEnumerations = comparisonInstances.map(t => {
                    val op = t.comparisonState.getOperator()
                    val isLeft = t.isLeft
                    // get the variable in the opposite side in comparisons
                    // note that oppositeVariable may be different with the oppositeVariables when reducing this relation
                    // this may happen when the query is non-full
                    ComparisonInEnumeration(op, t.variable, t.comparisonState.getFirstValidVariableInHistory(!isLeft, lastMaximumVariableSet), isLeft)
                })
                relationState.setComparisonInEnumerations(comparisonsInEnumerations)

                val oppositeVariables = comparisonsInEnumerations.map(t => t.variable2)
                comparisonVariableSet.put(relationState.getRelationId(), oppositeVariables.toSet)
                joinVariableList.put(relationState.getRelationId(), relationState.getJoinVariablesWithParent())
                maximumVariableSet.put(relationState.getRelationId(), lastMaximumVariableSet)
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
        for (relationState <- (lastRelationState :: actualEnumerateRelationList.toList).reverse) {
            // we should be able to keep all the variables we need
            assert(requireVariables.subsetOf(maximumVariableSet(relationState.getRelationId())))

            if (previousRelationId < 0) {
                // this is the last step in enumeration. no need to keyBy
                keyByVariables.remove(relationState.getRelationId())
                keepVariables.put(relationState.getRelationId(), requireVariables)
            } else {
                keyByVariables.put(relationState.getRelationId(), joinVariableList(previousRelationId))
                keepVariables.put(relationState.getRelationId(), requireVariables)
            }

            if (relationState.getRelationId() != lastRelationState.getRelationId()) {
                // update requireVariables only when current relation is not the root
                requireVariables = keepVariables(relationState.getRelationId()) ++ keyByVariables.getOrElse(relationState.getRelationId(), Set.empty) ++
                    comparisonVariableSet(relationState.getRelationId()) -- relationState.getCurrentVariables().toSet

                previousRelationId = relationState.getRelationId()
            }
        }

        (actualEnumerateRelationList.toList, keepVariables.toMap, keyByVariables.toMap,
            keepVariables(lastRelationState.getRelationId()), keyByVariables.getOrElse(lastRelationState.getRelationId(), List.empty))
    }

    def enumerate(lastRelationState: RelationState, subsetReduceOrder: List[RelationState],
                  finalOutputVariables: List[Variable], formatResult: Boolean, treeLikeArrayTypeParameters: mutable.HashMap[String, String],
                  buffer: ListBuffer[EnumerateAction], manager: RelationVariableNamesManager, assigner: VariableNameAssigner): Unit = {
        // preprocess the enumeration list
        // reverse the subsetReduceOrderList since we preprocess in a top-down and then bottom-up manner
        val (actualEnumerateRelationList, keepVariables, keyByVariables, rootKeepVariables, rootKeyByVariables)
            = preprocessEnumerations(lastRelationState, subsetReduceOrder.reverse, finalOutputVariables)

        val intermediateResultVariables = ListBuffer.empty[Variable]
        var intermediateResultVariableName: String = manager.getKeyByAnyKeyRelation(lastRelationState.getRelationId()).get

        // root relation prepares for enumeration
        val rootRelationVariablesDict = lastRelationState.getCurrentVariables().zipWithIndex.toMap
        val rootKeepVariableList = lastRelationState.getCurrentVariables().filter(v => rootKeepVariables.contains(v))
        intermediateResultVariables.appendAll(rootKeepVariableList)

        if (rootKeyByVariables.nonEmpty) {
            val fromVariableName = manager.getKeyByAnyKeyRelation(lastRelationState.getRelationId()).get
            val variableName = assigner.newVariableName()
            intermediateResultVariableName = variableName
            buffer.append(RootPrepareEnumerationAction(
                variableName, fromVariableName,
                rootKeyByVariables.map(rootRelationVariablesDict),
                rootKeyByVariables.map(v => v.dataType).map(t => (s: String) => t.castFromAny(s)),
                rootKeepVariableList.map(rootRelationVariablesDict)))
        }

        var previousStepKeyByVariables = rootKeyByVariables
        for (index <- actualEnumerateRelationList.indices) {
            val relationState = actualEnumerateRelationList(index)
            val currentRelationVariables = relationState.getCurrentVariables()

            val currentRelationVariablesDict = currentRelationVariables.zipWithIndex.toMap
            val intermediateResultVariablesDict = intermediateResultVariables.toList.zipWithIndex.toMap
            val currentKeepVariables = keepVariables(relationState.getRelationId())
            val (currentRelationIndices, intermediateResultIndices, newIntermediateVariables) =
                getExtractIndices(currentRelationVariables, intermediateResultVariables.toList, currentKeepVariables)

            val optKeyByVariables = keyByVariables.get(relationState.getRelationId())
            val currentStepKeyByVariables = optKeyByVariables.getOrElse(previousStepKeyByVariables)
            val resultKeySelectors = if (optKeyByVariables.nonEmpty) {
                optKeyByVariables.get.map(v => {
                    if (currentRelationVariables.contains(v)) {
                        (l: String, r: String) => v.dataType.castFromAny(s"${r}(${currentRelationVariablesDict(v)})")
                    } else
                        (l: String, r: String) => v.dataType.castFromAny(s"${l}(${intermediateResultVariablesDict(v)})")
                })
            } else {
                List()
            }

            val incidentComparisonsSize = relationState.getComparisonInstances().size + relationState.getExtraFilterFunctions().size
            incidentComparisonsSize match {
                case 0 =>
                    val currentVariableName = relationState.getEnumerateVariableName()
                    val newVariableName = assigner.newVariableName()
                    val action = EnumerateWithoutComparisonAction(newVariableName, currentVariableName, intermediateResultVariableName,
                        currentRelationIndices, intermediateResultIndices, resultKeySelectors)
                    buffer.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                    intermediateResultVariableName = newVariableName
                case 1 =>
                    val comparisonInEnumeration = relationState.getComparisonInEnumerations().head
                    val compareVariableInCurrent = comparisonInEnumeration.variable1
                    val compareVariableInIntermediate = comparisonInEnumeration.variable2
                    val compareKeyIndexInCurrent = currentRelationVariablesDict(compareVariableInCurrent)
                    val compareKeyIndexInIntermediate = intermediateResultVariablesDict(compareVariableInIntermediate)

                    assert(comparisonInEnumeration.operator.isInstanceOf[BinaryOperator])
                    val op = comparisonInEnumeration.operator.asInstanceOf[BinaryOperator]
                    val resultKeyByVariableTypeParameters =
                        if (currentStepKeyByVariables.size == 1)
                            currentStepKeyByVariables.head.dataType.getScalaTypeName
                        else
                            currentStepKeyByVariables.map(v => v.dataType.getScalaTypeName).mkString("(",",",")")
                    val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${compareVariableInIntermediate.dataType.getScalaTypeName}," +
                        s"${compareVariableInCurrent.dataType.getScalaTypeName},$resultKeyByVariableTypeParameters"

                    val currentVariableName = relationState.getEnumerateVariableName()
                    val newVariableName = assigner.newVariableName()
                    val action = EnumerateWithOneComparisonAction(newVariableName, currentVariableName, intermediateResultVariableName,
                        compareKeyIndexInCurrent, compareKeyIndexInIntermediate, op.getFuncLiteral(isReverse = comparisonInEnumeration.isLeft),
                        currentRelationIndices, intermediateResultIndices, resultKeySelectors, typeParameters)
                    buffer.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                    intermediateResultVariableName = newVariableName
                case 2 =>
                    val comparisonInEnumeration1 = relationState.getComparisonInEnumerations()(0)
                    val comparisonInEnumeration2 = relationState.getComparisonInEnumerations()(1)
                    val intermediateResultCompareVariable1 = comparisonInEnumeration1.variable2
                    val intermediateResultCompareVariable2 = comparisonInEnumeration2.variable2
                    val intermediateResultCompareVariableIndex1 = intermediateResultVariables.toList.indexOf(intermediateResultCompareVariable1)
                    val intermediateResultCompareVariableIndex2 = intermediateResultVariables.toList.indexOf(intermediateResultCompareVariable2)

                    val resultKeyByVariableTypeParameters =
                        if (currentStepKeyByVariables.size == 1)
                            currentStepKeyByVariables.head.dataType.getScalaTypeName
                        else
                            currentStepKeyByVariables.map(v => v.dataType.getScalaTypeName).mkString("(", ",", ")")

                    val currentVariableName = relationState.getEnumerateVariableName()
                    val newVariableName = assigner.newVariableName()
                    val typeParameters = s"${treeLikeArrayTypeParameters(currentVariableName)}," +
                        s"${intermediateResultCompareVariable1.dataType.getScalaTypeName}," +
                        s"${intermediateResultCompareVariable2.dataType.getScalaTypeName},$resultKeyByVariableTypeParameters"

                    val action = EnumerateWithTwoComparisonsAction(newVariableName, currentVariableName, intermediateResultVariableName,
                        intermediateResultCompareVariableIndex1, intermediateResultCompareVariableIndex2,
                        currentRelationIndices, intermediateResultIndices, resultKeySelectors,
                        typeParameters)
                    buffer.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                    intermediateResultVariableName = newVariableName
                case _ =>
                    // we have more than 2 incident comparisons
                    val comparisonInEnumeration = relationState.getComparisonInEnumerations().head
                    val compareVariableInCurrent = comparisonInEnumeration.variable1
                    val compareVariableInIntermediate = comparisonInEnumeration.variable2
                    val compareKeyIndexInCurrent = currentRelationVariablesDict(compareVariableInCurrent)
                    val compareKeyIndexInIntermediate = intermediateResultVariablesDict(compareVariableInIntermediate)

                    val extraFilters = relationState.getExtraFilterFunctions().map(f => f(intermediateResultVariables, currentRelationVariables)("l", "r"))

                    assert(comparisonInEnumeration.operator.isInstanceOf[BinaryOperator])
                    val op = comparisonInEnumeration.operator.asInstanceOf[BinaryOperator]
                    val resultKeyByVariableTypeParameters =
                        if (currentStepKeyByVariables.size == 1)
                            currentStepKeyByVariables.head.dataType.getScalaTypeName
                        else
                            currentStepKeyByVariables.map(v => v.dataType.getScalaTypeName).mkString("(", ",", ")")
                    val typeParameters = s"${op.leftTypeName},${op.rightTypeName},${compareVariableInIntermediate.dataType.getScalaTypeName}," +
                        s"${compareVariableInCurrent.dataType.getScalaTypeName},$resultKeyByVariableTypeParameters"

                    val currentVariableName = relationState.getEnumerateVariableName()
                    val newVariableName = assigner.newVariableName()
                    val action = EnumerateWithMoreThanTwoComparisonsAction(newVariableName, currentVariableName, intermediateResultVariableName,
                        compareKeyIndexInCurrent, compareKeyIndexInIntermediate, op.getFuncLiteral(isReverse = comparisonInEnumeration.isLeft),
                        extraFilters, currentRelationIndices, intermediateResultIndices, resultKeySelectors, typeParameters)
                    buffer.append(action)
                    intermediateResultVariables.clear()
                    intermediateResultVariables.appendAll(newIntermediateVariables)
                    intermediateResultVariableName = newVariableName
            }

            // save the keyByVariables. The last enumerate() need this info as the key type parameter.
            previousStepKeyByVariables = currentStepKeyByVariables
        }

        if (formatResult) {
            val variableName = assigner.newVariableName()
            val finalVariableFormatters = finalOutputVariables.map(v => v.dataType.format(_))
            buffer.append(FormatResultAction(variableName, intermediateResultVariableName, finalVariableFormatters))
        } else {
            buffer.append(CountResultAction(intermediateResultVariableName))
        }
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

    private def getKeyByRelationVariableName(relationId: Int, keyIndices: List[Int], keyByTypes: List[DataType],
                                             buffer: ListBuffer[ReduceAction], manager: RelationVariableNamesManager,
                                             assigner: VariableNameAssigner): String = {
        val opt1 = manager.getKeyByRelation(relationId, keyIndices)
        if (opt1.nonEmpty) {
            opt1.get
        } else {
            val opt2 = manager.getKeyByAnyKeyRelation(relationId)
            if (opt2.nonEmpty) {
                val fromVariableName = opt2.get
                val newVariableName = assigner.newVariableName()
                buffer.append(KeyByAction(newVariableName, fromVariableName, keyIndices,
                    keyByTypes.map(t => (s: String) => t.castFromAny(s)), true))
                manager.setKeyByRelation(relationId, keyIndices, newVariableName)
                newVariableName
            } else {
                val rawVariableName = manager.getRawRelation(relationId)
                val newVariableName = assigner.newVariableName()
                buffer.append(KeyByAction(newVariableName, rawVariableName, keyIndices,
                    keyByTypes.map(t => (s: String) => t.castFromAny(s)), false))
                manager.setKeyByRelation(relationId, keyIndices, newVariableName)
                newVariableName
            }
        }
    }

    private def getGroupByRelationVariableName(relationId: Int, groupIndices: List[Int], groupTypes: List[DataType],
                                               buffer: ListBuffer[ReduceAction], manager: RelationVariableNamesManager,
                                               assigner: VariableNameAssigner): String = {
        val opt = manager.getGroupByRelation(relationId, groupIndices)
        if (opt.nonEmpty) {
            opt.get
        } else {
            val keyByVariableName = getKeyByRelationVariableName(relationId, groupIndices, groupTypes, buffer, manager, assigner)
            val newVariableName = assigner.newVariableName()
            buffer.append(GroupByAction(newVariableName, keyByVariableName))
            manager.setGroupByRelation(relationId, groupIndices, newVariableName)
            newVariableName
        }
    }

    private def getSourceTableNames(relations: List[Relation]): List[String] = {
        relations.flatMap {
            case relation: AggregatedRelation => List(relation.getTableName())
            case relation: AuxiliaryRelation => getSourceTableNames(List(relation.supportingRelation))
            case relation: BagRelation => getSourceTableNames(relation.getInternalRelations)
            case relation: TableScanRelation => List(relation.getTableName())
        }
    }
}

class RelationState(private val relation: Relation) {
    private val extraColumns: ListBuffer[ExtraColumn] = ListBuffer.empty
    private val currentVariables: ListBuffer[Variable] = ListBuffer.empty
    currentVariables.appendAll(relation.getVariableList())
    private val semiJoinTasks: ListBuffer[SemiJoinTask] = ListBuffer.empty[SemiJoinTask]

    private var joinVariablesWithParent: List[Variable] = _
    private var comparisonInstances: List[ComparisonInstance] = _
    private var extraFilterFunctions: List[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)] = _
    private var enumerateVariableName: String = _

    private var comparisonInEnumerations : List[ComparisonInEnumeration] = _

    def getRelationId(): Int =
        relation.getRelationId()

    def getRelation(): Relation =
        relation

    def addExtraColumn(extraColumn: ExtraColumn, extraVariable: Variable): Unit = {
        extraColumns.append(extraColumn)
        currentVariables.append(extraVariable)
    }

    def getExtraColumns(): List[ExtraColumn] =
        extraColumns.toList

    def getCurrentVariables(): List[Variable] =
        currentVariables.toList

    def addSemiJoinTask(task: SemiJoinTask): Unit =
        semiJoinTasks.append(task)

    def getSemiJoinTasks(): List[SemiJoinTask] =
        semiJoinTasks.toList

    def addComputationColumn(columnVariable: Variable): Unit =
        currentVariables.append(columnVariable)

    def setEnumerationState(joinVariablesWithParent: List[Variable] ,
                            comparisonInstances: List[ComparisonInstance],
                            extraFilterFunctions: List[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)],
                            enumerateVariableName: String): Unit = {
        this.joinVariablesWithParent = joinVariablesWithParent
        this.comparisonInstances = comparisonInstances
        this.extraFilterFunctions = extraFilterFunctions
        this.enumerateVariableName = enumerateVariableName
    }

    def getJoinVariablesWithParent(): List[Variable] = joinVariablesWithParent

    def getComparisonInstances(): List[ComparisonInstance] = comparisonInstances

    def getExtraFilterFunctions(): List[(ListBuffer[Variable], List[Variable]) => ((String, String) => String)] = extraFilterFunctions

    def getEnumerateVariableName(): String = enumerateVariableName

    def setComparisonInEnumerations(comparisonInEnumerations: List[ComparisonInEnumeration]): Unit =
        this.comparisonInEnumerations = comparisonInEnumerations

    def getComparisonInEnumerations(): List[ComparisonInEnumeration] = comparisonInEnumerations
}

class ComparisonState(private val comparison: Comparison, private val manager: VariableManager) {
    private val op = comparison.op
    private var left: Expression = comparison.left
    private var right: Expression = comparison.right

    private val leftHistory: ListBuffer[Expression] = ListBuffer(comparison.left)
    private val rightHistory: ListBuffer[Expression] = ListBuffer(comparison.right)

    val incidentRelationIds: mutable.HashSet[Int] = mutable.HashSet.empty
    comparison.getNodes().flatMap(e => Set(e.getSrc, e.getDst)).foreach(r => incidentRelationIds.add(r.getRelationId()))

    private var optAssignedVariableLeft: Option[Variable] = None
    private var optAssignedVariableRight: Option[Variable] = None

    assignVariablesForNonTrivialExpressions(manager)

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
        ComparisonInstance(variable, isLeft, this)
    }

    // assign an new Variable for expressions like (v2 + v5)
    // this is necessary because when we reduce a relation with comparison (v1 + v2) < (v3 + v4)
    private def assignVariablesForNonTrivialExpressions(manager: VariableManager): Unit = {
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

    def getFirstValidVariableInHistory(isLeft: Boolean, validVariables: Set[Variable]): Variable = {
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

case class ComparisonInstance(variable: Variable, isLeft: Boolean, comparisonState: ComparisonState)

case class ComparisonInEnumeration(operator: Operator, variable1: Variable, variable2: Variable, isLeft: Boolean)

case class SemiJoinTask(childVariableName: String, joinVariables: List[Variable])

sealed trait ExtraColumn
case class CommonExtraColumn(columnVariableName: String, joinVariables: List[Variable]) extends ExtraColumn
case class ComparisonExtraColumn(columnVariableName: String, joinVariables: List[Variable], comparisonState: ComparisonState) extends ExtraColumn

case class CompileResult(setupActions: List[SetupAction],
                         reduceActions: List[ReduceAction],
                         enumerateActions: List[EnumerateAction])