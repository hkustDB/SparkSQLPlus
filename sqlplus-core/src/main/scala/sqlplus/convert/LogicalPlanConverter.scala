package sqlplus.convert

import com.google.common.collect.BoundType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.util.{NlsString, Sarg}
import sqlplus.expression._
import sqlplus.ghd.GhdAlgorithm
import sqlplus.graph._
import sqlplus.gyo.GyoAlgorithm
import sqlplus.types._
import sqlplus.utils.DisjointSet

import java.util.GregorianCalendar
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Convert logical plan to join trees. Then construct a ComparisonHyperGraph for each join tree.
 * Select the join tree and its ComparisonHyperGraph with minimum degree.
 *
 */
class LogicalPlanConverter(val variableManager: VariableManager) {
    val gyo: GyoAlgorithm = new GyoAlgorithm
    val ghd: GhdAlgorithm = new GhdAlgorithm

    def run(root: RelNode): RunResult = {
        val (relations, conditions, outputVariables, computations, isFull, groupByVariables, aggregations, optTopK) = traverseLogicalPlan(root)
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        val optGyoResult = if (aggregations.isEmpty) {
            // non-aggregation query, try to find a jointree with outputVariables at the top
            // if the query is non-free-connex, terminate and use GHD instead
            gyo.run(relationalHyperGraph, outputVariables.toSet, true)
        } else {
            // aggregation query, try to find a jointree with groupByVariables at the top
            // if the query is non-free-connex but acyclic, find a jointree such that groupByVariables are closed to the root
            // if the query is cyclic, terminate and use GHD instead
            gyo.run(relationalHyperGraph, groupByVariables.toSet, false)
        }

        val joinTreeWithHyperGraphs = if (optGyoResult.nonEmpty) {
            optGyoResult.get.joinTreeWithHyperGraphs
        } else {
            if (aggregations.isEmpty) {
                // non-aggregation query, try to find a ghd with outputVariables at the top
                ghd.run(relationalHyperGraph, outputVariables.toSet).joinTreeWithHyperGraphs
            } else {
                // aggregation query, try to find any valid ghd for full query
                ghd.run(relationalHyperGraph, relations.flatMap(r => r.getVariableList()).toSet).joinTreeWithHyperGraphs
            }
        }

        // construct a ComparisonHyperGraph for each join tree, the ComparisonHyperGraph has the minimum degree
        val joinTreesWithComparisonHyperGraph: List[(JoinTree, ComparisonHyperGraph)] = joinTreeWithHyperGraphs.flatMap(t => {
            val joinTree = t._1
            val hyperGraph = t._2
            val comparisonHyperEdges: List[Comparison] = conditions.map({
                case LessThanCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator("<", leftOperand, rightOperand, false)
                    Comparison(path, op, leftOperand, rightOperand)
                case LessThanOrEqualToCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator("<=", leftOperand, rightOperand, false)
                    Comparison(path, op, leftOperand, rightOperand)
                case GreaterThanCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator(">", leftOperand, rightOperand, false)
                    Comparison(path, op, leftOperand, rightOperand)
                case GreaterThanOrEqualToCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator(">=", leftOperand, rightOperand, false)
                    Comparison(path, op, leftOperand, rightOperand)
                case EqualToLiteralCondition(operand, literal) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("=", operand, literal, false)
                    Comparison(path, op, operand, operand)
                case NotEqualToLiteralCondition(operand, literal) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("<>", operand, literal, false)
                    Comparison(path, op, operand, operand)
                case LikeCondition(operand, s, isNeg) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("LIKE", operand, s, isNeg)
                    Comparison(path, op, operand, operand)
                case InCondition(operand, literals, isNeg) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("IN", operand, literals, isNeg)
                    Comparison(path, op, operand, operand)
            })

            val comparisonHyperGraph = new ComparisonHyperGraph(comparisonHyperEdges.toSet)

            // joinTreesWithComparisonHyperGraph is a candidate(for selection of minimum degree)
            // only when the comparisonHyperGraph is berge-acyclic
            if (comparisonHyperGraph.isBergeAcyclic())
                List((joinTree, comparisonHyperGraph))
            else
                List.empty
        })

        RunResult(joinTreesWithComparisonHyperGraph, outputVariables, computations, isFull, groupByVariables, aggregations, optTopK)
    }

    def candidatesWithLimit(list: List[(JoinTree, ComparisonHyperGraph)], limit: Int): List[(JoinTree, ComparisonHyperGraph)] = {
        val zippedWithDegree = list.map(t => (t._1, t._2, t._2.getDegree()))
        val minDegree = zippedWithDegree.map(t => t._3).min
        zippedWithDegree.filter(t => t._3 == minDegree).take(limit).map(t => (t._1, t._2))
    }

    def convert(root: RelNode): ConvertResult = {
        val runResult = run(root)

        // select the joinTree and ComparisonHyperGraph with minimum degree
        val selected = runResult.joinTreesWithComparisonHyperGraph.minBy(t => t._2.getDegree())

        ConvertResult(selected._1, selected._2, runResult.outputVariables, runResult.computations, runResult.groupByVariables, runResult.aggregations, runResult.optTopK)
    }

    def traverseLogicalPlan(node: RelNode): (List[Relation], List[Condition], List[Variable], List[(Variable, Expression)], Boolean,
            List[Variable], List[(Variable, String, List[Expression])], Option[TopK]) = {
        var optLogicalSort: Option[LogicalSort] = None
        val root: RelNode = node match {
            case sort: LogicalSort =>
                optLogicalSort = Some(sort)
                sort.getInput
            case _ =>
                node
        }

        // Non-aggregation:
        // (1) LogicalProject(root) -> LogicalFilter -> LogicalJoin/LogicalAggregate/LogicalTableScan
        // Aggregation:
        // (1) LogicalProject(root) -> LogicalAggregate -> LogicalFilter -> LogicalJoin/LogicalAggregate/LogicalTableScan
        // (2) LogicalProject(root) -> LogicalAggregate -> LogicalProject -> LogicalFilter -> LogicalJoin/LogicalAggregate/LogicalTableScan
        // (3) LogicalAggregate(root) -> LogicalFilter -> LogicalJoin/LogicalAggregate/LogicalTableScan
        // (4) LogicalAggregate(root) -> LogicalProject -> LogicalFilter -> LogicalJoin/LogicalAggregate/LogicalTableScan
        assert(root.isInstanceOf[LogicalProject] || root.isInstanceOf[LogicalAggregate])
        val optLogicalProject = root match {
            case project: LogicalProject => Some(project)
            case _ => None
        }

        val optLogicalAggregate = if(optLogicalProject.nonEmpty && optLogicalProject.get.getInput.isInstanceOf[LogicalAggregate]) {
            Some(optLogicalProject.get.getInput.asInstanceOf[LogicalAggregate])
        } else root match {
            case aggregate: LogicalAggregate => Some(aggregate)
            case _ => None
        }

        val optIntermediateLogicalProject = if (optLogicalAggregate.nonEmpty && optLogicalAggregate.get.getInput.isInstanceOf[LogicalProject])
            Some(optLogicalAggregate.get.getInput.asInstanceOf[LogicalProject])
        else
            None

        val logicalFilter = if (optIntermediateLogicalProject.nonEmpty)
            optIntermediateLogicalProject.get.getInput.asInstanceOf[LogicalFilter]
        else if (optLogicalAggregate.nonEmpty)
            optLogicalAggregate.get.getInput.asInstanceOf[LogicalFilter]
        else
            optLogicalProject.get.getInput.asInstanceOf[LogicalFilter]

        val condition = logicalFilter.getCondition.asInstanceOf[RexCall]
        assert(condition.getOperator.getName != "OR")
        val operands = (if (condition.getOperator.getName == "AND") condition.getOperands.toList else List(condition))
            .filter(n => !n.isInstanceOf[RexLiteral] || n.asInstanceOf[RexLiteral].getValue.toString != "true")
        assert(operands.stream.allMatch((operand: RexNode) => operand.isInstanceOf[RexCall]))

        val variableTableBuffer = new ArrayBuffer[Variable]()
        val disjointSet = new DisjointSet[Variable]()
        for (field <- logicalFilter.getInput.getRowType.getFieldList) {
            val fieldType = DataType.fromSqlType(field.getType.getSqlTypeName)
            val newVariable = variableManager.getNewVariable(fieldType)
            disjointSet.makeNewSet(newVariable)
            variableTableBuffer.append(newVariable)
        }
        val variableTable = variableTableBuffer.toArray
        val conditionRexCalls = ListBuffer.empty[(RexCall, Boolean)]    // List[(rexCall, isNeg)]

        for (operand <- operands) {
            processOperandInCondition(operand, variableTable, disjointSet, false, conditionRexCalls)
        }

        for (i <- variableTable.indices) {
            // replace variables in variableTable with their representatives
            variableTable(i) = disjointSet.getRepresentative(variableTable(i))
        }

        val relations = visitLogicalRelNode(logicalFilter.getInput, variableTable, 0)
        val conditions = conditionRexCalls.toList.map(t => {
            val (rexCall, isNeg) = t
            rexCall.getOperator.getName match {
                case "=" if rexCall.getOperands.get(1).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle r_name = 'EUROPE'
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                    EqualToLiteralCondition(operand, literal)
                case "=" if rexCall.getOperands.get(0).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle 'EUROPE' = r_name
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(0).asInstanceOf[RexLiteral])
                    EqualToLiteralCondition(operand, literal)
                case "<>" if rexCall.getOperands.get(1).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle r_name <> 'EUROPE'
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                    NotEqualToLiteralCondition(operand, literal)
                case "<>" if rexCall.getOperands.get(0).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle 'EUROPE' <> r_name
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(0).asInstanceOf[RexLiteral])
                    NotEqualToLiteralCondition(operand, literal)
                case "<" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                    LessThanCondition(leftOperand, rightOperand)
                case "<=" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                    LessThanOrEqualToCondition(leftOperand, rightOperand)
                case ">" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                    GreaterThanCondition(leftOperand, rightOperand)
                case ">=" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                    GreaterThanOrEqualToCondition(leftOperand, rightOperand)
                case "LIKE" =>
                    assert(rexCall.getOperands.get(0).isInstanceOf[RexInputRef])
                    assert(rexCall.getOperands.get(1).isInstanceOf[RexLiteral])
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val s = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral]).asInstanceOf[StringLiteralExpression]
                    LikeCondition(operand, s, isNeg)
                case "OR" =>
                    // we don't allow explicit 'OR'. The 'OR' must come from condition like R.a IN ('b', 'c', 'd').
                    // this 'IN' becomes a 'OR' over 3 '=' with literal. Also, the column in the '=' must be the same.
                    assert(rexCall.getOperands.forall(n => n.isInstanceOf[RexCall] && n.asInstanceOf[RexCall].getOperator.getName == "="))
                    val leftOperands = rexCall.getOperands.map(n => n.asInstanceOf[RexCall].getOperands.get(0))
                    val rightOperands = rexCall.getOperands.map(n => n.asInstanceOf[RexCall].getOperands.get(1))
                    assert(leftOperands.forall(n => n.isInstanceOf[RexInputRef]))
                    assert(rightOperands.forall(n => n.isInstanceOf[RexLiteral]))
                    val operand = convertRexNodeToExpression(leftOperands.head, variableTable)
                    assert(leftOperands.forall(n => convertRexNodeToExpression(n, variableTable) == operand))
                    val literals = rightOperands.map(n => convertRexLiteralToExpression(n.asInstanceOf[RexLiteral])).toList
                    InCondition(operand, literals, isNeg)
            }
        })

        if (optLogicalAggregate.nonEmpty) {
            // computations are those "intermediate" variables used in aggregate functions.
            // e.g., for SUM(v1 * v4), we store v5 -> *(v1, v4) in computations.
            val intermediateComputations = mutable.HashMap.empty[Variable, Expression]
            val aggregateInputVariables = if (optIntermediateLogicalProject.isEmpty) {
                // if no IntermediateLogicalProject, the aggregation is applied on the result of LogicalFilter
                variableTable.foreach(v => {
                    val expr = SingleVariableExpression(v)
                    intermediateComputations(v) = expr
                })
                variableTable.toList
            } else {
                optIntermediateLogicalProject.get.getProjects.toList.map({
                    case call: RexCall =>
                        val expr = convertRexCallToExpression(call, variableTable)
                        val variable = variableManager.getNewVariable(DataType.fromSqlType(call.getType.getSqlTypeName))
                        intermediateComputations(variable) = expr
                        variable
                    case inputRef: RexInputRef =>
                        val variable = variableTable(inputRef.getIndex)
                        intermediateComputations(variable) = SingleVariableExpression(variable)
                        variable
                    case literal: RexLiteral =>
                        val expr = convertRexLiteralToExpression(literal)
                        val variable = variableManager.getNewVariable(DataType.fromSqlType(literal.getType.getSqlTypeName))
                        intermediateComputations(variable) = expr
                        variable
                })
            }

            val logicalAggregate = optLogicalAggregate.get
            val groupByVariables = logicalAggregate.getGroupSet.asList().map(_.toInt).map(i => aggregateInputVariables(i)).toList
            val aggregateCalls = logicalAggregate.getAggCallList.toList
            val aggregations = aggregateCalls.map(c => {
                val aggregateFunction = c.getAggregation.getName
                val aggregateArguments = c.getArgList.toList.map(i => intermediateComputations(aggregateInputVariables(i)))
                (variableManager.getNewVariable(DataType.fromSqlType(c.getType.getSqlTypeName)), aggregateFunction, aggregateArguments)
            })
            // the output of logicalAggregate is always groupByVariables followed by aggregation columns
            val aggregateFullResultVariables = groupByVariables ++ aggregations.map(t => t._1)
            val outputVariables = if (optLogicalProject.nonEmpty) {
                optLogicalProject.get.getProjects.toList.map(p => aggregateFullResultVariables(p.asInstanceOf[RexInputRef].getIndex))
            } else {
                // LogicalAggregate is the root. outputVariables are exactly the same the output of LogicalAggregate.
                aggregateFullResultVariables
            }

            val isFull = false  // isFull is useless for aggregation queries
            // report the computations for variables that are not in original relations
            val computations = intermediateComputations.filter(kv => outputVariables.contains(kv._1) && !kv._2.isInstanceOf[SingleVariableExpression]).toList

            (relations, conditions, outputVariables, computations, isFull, groupByVariables, aggregations, None)
        } else {
            val intermediateComputations = mutable.HashMap.empty[Variable, Expression]
            val outputVariables = optLogicalProject.get.getProjects.toList.map({
                case call: RexCall =>
                    val expr = convertRexCallToExpression(call, variableTable)
                    val variable = variableManager.getNewVariable(DataType.fromSqlType(call.getType.getSqlTypeName))
                    intermediateComputations(variable) = expr
                    variable
                case inputRef: RexInputRef =>
                    val variable = variableTable(inputRef.getIndex)
                    intermediateComputations(variable) = SingleVariableExpression(variable)
                    variable
                case literal: RexLiteral =>
                    val expr = convertRexLiteralToExpression(literal)
                    val variable = variableManager.getNewVariable(DataType.fromSqlType(literal.getType.getSqlTypeName))
                    intermediateComputations(variable) = expr
                    variable
            })
            val isFull = variableTable.forall(v => outputVariables.contains(v))
            val computations = intermediateComputations.filter(kv => outputVariables.contains(kv._1) && !kv._2.isInstanceOf[SingleVariableExpression]).toList

            val optTopK = optLogicalSort.map(logicalSort => {
                val isDesc = logicalSort.getCollation.getFieldCollations.get(0).getDirection.isDescending
                val sortBy = logicalSort.getCollation.getFieldCollations.get(0).getFieldIndex
                val limit = logicalSort.fetch.asInstanceOf[RexLiteral].getValue.asInstanceOf[java.math.BigDecimal].intValue()
                TopK(outputVariables(sortBy), isDesc, limit)
            })

            (relations, conditions, outputVariables, computations, isFull, List(), List(), optTopK)
        }
    }

    private def processOperandInCondition(operand: RexNode, variableTable: Array[Variable], disjointSet: DisjointSet[Variable],
                                          isNeg: Boolean, result: ListBuffer[(RexCall, Boolean)]): Unit = {
        val rexCall: RexCall = operand.asInstanceOf[RexCall]
        rexCall.getOperator.getName match {
            case "=" =>
                if ((rexCall.getOperands.get(1).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(0).isInstanceOf[RexLiteral])
                    || (rexCall.getOperands.get(0).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(1).isInstanceOf[RexLiteral])) {
                    // this is a filter condition like r_name = 'EUROPE'
                    result.append((rexCall, isNeg))
                } else {
                    // this is a join condition like R.a = S.b
                    val left: Expression = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                    val right: Expression = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)

                    val leftVariable = left.asInstanceOf[SingleVariableExpression].variable
                    val rightVariable = right.asInstanceOf[SingleVariableExpression].variable
                    disjointSet.merge(leftVariable, rightVariable)
                }
            case "<" | "<=" | ">" | ">=" | "LIKE" | "OR" =>
                result.append((rexCall, isNeg))
            case "NOT" =>
                processOperandInCondition(rexCall.getOperands.get(0), variableTable, disjointSet, !isNeg, result)
            case "<>" =>
                assert((rexCall.getOperands.get(1).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(0).isInstanceOf[RexLiteral])
                    || (rexCall.getOperands.get(0).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(1).isInstanceOf[RexLiteral]))
                result.append((rexCall, isNeg))
            case _ =>
                throw new UnsupportedOperationException(s"unsupported operator ${rexCall.getOperator.getName}")
        }
    }

    private def visitLogicalRelNode(relNode: RelNode, variableTable: Array[Variable], offset: Int): List[Relation] = relNode match {
        case join: LogicalJoin => visitLogicalJoin(join, variableTable, offset)
        case aggregate: LogicalAggregate => visitLogicalAggregate(aggregate, variableTable, offset)
        case scan: LogicalTableScan => visitLogicalTableScan(scan, variableTable, offset)
        // TODO: support sub-queries like '(SELECT COUNT(*) AS cnt, src FROM path GROUP BY src) AS C2'
        case _ => throw new UnsupportedOperationException
    }

    private def visitLogicalJoin(logicalJoin: LogicalJoin, variableTable: Array[Variable], offset: Int): List[Relation] = {
        val leftFieldCount = logicalJoin.getLeft.getRowType.getFieldCount
        visitLogicalRelNode(logicalJoin.getLeft, variableTable, offset) ++
            visitLogicalRelNode(logicalJoin.getRight, variableTable, offset + leftFieldCount)
    }

    private def visitLogicalAggregate(logicalAggregate: LogicalAggregate, variableTable: Array[Variable], offset: Int): List[Relation] = {
        // currently, only support patterns like '(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1'
        assert(logicalAggregate.getGroupCount == 1)
        assert(logicalAggregate.getAggCallList.size() == 1)

        val group = logicalAggregate.getGroupSet.asList().map(_.toInt)
        val variables = variableTable.slice(offset, offset + logicalAggregate.getRowType.getFieldCount)
        val func = logicalAggregate.getAggCallList.get(0).getAggregation.toString

        // TODO: support cases that can not be deal with AggregateProjectMergeRule
        assert(logicalAggregate.getInput.isInstanceOf[LogicalTableScan])
        val logicalTableScan = logicalAggregate.getInput.asInstanceOf[LogicalTableScan]
        val tableName = logicalTableScan.getTable.getQualifiedName.head

        if (logicalAggregate.getHints.nonEmpty) {
            List(new AggregatedRelation(tableName, variables.toList, group.toList, func, logicalAggregate.getHints.get(0).kvOptions.get("alias")))
        } else {
            List(new AggregatedRelation(tableName, variables.toList, group.toList, func, s"${func}_${group.mkString("_")}_$tableName"))
        }
    }

    private def visitLogicalTableScan(logicalTableScan: LogicalTableScan, variableTable: Array[Variable], offset: Int): List[Relation] = {
        val tableName = logicalTableScan.getTable.getQualifiedName.head
        val variables = variableTable.slice(offset, offset + logicalTableScan.getRowType.getFieldCount)

        if (logicalTableScan.getHints.nonEmpty) {
            List(new TableScanRelation(tableName, variables.toList, logicalTableScan.getHints.get(0).kvOptions.get("alias")))
        } else {
            List(new TableScanRelation(tableName, variables.toList, tableName))
        }
    }

    /**
     * Get the shortest path that connects two relations with variable from and to.
     * Note that such a shortest path can be proofed to be unique.
     *
     * @param nodes all the nodes(relations) in the join tree
     * @param joinTree collection of JoinTreeEdges
     * @param left Expression that lies on the left side of the comparison
     * @param right Expression that lies on the right side of the comparison
     * @return a list of JoinTreeEdge that form the shortest path
     */
    def getShortestInRelationalHyperGraph(nodes: Set[Relation], joinTree: JoinTree,
                                          left: Expression, right: Expression): List[JoinTreeEdge] = {
        val edges = new mutable.HashMap[Relation, mutable.HashSet[Relation]]()

        // add edges in join tree with 2 directions since we may need to go from parent to children
        joinTree.getEdges().foreach(edge => {
            edges.getOrElseUpdate(edge.getSrc, mutable.HashSet.empty).add(edge.getDst)
            edges.getOrElseUpdate(edge.getDst, mutable.HashSet.empty).add(edge.getSrc)
        })

        // we assume that the left or the right variables can be found in a single relation
        // after a transformation proceeding to this method
        val leftVariables = left.getVariables()
        val rightVariables = right.getVariables()

        // collection of nodes that contains the from/to variable, the nodes are of type RelationalHyperEdge
        val fromNodes = nodes.filter(e => e.getNodes().containsAll(leftVariables))
        val toNodes = nodes.filter(e => e.getNodes().containsAll(rightVariables))
        assert(fromNodes.nonEmpty)
        assert(toNodes.nonEmpty)

        // if some relations contain both the fromVariable and toVariable(it is a self comparison)
        // just return a singleton list with a special JoinTreeEdge that has the same element at src and dst
        val intersect = fromNodes.intersect(toNodes)
        if (intersect.nonEmpty) {
            val srcAndDst = intersect.head
            return List(new JoinTreeEdge(srcAndDst, srcAndDst))
        }

        // store the predecessor that on the path from source to the node.
        val nodeToPredecessorDict = new mutable.HashMap[Relation, Relation]()

        val currentQueue = mutable.Queue.empty[Relation]
        val nextQueue = mutable.Queue.empty[Relation]

        fromNodes.foreach(fromNode => {
            currentQueue.enqueue(fromNode)
            nodeToPredecessorDict(fromNode) = null
        })

        val reachedNodes = new mutable.HashSet[Relation]()
        while (reachedNodes.isEmpty) {
            while (currentQueue.nonEmpty) {
                val head = currentQueue.dequeue()
                val connectedNodes = edges(head)

                for (connectedNode <- connectedNodes) {
                    if (nodeToPredecessorDict.contains(connectedNode)) {
                        // we have already visited this node
                    } else {
                        // we have not visited this node yet
                        nodeToPredecessorDict(connectedNode) = head
                        nextQueue.enqueue(connectedNode)
                    }

                    // if we reach some target node, append it to the reachedNodes
                    // then we can recreate the whole path by tracing back
                    if (toNodes.contains(connectedNode)) {
                        reachedNodes.add(connectedNode)
                    }
                }
            }

            // move to next level
            while (nextQueue.nonEmpty)
                currentQueue.enqueue(nextQueue.dequeue())
        }

        // return the path starting from node until the predecessor is null
        def trace(node: Relation): List[JoinTreeEdge] = {
            val predecessor = nodeToPredecessorDict(node)
            if (predecessor == null) {
                // we reach the starting node, no more JoinTreeEdges are needed
                List()
            } else {
                // for each predecessor, trace its paths and append the JoinTreeEdge(p -> node) to all the paths
                joinTree.getEdgeByNodes(predecessor, node) :: trace(predecessor)
            }
        }

        assert(reachedNodes.size == 1)

        // recreate the paths by tracing back
        trace(reachedNodes.head).reverse
    }

    def convertRexNodeToExpression(rexNode: RexNode, variableTable: Array[Variable]): Expression = {
        rexNode match {
            case rexInputRef: RexInputRef =>
                SingleVariableExpression(variableTable(rexInputRef.getIndex))
            case rexCall: RexCall if rexCall.op.getName == "CAST" =>
                convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
            case rexCall: RexCall =>
                convertRexCallToExpression(rexCall, variableTable)
            case rexLiteral: RexLiteral =>
                convertRexLiteralToExpression(rexLiteral)
            case _ =>
                throw new UnsupportedOperationException(s"unknown rexNode type ${rexNode.getType}")
        }
    }

    def convertRexCallToExpression(rexCall: RexCall, variableTable: Array[Variable]): Expression = {
        rexCall.op.getName match {
            case "+" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                if (left.getType() == TimestampDataType && right.getType() == IntervalDataType) {
                    TimestampPlusIntervalExpression(left, right)
                } else if (left.getType() == DateDataType && right.getType() == IntervalDataType) {
                    DatePlusIntervalExpression(left, right)
                } else {
                    DataTypeCasting.promote(left.getType(), right.getType()) match {
                        case DoubleDataType => DoublePlusDoubleExpression(left, right)
                        case LongDataType => LongPlusLongExpression(left, right)
                        case IntDataType => IntPlusIntExpression(left, right)
                    }
                }
            case "-" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                if (left.getType() == TimestampDataType && right.getType() == IntervalDataType) {
                    TimestampMinusIntervalExpression(left, right)
                } else if (left.getType() == DateDataType && right.getType() == IntervalDataType) {
                    DateMinusIntervalExpression(left, right)
                } else {
                    DataTypeCasting.promote(left.getType(), right.getType()) match {
                        case DoubleDataType => DoubleMinusDoubleExpression(left, right)
                        case LongDataType => LongMinusLongExpression(left, right)
                        case IntDataType => IntMinusIntExpression(left, right)
                    }
                }
            case "*" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                DataTypeCasting.promote(left.getType(), right.getType()) match {
                    case DoubleDataType => DoubleTimesDoubleExpression(left, right)
                    case LongDataType => LongTimesLongExpression(left, right)
                    case IntDataType => IntTimesIntExpression(left, right)
                }
            case "/" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                DataTypeCasting.promote(left.getType(), right.getType()) match {
                    case DoubleDataType => DoubleDivideByDoubleExpression(left, right)
                    case LongDataType => LongDivideByLongExpression(left, right)
                    case IntDataType => IntDivideByIntExpression(left, right)
                }
            case "CASE" =>
                buildCaseWhenExpression(rexCall.getOperands.toList, variableTable)
            case "EXTRACT" =>
                buildExtractExpression(rexCall.getOperands.toList, variableTable)
            case _ =>
                throw new UnsupportedOperationException("unsupported op " + rexCall.op.getName)
        }
    }

    def convertRexCallToOperatorAndExpressions(rexCall: RexCall, variableTable: Array[Variable]): (Operator, List[Expression]) = {
        rexCall.getOperator.getName match {
            case "SEARCH" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable).asInstanceOf[SingleVariableExpression]
                val dataType = left.getType()
                val sarg = rexCall.getOperands.get(1).asInstanceOf[RexLiteral].getValue.asInstanceOf[Sarg[_]]
                val ranges = sarg.rangeSet.asRanges().toList
                val isNeg = !ranges.get(0).hasLowerBound || ranges.get(0).lowerBoundType() == BoundType.OPEN

                dataType match {
                    case StringDataType =>
                        val hashSet = mutable.HashSet.empty[String]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[NlsString].getValue)
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[NlsString].getValue)
                        })
                        (StringInLiterals(hashSet.toList, isNeg), List(left))
                    case IntDataType =>
                        val hashSet = mutable.HashSet.empty[Int]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[java.math.BigDecimal].intValue())
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[java.math.BigDecimal].intValue())
                        })
                        (IntInLiterals(hashSet.toList, isNeg), List(left))
                    case LongDataType =>
                        val hashSet = mutable.HashSet.empty[Long]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[java.math.BigDecimal].longValue())
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[java.math.BigDecimal].longValue())
                        })
                        (LongInLiterals(hashSet.toList, isNeg), List(left))
                    case DoubleDataType =>
                        val hashSet = mutable.HashSet.empty[Double]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[java.math.BigDecimal].doubleValue())
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[java.math.BigDecimal].doubleValue())
                        })
                        (DoubleInLiterals(hashSet.toList, isNeg), List(left))
                }
            case _ =>
                throw new UnsupportedOperationException()
        }
    }

    def convertRexLiteralToExpression(rexLiteral: RexLiteral): LiteralExpression = {
        rexLiteral.getType.getSqlTypeName.getName match {
            case "VARCHAR" => StringLiteralExpression(rexLiteral.getValue.asInstanceOf[NlsString].getValue)
            case "INTEGER" => IntLiteralExpression(rexLiteral.getValue.toString.toInt)
            case "BIGINT" => LongLiteralExpression(rexLiteral.getValue.toString.toLong)
            case "DECIMAL" => DoubleLiteralExpression(rexLiteral.getValue.toString.toDouble)
            case "CHAR" => StringLiteralExpression(rexLiteral.getValue.asInstanceOf[NlsString].getValue)
            case "INTERVAL_DAY" => IntervalLiteralExpression(rexLiteral.getValue.toString.toLong)
            case "DATE" => DateLiteralExpression(rexLiteral.getValue.asInstanceOf[GregorianCalendar].getTime.getTime)
            case _ => throw new UnsupportedOperationException("unsupported literal type " + rexLiteral.getType.getSqlTypeName.getName)
        }
    }

    def buildCaseWhenExpression(operands: List[RexNode], variableTable: Array[Variable]): CaseWhenExpression = {
        var index = 0
        assert(operands.size % 2 == 1)  // we should have a default branch
        val buffer = ListBuffer.empty[(Operator, List[Expression], Expression)]

        while (index + 1 < operands.size) {
            // when clause
            val w = operands(index)
            val (wOp, wExprs) = convertRexCallToOperatorAndExpressions(w.asInstanceOf[RexCall], variableTable)
            // then clause
            val t = operands(index + 1)
            val tExpr = convertRexNodeToExpression(t, variableTable)
            buffer.append((wOp, wExprs, tExpr))
            index += 2
        }

        // now we reach the default branch
        val dExpr = convertRexNodeToExpression(operands.last, variableTable)
        CaseWhenExpression(buffer.toList, dExpr)
    }

    def buildExtractExpression(operands: List[RexNode], variableTable: Array[Variable]): Expression = {
        val flag = operands.get(0).asInstanceOf[RexLiteral].getValue.toString
        flag match {
            case "YEAR" =>
                val from = convertRexNodeToExpression(operands(1), variableTable)
                ExtractYearExpression(from)
            case _ => throw new UnsupportedOperationException("unsupported flag " + flag + " in EXTRACT.")
        }
    }
}
