package sqlplus.convert

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalFilter, LogicalJoin, LogicalProject, LogicalTableScan}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.util.NlsString
import sqlplus.expression.{DoubleLiteralExpression, DoublePlusDoubleExpression, DoubleTimesDoubleExpression, Expression, IntLiteralExpression, IntPlusIntExpression, IntTimesIntExpression, IntervalLiteralExpression, LongPlusLongExpression, LongTimesLongExpression, SingleVariableExpression, StringLiteralExpression, TimestampPlusIntervalExpression, Variable, VariableManager}
import sqlplus.ghd.GhdAlgorithm
import sqlplus.graph.{AggregatedRelation, Comparison, ComparisonHyperGraph, JoinTree, JoinTreeEdge, Relation, RelationalHyperGraph, TableScanRelation}
import sqlplus.gyo.GyoAlgorithm
import sqlplus.types.{DataType, DoubleDataType, IntDataType, IntervalDataType, LongDataType, TimestampDataType}
import sqlplus.utils.DisjointSet

import scala.collection.JavaConversions._
import scala.collection.immutable.List
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
    var suffix: Int = 0

    def run(root: RelNode): (List[(JoinTree, ComparisonHyperGraph)], List[Variable], String) = {
        val (relations, comparisons, outputVariables, isFull) = traverseLogicalPlan(root)
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        val optGyoResult = gyo.run(relationalHyperGraph, outputVariables.toSet)
        val joinTreeWithHyperGraphs = if (optGyoResult.nonEmpty) {
            optGyoResult.get.joinTreeWithHyperGraphs
        } else {
            val result = ghd.run(relationalHyperGraph, outputVariables.toSet)
            result.joinTreeWithHyperGraphs
        }

        // construct a ComparisonHyperGraph for each join tree, the ComparisonHyperGraph has the minimum degree
        val joinTreesWithComparisonHyperGraph: List[(JoinTree, ComparisonHyperGraph)] = joinTreeWithHyperGraphs.flatMap(t => {
            val joinTree = t._1
            val hyperGraph = t._2
            val comparisonHyperEdges: List[Comparison] = comparisons.map(comparison => {
                val op = comparison._1
                val left = comparison._2
                val right = comparison._3
                val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, left, right).toSet
                Comparison(path, op, left, right)
            })

            val comparisonHyperGraph = new ComparisonHyperGraph(comparisonHyperEdges.toSet)

            // joinTreesWithComparisonHyperGraph is a candidate(for selection of minimum degree)
            // only when the comparisonHyperGraph is berge-acyclic
            if (comparisonHyperGraph.isBergeAcyclic())
                List((joinTree, comparisonHyperGraph))
            else
                List.empty
        })

        (joinTreesWithComparisonHyperGraph, outputVariables, isFull)
    }

    def candidatesWithLimit(list: List[(JoinTree, ComparisonHyperGraph)], limit: Int): List[(JoinTree, ComparisonHyperGraph)] = {
        val zippedWithDegree = list.map(t => (t._1, t._2, t._2.getDegree()))
        val minDegree = zippedWithDegree.map(t => t._3).min
        zippedWithDegree.filter(t => t._3 == minDegree).take(limit).map(t => (t._1, t._2))
    }

    def convert(root: RelNode): ConvertResult = {
        val (joinTreesWithComparisonHyperGraph, outputVariables, _) = run(root)

        // select the joinTree and ComparisonHyperGraph with minimum degree
        val selected = joinTreesWithComparisonHyperGraph.minBy(t => t._2.getDegree())

        ConvertResult(outputVariables, selected._1, selected._2)
    }

    def traverseLogicalPlan(root: RelNode): (List[Relation], List[(String, Expression, Expression)], List[Variable], String) = {
        assert(root.isInstanceOf[LogicalProject])
        assert(root.getInput(0).isInstanceOf[LogicalFilter])
        assert(root.getInput(0).getInput(0).isInstanceOf[LogicalJoin])

        val logicalProject = root.asInstanceOf[LogicalProject]
        val logicalFilter = root.getInput(0).asInstanceOf[LogicalFilter]
        assert(logicalFilter.getCondition.isInstanceOf[RexCall])
        val condition = logicalFilter.getCondition.asInstanceOf[RexCall]
        assert(condition.getOperator.getName == "AND")
        val operands = condition.getOperands
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
        val conditions = ListBuffer.empty[RexCall]

        for (operand <- operands) {
            val rexCall: RexCall = operand.asInstanceOf[RexCall]
            rexCall.getOperator.getName match {
                case "=" =>
                    // we can't handle conditions like R.a = S.b + T.c
                    val left: RexInputRef = rexCall.getOperands.get(0).asInstanceOf[RexInputRef]
                    val right: RexInputRef = rexCall.getOperands.get(1).asInstanceOf[RexInputRef]

                    val leftVariable = variableTable(left.getIndex)
                    val rightVariable = variableTable(right.getIndex)
                    disjointSet.merge(leftVariable, rightVariable)
                case "<" | "<=" | ">" | ">=" =>
                    conditions.append(rexCall)
                case "LIKE" =>
                    assert(rexCall.getOperands.get(0).isInstanceOf[RexInputRef])
                    assert(rexCall.getOperands.get(1).isInstanceOf[RexLiteral])
                    conditions.append(rexCall)
                case _ =>
                    throw new UnsupportedOperationException(s"unsupported operator ${rexCall.getOperator.getName}")
            }
        }

        for (i <- variableTable.indices) {
            // replace variables in variableTable with their representatives
            variableTable(i) = disjointSet.getRepresentative(variableTable(i))
        }

        val relations = visitLogicalRelNode(logicalFilter.getInput, variableTable, 0)
        val comparisons = conditions.toList.map(rawComparison => {
            val op = rawComparison.getOperator.getName
            val leftExpr = convertRexNodeToExpression(rawComparison.getOperands.get(0), variableTable)
            val rightExpr = convertRexNodeToExpression(rawComparison.getOperands.get(1), variableTable)
            (op, leftExpr, rightExpr)
        })

        val outputVariables = logicalProject.getProjects.toList.map(p => variableTable(p.asInstanceOf[RexInputRef].getIndex))
        val isFull = variableTable.forall(v => outputVariables.contains(v))

        (relations, comparisons, outputVariables, if (isFull) "full" else "non-full")
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
                if (left.getType() == IntDataType && right.getType() == IntDataType)
                    IntPlusIntExpression(left, right)
                else if (left.getType() == LongDataType && right.getType() == LongDataType) {
                    LongPlusLongExpression(left, right)
                } else if (left.getType() == TimestampDataType && right.getType() == IntervalDataType) {
                    TimestampPlusIntervalExpression(left, right)
                } else if (left.getType() == DoubleDataType && right.getType() == DoubleDataType) {
                    DoublePlusDoubleExpression(left, right)
                } else {
                    // TODO: more types
                    throw new UnsupportedOperationException(s"unsupported + for ${left.getType()} and ${right.getType()}")
                }
            case "*" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableTable)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableTable)
                if (left.getType() == IntDataType && right.getType() == IntDataType) {
                    IntTimesIntExpression(left, right)
                } else if (left.getType() == LongDataType && right.getType() == LongDataType) {
                    LongTimesLongExpression(left, right)
                } else if (left.getType() == DoubleDataType && right.getType() == DoubleDataType) {
                    DoubleTimesDoubleExpression(left, right)
                } else {
                    // TODO: more types
                    throw new UnsupportedOperationException(s"unsupported * for ${left.getType()} and ${right.getType()}")
                }
            case _ =>
                throw new UnsupportedOperationException("unsupported op " + rexCall.op.getName)
        }
    }

    def convertRexLiteralToExpression(rexLiteral: RexLiteral): Expression = {
        rexLiteral.getType.getSqlTypeName.getName match {
            case "VARCHAR" => StringLiteralExpression(rexLiteral.getValue.toString)
            case "INTEGER" => IntLiteralExpression(rexLiteral.getValue.toString.toInt)
            case "DECIMAL" => DoubleLiteralExpression(rexLiteral.getValue.toString.toDouble)
            case "CHAR" => StringLiteralExpression(rexLiteral.getValue.asInstanceOf[NlsString].getValue)
            case "INTERVAL_DAY" => IntervalLiteralExpression(rexLiteral.getValue.toString.toLong)
            case _ => throw new UnsupportedOperationException("unsupported literal type " + rexLiteral.getType.getSqlTypeName.getName)
        }
    }
}
