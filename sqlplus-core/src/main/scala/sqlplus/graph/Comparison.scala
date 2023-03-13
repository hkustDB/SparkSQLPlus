package sqlplus.graph

import sqlplus.expression.{Operator, Expression}

/**
 * A comparison is a set of joinTreeEdge with a ComparisonOperator and 2 expressions.
 *
 * @param nodes the nodes(joinTreeEdge) in this comparison
 * @param op the operator in this comparison
 * @param left the left expression
 * @param right the right expression
 */
class Comparison(val nodes: Set[JoinTreeEdge], val op: Operator, val left: Expression, val right: Expression) extends HyperEdge[JoinTreeEdge] {
    val comparisonId = Comparison.getNewComparisonId()

    def getComparisonId(): Int = comparisonId

    override def getNodes(): Set[JoinTreeEdge] = nodes

    override def equals(obj: Any): Boolean = obj match {
        case that: Comparison => that.comparisonId == this.comparisonId
        case _ => false
    }

    override def hashCode(): Int = (nodes, op, left, right).##

    override def toString: String = {
        val path = nodes.map(e => e.getSrc.getRelationId() + "<->" + e.getDst.getRelationId()).mkString(",")
        s"Comparison[id=$comparisonId][op=${op.getFuncName()}][left=$left][right=$right][path=$path]"
    }
}

object Comparison {
    var ID = 0

    def getNewComparisonId(): Int = {
        ID += 1
        ID
    }

    def apply(nodes: Set[JoinTreeEdge], op: String, left: Expression, right: Expression): Comparison = {
        new Comparison(nodes, Operator.getOperator(op, left, right), left, right)
    }
}