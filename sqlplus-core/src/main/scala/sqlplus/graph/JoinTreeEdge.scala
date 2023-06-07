package sqlplus.graph

/**
 * A joinTreeEdge is a directed edge from parent relation to child relation.
 * Since it is also used in finding the shortest path, x = JoinTreeEdge(a,b) and
 * y = JoinTreeEdge(b,a) should have the same hashcode, and x.equals(y) must return true.
 *
 * @param node1 the first relation
 * @param node2 the second relation
 */
class JoinTreeEdge(val node1: Relation, val node2: Relation) extends Edge[Relation] {
    override def getSrc: Relation = node1
    override def getDst: Relation = node2

    override def toString: String =
        "JoinTreeEdge(" + node1.toString + "->" + node2.toString + ")"

    override def hashCode(): Int = node1.## ^ node2.##

    override def equals(obj: Any): Boolean = obj match {
        case that: JoinTreeEdge => (that.node1.equals(this.node1) && that.node2.equals(this.node2)) ||
            (that.node1.equals(this.node2) && that.node2.equals(this.node1))
        case _ => false
    }

    def connects(n1: Relation, n2: Relation): Boolean =
        (n1.equals(node1) && n2.equals(node2)) || (n1.equals(node2) && n2.equals(node1))
}
