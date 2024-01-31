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

    lazy val keyType: KeyType = computeKeyType()

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

    private def computeKeyType(): KeyType = {
        val joinKey = node1.getNodes().intersect(node2.getNodes())
        (node1.getPrimaryKeys().subsetOf(joinKey), node2.getPrimaryKeys().subsetOf(joinKey)) match {
            case (true, true) => KeyTypeBoth
            case (true, false) => KeyTypeParent
            case (false, true) => KeyTypeChild
            case (false, false) => KeyTypeNone
        }
    }
}

sealed trait KeyType

case object KeyTypeParent extends KeyType {
    override def toString: String = "parent"
}

case object KeyTypeChild extends KeyType {
    override def toString: String = "child"
}

case object KeyTypeBoth extends KeyType {
    override def toString: String = "both"
}

case object KeyTypeNone extends KeyType {
    override def toString: String = "none"
}
