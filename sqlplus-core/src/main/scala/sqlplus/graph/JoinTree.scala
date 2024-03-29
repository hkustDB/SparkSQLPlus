package sqlplus.graph

/**
 * A joinTree is a set of JoinTreeEdge and a root relation.
 *
 * @param root the root relation
 * @param edges the joinTreeEdges
 * @param subset the minimal subset(including the root) that covers all the output variables
 */
class JoinTree(val root: Relation, val edges: Set[JoinTreeEdge], val subset: Set[Relation], val isFixRoot: Boolean) extends HyperGraph[Relation, JoinTreeEdge] {
    lazy val maxFanout: Int = if (edges.nonEmpty) edges.groupBy(e => e.getSrc).values.map(s => s.size).max else 0

    lazy val pk2fkCount: Int = edges.count(e => e.keyType == KeyTypeParent)

    lazy val fk2pkCount: Int = edges.count(e => e.keyType == KeyTypeChild)

    override def getEdges(): Set[JoinTreeEdge] = edges

    /**
     * get the joinTreeEdge that directly connects these two nodes.
     * @param node1 the first node
     * @param node2 the second node
     * @return the joinTreeEdge
     */
    def getEdgeByNodes(node1: Relation, node2: Relation): JoinTreeEdge =
        edges.find(e => e.connects(node1, node2)).get

    def getRoot(): Relation = root

    def getSubset(): Set[Relation] = subset

    def getMaxFanout(): Int = maxFanout
}

object JoinTree {
    def apply(root: Relation, edges: Set[JoinTreeEdge], subset: Set[Relation], isFixRoot: Boolean): JoinTree = {
        new JoinTree(root, edges, subset, isFixRoot)
    }
}

