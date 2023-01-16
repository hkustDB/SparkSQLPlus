package sqlplus.graph

/**
 * A joinTree is a set of JoinTreeEdge and a root relation.
 * @param root the root relation
 * @param edges the joinTreeEdges
 */
class JoinTree(val root: Relation, val edges: Set[JoinTreeEdge], val subset: Set[Relation]) extends HyperGraph[Relation, JoinTreeEdge] {
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
}

object JoinTree {
    def apply(root: Relation, edges: Set[JoinTreeEdge], subset: Set[Relation]): JoinTree = {
        new JoinTree(root, edges, subset)
    }
}

