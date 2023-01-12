package sqlplus.graph

/**
 * A hyperEdge is a set of nodes in this hyperEdge.
 * @tparam T the type of the nodes
 */
trait HyperEdge[T] {
    def getNodes(): Set[T]
}
