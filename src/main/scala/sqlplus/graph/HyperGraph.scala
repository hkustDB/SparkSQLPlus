package sqlplus.graph

/**
 * A hyperGraph is a set of hyperEdges.
 * @tparam N the type of the nodes
 * @tparam E the type of the hyperEdges. E must be a subclass of HyperEdge[N]
 */
trait HyperGraph[N,E <: HyperEdge[N]] {

    def getEdges(): Set[E]

    def nonEmpty(): Boolean = !isEmpty()

    def isEmpty(): Boolean = getEdges().isEmpty

    def size(): Int = getEdges().size
}
