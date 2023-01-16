package sqlplus.graph

/**
 * A (directed) edge is a special hyperEdge that contains exactly 2 nodes.
 * @tparam T the type of the nodes
 */
trait Edge[T] extends HyperEdge[T] {
    def getSrc: T
    def getDst: T

    override def getNodes(): Set[T] = Set(getSrc, getDst)
}
