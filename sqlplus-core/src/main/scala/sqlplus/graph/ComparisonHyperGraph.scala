package sqlplus.graph

import scala.collection.mutable

/**
 * A comparisonHyperGraph is a set of comparisons. Its nodes are joinTreeEdges and its hyperEdges are comparisons.
 * @param edges the set of comparisons
 */
class ComparisonHyperGraph(val edges: Set[Comparison]) extends HyperGraph[JoinTreeEdge, Comparison] {
    override def getEdges(): Set[Comparison] = edges

    def getDegree(): Int = {
        val degrees = new mutable.HashMap[JoinTreeEdge, Int]()

        for (
            edge <- getEdges();
            node <- edge.getNodes()
        ) {
            degrees(node) = degrees.getOrElse(node, 0) + 1
        }

        if (degrees.nonEmpty) degrees.values.max else 0
    }

    def isBergeAcyclic(): Boolean = {
        // create a bipartite graph with the comparisons as one set and the joinTreeEdges as another set.
        // one element from the joinTreeEdges set is connected with one element from the comparisons set
        // if and only if the comparison contains the joinTreeEdge
        val nodesInBipartiteGraph: Set[Either[Comparison, JoinTreeEdge]] = edges.map(cmp => Left(cmp)) ++
            edges.flatMap(cmp => cmp.getNodes()).map(joinTreeEdge => Right(joinTreeEdge))
        val nodesInBipartiteGraphToId: Map[Either[Comparison, JoinTreeEdge], Int] = nodesInBipartiteGraph.zipWithIndex.toMap
        val bipartiteGraphEdges: mutable.HashMap[Int, mutable.HashSet[Int]] = mutable.HashMap.empty
        edges.foreach(cmp => {
            val cmpId = nodesInBipartiteGraphToId(Left(cmp))
            val joinTreeEdges = cmp.getNodes()
            joinTreeEdges.foreach(joinTreeEdge => {
                val joinTreeEdgeId = nodesInBipartiteGraphToId(Right(joinTreeEdge))
                bipartiteGraphEdges.getOrElseUpdate(cmpId, mutable.HashSet.empty).add(joinTreeEdgeId)
                bipartiteGraphEdges.getOrElseUpdate(joinTreeEdgeId, mutable.HashSet.empty).add(cmpId)
            })
        })

        // check if circle exists
        val visitedNodes: mutable.HashSet[Int] = mutable.HashSet.empty
        val predecessors: Array[Int] = Array.fill(nodesInBipartiteGraph.size)(-1)

        def noCircle(node: Int): Boolean = {
            if (!visitedNodes.contains(node)) {
                // next node can any connected node other than the predecessor
                val nextNodes = bipartiteGraphEdges(node).toSet - predecessors(node)
                if (nextNodes.intersect(visitedNodes).nonEmpty) {
                    // this node connects with a visited node which differs from its predecessor
                    false
                } else {
                    visitedNodes.add(node)
                    nextNodes.forall(next => {
                        predecessors(next) = node
                        noCircle(next)
                    })
                }
            } else {
                true
            }
        }

        nodesInBipartiteGraphToId.values.forall(id => noCircle(id))
    }
}
