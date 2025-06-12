package sqlplus.graph

import sqlplus.expression.Variable

import scala.collection.mutable

/**
 * A joinTree is a set of JoinTreeEdge and a root relation.
 *
 * @param root the root relation
 * @param edges the joinTreeEdges
 * @param subset the minimal subset(including the root) that covers all the output variables
 */
class JoinTree(val root: Relation, val edges: Set[JoinTreeEdge], val subset: Set[Relation], val isFixRoot: Boolean) extends HyperGraph[Relation, JoinTreeEdge] {
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
}

object JoinTree {
    def apply(root: Relation, edges: Set[JoinTreeEdge], subset: Set[Relation], isFixRoot: Boolean): JoinTree = {
        new JoinTree(root, edges, subset, isFixRoot)
    }

    def computeReserveVariables(joinTree: JoinTree): Map[Relation, List[String]] = {
        val mergedFromTop = mutable.HashMap.empty[Relation, Set[Variable]]
        val mergedFromBottom = mutable.HashMap.empty[Relation, Set[Variable]]
        val reserve = mutable.HashMap.empty[Relation, Set[Variable]]
        val children = joinTree.getEdges().groupBy(e => e.getSrc).map(t => (t._1, t._2.map(e => e.getDst)))

        def f(relation: Relation, parent: Relation): Unit = {
            mergedFromBottom(relation) = relation.getNodes()

            if (children.contains(relation)) {
                children(relation).foreach(c => f(c, relation))
            }

            if (parent != null) {
                mergedFromBottom(parent) = mergedFromBottom(parent).union(mergedFromBottom(relation))
            }
        }
        f(joinTree.root, null)

        def g(relation: Relation): Unit = {
            reserve(relation) = mergedFromBottom(relation).intersect(mergedFromTop(relation))

            if (children.contains(relation)) {
                children(relation).foreach(c => {
                    val tmp = mutable.HashSet.empty[Variable]
                    children(relation).filter(r => r != c).foreach(r => mergedFromBottom(r).foreach(v => tmp.add(v)))
                    mergedFromTop(relation).foreach(v => tmp.add(v))
                    relation.getNodes().foreach(v => tmp.add(v))
                    mergedFromTop(c) = tmp.toSet
                    g(c)
                })
            }
        }
        mergedFromTop(joinTree.root) = Set.empty
        g(joinTree.root)

        reserve.map(t => (t._1, t._2.toList.map(v => v.name))).toMap
    }
}

