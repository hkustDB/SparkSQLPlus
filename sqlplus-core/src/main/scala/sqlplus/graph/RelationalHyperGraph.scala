package sqlplus.graph

import sqlplus.expression.Variable

/**
 * A relationalHyperGraph is a set of relations(hyperEdges). Its nodes are variables and its hyperEdges are relations.
 * This class is immutable. Adding or removing a hyperEdge from a relationalHyperGraph instance will create an new
 * relationalHyperGraph instance.
 *
 * @param edges the relations in this relationalHyperGraph
 */
class RelationalHyperGraph(val edges: Set[Relation]) extends HyperGraph[Variable, Relation] {

    override def getEdges(): Set[Relation] = edges

    def addHyperEdge(edge: Relation): RelationalHyperGraph =
        new RelationalHyperGraph(edges + edge)

    def removeHyperEdge(edge: Relation): RelationalHyperGraph =
        new RelationalHyperGraph(edges - edge)

    def replaceHyperEdge(edge1: Relation, edge2: Relation): RelationalHyperGraph =
        new RelationalHyperGraph((edges - edge1) + edge2)

    def getSignature(): String = {
        edges.toList.map(r => {
            val relationName = r.getTableName()
            val variables = r.getNodes().map(v => v.name).toList.sorted.mkString("[",",","]")
            s"$relationName-$variables"
        }).sorted.mkString("#")
    }
}

object RelationalHyperGraph {
    val EMPTY = new RelationalHyperGraph(Set.empty[Relation])
}
