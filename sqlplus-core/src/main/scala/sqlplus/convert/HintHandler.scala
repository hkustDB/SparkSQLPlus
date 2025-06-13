package sqlplus.convert

import sqlplus.expression.Variable
import sqlplus.graph.{JoinTree, JoinTreeEdge, Relation, RelationalHyperGraph}
import sqlplus.plan.hint.HintNode

import scala.collection.mutable

class HintHandler {
    def handle(context: Context, hint: HintNode): HandleResult = {
        val relations = context.relations
        val requiredVariables = context.requiredVariables
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables

        val relationMap = relationalHyperGraph.getEdges().flatMap(r => {
            if (r.getTableDisplayName() == r.getTableName()) {
                Set((r.getTableDisplayName(), r))
            } else {
                Set((r.getTableDisplayName(), r), (r.getTableName(), r))
            }
        }).toMap

        val parents = mutable.HashMap.empty[Relation, Relation]
        val edges = mutable.HashSet.empty[JoinTreeEdge]
        val subset = mutable.HashSet.empty[Relation]
        val uncovered = mutable.HashSet.empty[Variable]
        uncovered ++= topVariables
        val visited = mutable.HashSet.empty[Relation]
        var isFreeConnex = true

        def visit(node: HintNode, parent: HintNode): Unit = {
            val relation = relationMap(node.getRelation)
            if (visited.contains(relation)) {
                throw new RuntimeException(s"${relation.getTableDisplayName()} is duplicated in the given hint plan.")
            }
            visited.add(relation)

            if (parent != null) {
                parents(relation) = relationMap(parent.getRelation)
                edges.add(new JoinTreeEdge(relationMap(parent.getRelation), relation))
            }

            if (uncovered.intersect(relation.getNodes()).nonEmpty) {
                var p = parents.getOrElse(relation, null)
                while (p != null) {
                    if (!subset.contains(p)) {
                        isFreeConnex = false
                        subset.add(p)
                        p = parents.getOrElse(p, null)
                    } else {
                        p = null
                    }
                }

                subset.add(relation)
                relation.getNodes().foreach(v => uncovered.remove(v))
            }

            if (node.getChildren != null) {
                node.getChildren.forEach(n => visit(n, node))
            }
        }

        visit(hint, null)

        if (uncovered.nonEmpty) {
            throw new RuntimeException("Some variables are uncovered by the given hint plan.")
        }

        if (visited.size != relationalHyperGraph.getEdges().size) {
            throw new RuntimeException("Some hyperedges are uncovered by the given hint plan.")
        }

        val root = relationMap(hint.getRelation)

        val joinTree = JoinTree(root, edges.toSet, subset.toSet, isFixRoot = false)
        HandleResult(List((joinTree, relationalHyperGraph, List.empty[ExtraCondition])))
    }
}
