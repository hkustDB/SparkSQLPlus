package sqlplus.convert

import sqlplus.expression.{SingleVariableExpression, VariableManager}
import sqlplus.graph.{Relation, RelationalHyperGraph}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CyclicHandler(val variableManager: VariableManager,
                    val breakableCyclicHandler: BreakableCyclicHandler,
                    val unbreakableCyclicHandler: UnbreakableCyclicHandler) {

    def handle(context: Context): HandleResult = {
        val relations = context.relations
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        val optBreakResult = break(relationalHyperGraph)
        if (optBreakResult.nonEmpty) {
            val breakResult = optBreakResult.get
            breakableCyclicHandler.handle(context, breakResult)
        } else {
            unbreakableCyclicHandler.handle(context, relationalHyperGraph)
        }
    }

    def break(relationalHyperGraph: RelationalHyperGraph): Option[List[(RelationalHyperGraph, List[ExtraCondition])]] = {
        // A query is breakable if:
        // 1. Primary Key is nonempty for all relations
        // 2. There is only one node with 0 in-degree (the fact table)
        // 3. All the relations are reachable from the fact table
        // 4. Starting from the fact table, there are more than 1 paths that meet at the same table (e.g., nation in TPCH-Q5)
        if (relationalHyperGraph.getEdges().exists(r => r.getPrimaryKeys().isEmpty)) {
            // cond 1 unsatisfied
            return None
        }

        // find the fact table (if any)
        val reachable: mutable.HashMap[Relation, mutable.HashSet[Relation]] = mutable.HashMap.empty
        val inDegree: mutable.HashMap[Relation, Int] = mutable.HashMap.empty
        relationalHyperGraph.getEdges().foreach(r => inDegree(r) = 0)

        for {
            r1 <- relationalHyperGraph.getEdges()
            r2 <- relationalHyperGraph.getEdges()
            if r1.relationId < r2.relationId
        } {
            val overlap = r1.getNodes().intersect(r2.getNodes())
            if (overlap.nonEmpty) {
                if (r1.getPrimaryKeys().subsetOf(overlap)) {
                    reachable.getOrElseUpdate(r2, mutable.HashSet.empty[Relation]).add(r1)
                    inDegree(r1) = inDegree(r1) + 1
                } else if (r2.getPrimaryKeys().subsetOf(overlap)) {
                    reachable.getOrElseUpdate(r1, mutable.HashSet.empty[Relation]).add(r2)
                    inDegree(r2) = inDegree(r2) + 1
                }
            }
        }

        if (inDegree.count(t => t._2 == 0) != 1) {
            // cond 2 unsatisfied
            return None
        }

        val fact = inDegree.find(t => t._2 == 0).get._1
        val queue = mutable.Queue.empty[Relation]
        val seen = mutable.Set.empty[Relation]
        val meet = mutable.Set.empty[Relation]
        queue.enqueue(fact)

        while (queue.nonEmpty) {
            val head = queue.dequeue()
            if (!seen.contains(head)) {
                seen.add(head)
                if (reachable.contains(head)) {
                    reachable(head).foreach(r => queue.enqueue(r))
                }
            } else {
                meet.add(head)
            }
        }

        if (seen.size < relationalHyperGraph.getEdges().size) {
            // cond 3 unsatisfied
            return None
        }

        if (meet.size > 1 || meet.isEmpty) {
            // currently, only one meet is supported
            return None
        }

        val parents = relationalHyperGraph.getEdges().filter(r => reachable.contains(r) && reachable(r).contains(meet.head))
        val results = parents.map(p => {
            // keep the join between p and meet
            // break the join between other relations and meet by replacing the join variable with a dummy variable
            // each choice of p results in a different relationalHyperGraph and extraEqualConditions
            val others = parents - p
            val meetVariables = meet.head.getVariableList().toSet
            var updatedHyperGraph = relationalHyperGraph
            val extraEqualConditions = ListBuffer.empty[ExtraCondition]

            others.foreach(o => {
                updatedHyperGraph = updatedHyperGraph.removeHyperEdge(o)
                val intersect = meetVariables.intersect(o.getVariableList().toSet)
                val replace = intersect.map(v => (v, variableManager.getNewVariable(v.dataType)))
                val newHyperEdge = o.replaceVariables(replace.toMap)
                updatedHyperGraph = updatedHyperGraph.addHyperEdge(newHyperEdge)
                extraEqualConditions.appendAll(replace.map(t => ExtraEqualToCondition(SingleVariableExpression(t._1), SingleVariableExpression(t._2))))
            })

            (updatedHyperGraph, extraEqualConditions.toList)
        })
        Some(results.toList)
    }
}
