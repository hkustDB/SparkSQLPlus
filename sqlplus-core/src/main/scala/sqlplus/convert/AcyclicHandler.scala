package sqlplus.convert

import sqlplus.expression.Variable
import sqlplus.graph.{Relation, RelationalHyperGraph}
import sqlplus.gyo.GyoAlgorithm

import scala.collection.mutable

class AcyclicHandler(val gyo: GyoAlgorithm) {
    def handle(context: Context): HandleResult = {
        val relations = context.relations
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        handle(context, relationalHyperGraph)
    }

    def handle(context: Context, relationalHyperGraph: RelationalHyperGraph): HandleResult = {
        val requiredVariables = context.requiredVariables
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations

        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables

        val gyoResult = gyo.run(relationalHyperGraph, topVariables)
        val handleResult = HandleResult.fromGyoResult(gyoResult)

        val fixRootRelations = tryFixRoot(relationalHyperGraph, aggregations.nonEmpty, groupByVariables.toSet)
        fixRootRelations.foldLeft(handleResult)((z, r) => {
            HandleResult.merge(z, HandleResult.fromGyoResult(gyo.runWithFixRoot(relationalHyperGraph, r)))
        })
    }

    def tryFixRoot(relationalHyperGraph: RelationalHyperGraph, isAggregation: Boolean, groupByVariables: Set[Variable]): List[Relation] = {
        // candidate relation(s): size >= factor * largest relation size
        val factor = 0.8

        // We can fix the root of a query if:
        // 1. The query is an aggregation query.
        // 2. All relations have a cardinality greater than zero.
        // 3. All relations have a non-empty primary key.
        // 4. The candidate relation does not include any group-by variables
        //    (if the largest relation contains group-by variables, GYO will handle it without fixing the root).
        // 5. The candidate relation's variables determine the group-by variables.
        if (!isAggregation || relationalHyperGraph.getEdges().exists(r => r.getCardinality() <= 0 || r.getPrimaryKeys().isEmpty))
            return List()

        val largestCardinality = relationalHyperGraph.getEdges().map(r => r.getCardinality()).max
        val largestRelations = relationalHyperGraph.getEdges().filter(r => r.getCardinality() >= largestCardinality * factor)

        val candidateRelations = largestRelations.filter(r => r.getNodes().intersect(groupByVariables).isEmpty)

        val chaseVariables = mutable.HashMap.empty[Relation, Set[Variable]]
        relationalHyperGraph.getEdges().foreach(r => {
            chaseVariables(r) = r.getNodes()
        })

        // TODO: refactor the loop()
        def loop(): Unit = {
            for {
                r1 <- relationalHyperGraph.getEdges()
                r2 <- relationalHyperGraph.getEdges()
                if r1 != r2
            } {
                if (r1.getPrimaryKeys().subsetOf(chaseVariables(r2)) && !chaseVariables(r1).subsetOf(chaseVariables(r2))) {
                    chaseVariables(r2) = chaseVariables(r2).union(chaseVariables(r1))
                    return loop()
                } else if (r2.getPrimaryKeys().subsetOf(chaseVariables(r1)) && !chaseVariables(r2).subsetOf(chaseVariables(r1))) {
                    chaseVariables(r1) = chaseVariables(r1).union(chaseVariables(r2))
                    return loop()
                }
            }
        }

        loop()
        candidateRelations.filter(r => groupByVariables.subsetOf(chaseVariables(r))).toList
    }
}
