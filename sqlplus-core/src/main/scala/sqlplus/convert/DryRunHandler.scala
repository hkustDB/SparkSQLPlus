package sqlplus.convert

import sqlplus.graph.RelationalHyperGraph
import sqlplus.gyo.GyoAlgorithm

class DryRunHandler(gyo: GyoAlgorithm) {
    def handle(context: Context): Option[HandleResult] = {
        val relations = context.relations
        val requiredVariables = context.requiredVariables
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        gyo.dryRun(relationalHyperGraph, topVariables).map(HandleResult.fromGyoResult)
    }
}
