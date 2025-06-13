package sqlplus.convert

import sqlplus.ghd.GhdAlgorithm
import sqlplus.graph.RelationalHyperGraph

class UnbreakableCyclicHandler(ghd: GhdAlgorithm) {
    def handle(context: Context, relationalHyperGraph: RelationalHyperGraph): HandleResult = {
        val requiredVariables = context.requiredVariables
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables

        HandleResult.fromGhdResult(ghd.run(relationalHyperGraph, topVariables))
    }
}
