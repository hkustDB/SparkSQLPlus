package sqlplus.convert

import sqlplus.ghd.GhdAlgorithm
import sqlplus.graph.RelationalHyperGraph

class UnbreakableCyclicHandler(ghd: GhdAlgorithm) {
    def handle(context: Context, relationalHyperGraph: RelationalHyperGraph): HandleResult = {
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val topVariables = groupByVariables.toSet

        HandleResult.fromGhdResult(ghd.run(relationalHyperGraph, topVariables))
    }
}
