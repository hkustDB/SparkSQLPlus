package sqlplus.convert

import sqlplus.graph.RelationalHyperGraph

class BreakableCyclicHandler(acyclicHandler: AcyclicHandler) {
    def handle(context: Context, breakResult: List[(RelationalHyperGraph, List[ExtraCondition])]): HandleResult = {
        breakResult.map(t => {
            val hyperGraph = t._1
            val extraConditions = t._2
            HandleResult.appendExtraConditions(acyclicHandler.handle(context, hyperGraph), extraConditions)
        }).reduce(HandleResult.merge)
    }
}
