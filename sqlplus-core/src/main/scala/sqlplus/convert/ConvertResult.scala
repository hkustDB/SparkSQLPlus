package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{ComparisonHyperGraph, JoinTree}

case class ConvertResult(candidates: List[(JoinTree, ComparisonHyperGraph, List[ExtraCondition])],
                         outputVariables: List[Variable], computations: List[(Variable, Expression)], isFull: Boolean,
                         groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])],
                         optTopK: Option[TopK])

object ConvertResult {
    def buildFromSingleResult(result: (JoinTree, ComparisonHyperGraph, List[ExtraCondition]),
                              outputVariables: List[Variable], computations: List[(Variable, Expression)], isFull: Boolean,
                              groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])],
                              optTopK: Option[TopK]): ConvertResult = {
        ConvertResult(List(result), outputVariables, computations, isFull, groupByVariables, aggregations, optTopK)
    }
}
