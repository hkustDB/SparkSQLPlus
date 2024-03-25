package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{ComparisonHyperGraph, JoinTree}

import scala.collection.mutable.ListBuffer

case class RunResult(candidates: List[(JoinTree, ComparisonHyperGraph, List[ExtraCondition])],
                     outputVariables: List[Variable], computations: List[(Variable, Expression)], isFull: Boolean, isFreeConnex: Boolean,
                     groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])],
                     optTopK: Option[TopK])

object RunResult {
    def buildFromSingleResult(result: (JoinTree, ComparisonHyperGraph, List[ExtraCondition]),
                              outputVariables: List[Variable], computations: List[(Variable, Expression)], isFull: Boolean, isFreeConnex: Boolean,
                              groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])],
                              optTopK: Option[TopK]): RunResult = {
        RunResult(List(result), outputVariables, computations, isFull, isFreeConnex, groupByVariables, aggregations, optTopK)
    }

    def sample(result: RunResult, limit: Int, sampleSize: Int): RunResult = {
        if (result.candidates.size <= limit) {
            result
        } else {
            assert(sampleSize <= limit)
            val keep = limit - sampleSize
            val r = ListBuffer.empty[(JoinTree, ComparisonHyperGraph, List[ExtraCondition])]
            r.appendAll(result.candidates.take(keep))

            val step = (result.candidates.size - keep) / (sampleSize)
            for (i <- keep.until(result.candidates.size, step)) {
                r.append(result.candidates(i))
            }

            val candidates = r.take(limit).toList
            RunResult(candidates, result.outputVariables, result.computations, result.isFull, result.isFreeConnex,
                result.groupByVariables, result.aggregations, result.optTopK)
        }
    }
}
