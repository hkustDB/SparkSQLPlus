package sqlplus.convert

import sqlplus.ghd.GhdResult
import sqlplus.graph.{JoinTree, RelationalHyperGraph}
import sqlplus.gyo.GyoResult

case class HandleResult(result: List[(JoinTree, RelationalHyperGraph, List[ExtraCondition])])

object HandleResult {
    def merge(r1: HandleResult, r2: HandleResult): HandleResult = {
        HandleResult(r1.result ++ r2.result)
    }

    def fromGyoResult(gyoResult: GyoResult): HandleResult = {
        HandleResult(gyoResult.candidates.map(t => (t._1, t._2, List.empty)))
    }

    def fromGhdResult(ghdResult: GhdResult): HandleResult = {
        HandleResult(ghdResult.candidates.map(t => (t._1, t._2, List.empty)))
    }

    def appendExtraConditions(handleResult: HandleResult, extraConditions: List[ExtraCondition]): HandleResult = {
        HandleResult(handleResult.result.map(t => (t._1, t._2, t._3 ++ extraConditions)))
    }
}