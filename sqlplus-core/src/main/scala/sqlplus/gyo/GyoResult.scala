package sqlplus.gyo

import sqlplus.convert.ExtraCondition
import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GyoResult(candidates: List[(JoinTree, RelationalHyperGraph)], isFreeConnex: Boolean, extraEqualConditions: List[ExtraCondition])
