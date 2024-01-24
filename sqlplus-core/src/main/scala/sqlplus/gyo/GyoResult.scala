package sqlplus.gyo

import sqlplus.expression.Variable
import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GyoResult(candidates: List[(JoinTree, RelationalHyperGraph)], isFreeConnex: Boolean, extraEqualConditions: List[(Variable, Variable)])
