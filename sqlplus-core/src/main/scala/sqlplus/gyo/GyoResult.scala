package sqlplus.gyo

import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GyoResult(candidates: List[(JoinTree, RelationalHyperGraph)])
