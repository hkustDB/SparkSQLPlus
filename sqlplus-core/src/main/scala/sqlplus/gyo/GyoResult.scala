package sqlplus.gyo

import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GyoResult(joinTreeWithHyperGraphs: List[(JoinTree, RelationalHyperGraph)])
