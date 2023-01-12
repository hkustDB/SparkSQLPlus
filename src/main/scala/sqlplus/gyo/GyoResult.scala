package sqlplus.gyo

import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GyoResult(joinTrees: List[JoinTree], hyperGraph: RelationalHyperGraph)
