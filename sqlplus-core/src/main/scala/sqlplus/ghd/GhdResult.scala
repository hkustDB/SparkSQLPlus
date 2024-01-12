package sqlplus.ghd

import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GhdResult(candidates: List[(JoinTree, RelationalHyperGraph)])