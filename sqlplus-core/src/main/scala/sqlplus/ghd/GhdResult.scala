package sqlplus.ghd

import sqlplus.graph.{JoinTree, RelationalHyperGraph}

case class GhdResult(joinTreeWithHyperGraphs: List[(JoinTree, RelationalHyperGraph)])