package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{ComparisonHyperGraph, JoinTree}

case class RunResult(joinTreesWithComparisonHyperGraph: List[(JoinTree, ComparisonHyperGraph)],
                     outputVariables: List[Variable], isFull: Boolean,
                     groupByVariables: List[Variable], aggregations: List[(String, List[Expression], Variable)])
