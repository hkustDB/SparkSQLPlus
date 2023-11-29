package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{ComparisonHyperGraph, JoinTree}

case class ConvertResult(joinTree: JoinTree, comparisonHyperGraph: ComparisonHyperGraph,
                         outputVariables: List[Variable],
                         groupByVariables: List[Variable], aggregations: List[(String, List[Expression], Variable)])
