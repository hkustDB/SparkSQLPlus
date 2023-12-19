package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{ComparisonHyperGraph, JoinTree}

case class ConvertResult(joinTree: JoinTree, comparisonHyperGraph: ComparisonHyperGraph,
                         outputVariables: List[Variable], computations: List[(Variable, Expression)],
                         groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])])
