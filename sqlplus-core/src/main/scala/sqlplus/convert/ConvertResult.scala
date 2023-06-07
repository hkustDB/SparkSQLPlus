package sqlplus.convert

import sqlplus.expression.Variable
import sqlplus.graph.{ComparisonHyperGraph, JoinTree, Relation}

case class ConvertResult(outputVariables: List[Variable], joinTree: JoinTree, comparisonHyperGraph: ComparisonHyperGraph)
