package sqlplus.convert

import sqlplus.expression.Variable
import sqlplus.graph.{ComparisonHyperGraph, JoinTree, Relation}

case class ConvertResult(relations: List[Relation], outputVariables: List[Variable], joinTree: JoinTree, comparisonHyperGraph: ComparisonHyperGraph)
