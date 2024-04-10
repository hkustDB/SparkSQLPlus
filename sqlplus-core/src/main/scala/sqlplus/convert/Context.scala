package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.Relation

import scala.collection.mutable

class Context {
    var relations = List.empty[Relation]
    var conditions = List.empty[Condition]
    var outputVariables = List.empty[Variable]
    var requiredVariables = Set.empty[Variable]
    var computations = Map.empty[Variable, Expression]
    var isFull: Boolean = false
    var groupByVariables = List.empty[Variable]
    var aggregations = List.empty[(Variable, String, List[Expression])]
    var optTopK: Option[TopK] = None

    val dependingVariables = mutable.HashMap.empty[Variable, Set[Variable]]
}
