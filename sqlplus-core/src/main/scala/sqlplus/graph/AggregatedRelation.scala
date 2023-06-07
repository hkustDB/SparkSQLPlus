package sqlplus.graph

import sqlplus.expression.Variable

// TODO: support aggregation over not only table scan relations
class AggregatedRelation(val tableName: String, val variables: List[Variable],
                         val group: List[Int], val func: String, val tableDisplayName: String) extends Relation {

    override def getTableName(): String = tableName

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        val groups = group.mkString("(", ",", ")")
        s"AggregatedRelation[id=${getRelationId()}][source=$tableName][cols=$columns][group=$groups][func=$func]"
    }

    override def getTableDisplayName(): String = tableDisplayName
}
