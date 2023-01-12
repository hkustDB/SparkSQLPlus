package sqlplus.graph

import sqlplus.expression.Variable

class TableScanRelation(val tableName: String, val variables: List[Variable], val tableDisplayName: String) extends Relation {
    def getTableName(): String = tableName

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"TableScanRelation[id=${getRelationId()}][source=$tableName][cols=$columns]"
    }

    override def getTableDisplayName(): String = tableDisplayName
}
