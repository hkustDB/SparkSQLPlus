package sqlplus.graph

import sqlplus.expression.Variable

class TableScanRelation(val tableName: String, val variables: List[Variable], val tableDisplayName: String, val primaryKeys: Set[Variable]) extends Relation {
    def getTableName(): String = tableName

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"TableScanRelation[id=${getRelationId()}][source=$tableName][cols=$columns]"
    }

    override def getTableDisplayName(): String = tableDisplayName

    override def getPrimaryKeys(): Set[Variable] = primaryKeys

    override def replaceVariables(map: Map[Variable, Variable]): Relation = {
        val newVariables = variables.map(v => if (map.contains(v)) map(v) else v)
        new TableScanRelation(tableName, newVariables, tableDisplayName, primaryKeys)
    }
}
