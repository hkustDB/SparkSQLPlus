package sqlplus.graph

import sqlplus.expression.Variable

class AuxiliaryRelation(val tableName: String, val variables: List[Variable], val supportingRelation: Relation, val tableDisplayName: String, val primaryKeys: Set[Variable]) extends Relation {
    override def getTableName(): String = tableName

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"AuxiliaryRelation[id=${getRelationId()}][source=$tableName][cols=$columns]"
    }

    override def getTableDisplayName(): String = tableDisplayName

    override def getPrimaryKeys(): Set[Variable] = primaryKeys

    override def replaceVariables(map: Map[Variable, Variable]): Relation = throw new UnsupportedOperationException()
}

object AuxiliaryRelation {
    /**
     * create a AuxiliaryRelation from the given relation.
     * @param relation the relation
     * @param remainVariables the remaining variables
     * @return a new AuxiliaryRelation
     */
    def createFrom(supportingRelation: Relation, remainVariables: List[Variable]): AuxiliaryRelation = {
        val name = s"[${supportingRelation.getTableName()}]"
        val displayName = s"[${supportingRelation.getTableDisplayName()}]"
        assert(remainVariables.forall(v => supportingRelation.getNodes().contains(v)))
        val primaryKeys: Set[Variable] = if (supportingRelation.getPrimaryKeys().subsetOf(remainVariables.toSet)) supportingRelation.getPrimaryKeys() else Set.empty
        new AuxiliaryRelation(name, remainVariables, supportingRelation, displayName, primaryKeys)
    }
}
