package sqlplus.graph

import AuxiliaryRelation.createFrom
import sqlplus.expression.Variable

class AuxiliaryRelation(val tableName: String, val variables: List[Variable], val sourceRelation: Relation, val tableDisplayName: String) extends Relation {
    override def getTableName(): String = tableName

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"AuxiliaryRelation[id=${getRelationId()}][source=$tableName][cols=$columns]"
    }

    override def getTableDisplayName(): String = tableDisplayName
}

object AuxiliaryRelation {
    /**
     * create a AuxiliaryRelation by removing some variables from the given relation.
     * @param relation the relation
     * @param remainVariables the remaining variables
     * @return a new AuxiliaryRelation
     */
    def createFrom(relation: Relation, remainVariables: List[Variable]): AuxiliaryRelation = {
        val name = s"^${relation.getTableName()}"
        val displayName = s"^${relation.getTableDisplayName()}"
        assert(remainVariables.forall(v => relation.getNodes().contains(v)))
        relation match {
            case relation: TableScanRelation => new AuxiliaryRelation(name, remainVariables, relation, displayName)
            case relation: AggregatedRelation => new AuxiliaryRelation(name, remainVariables, relation, displayName)
            case relation: AuxiliaryRelation => new AuxiliaryRelation(name, remainVariables, relation.sourceRelation, displayName)
            case relation: BagRelation => new AuxiliaryRelation(name, remainVariables, relation, displayName)
        }
    }
}
