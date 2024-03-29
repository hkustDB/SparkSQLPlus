package sqlplus.graph

import sqlplus.expression.Variable

/**
 * A relation is a hyperEdge whose nodes are variables.
 */
abstract class Relation extends HyperEdge[Variable] {
    val relationId = Relation.getNewRelationId()

    final def getRelationId(): Int = relationId

    def getTableName(): String

    def getTableDisplayName(): String

    def removeVariables(variables: Set[Variable]): Relation =
        AuxiliaryRelation.createFrom(this, getVariableList().filterNot(v => variables.contains(v)))

    def project(variables: Set[Variable]): Relation =
        AuxiliaryRelation.createFrom(this, getVariableList().filter(v => variables.contains(v)))

    override def hashCode(): Int = getRelationId().##

    override def equals(obj: Any): Boolean = obj match {
        case that: Relation => that.getRelationId() == this.getRelationId()
        case _ => false
    }

    final override def getNodes(): Set[Variable] = {
        getVariableList().toSet
    }

    def getVariableList(): List[Variable]

    def getPrimaryKeys(): Set[Variable]

    def replaceVariables(map: Map[Variable, Variable]): Relation

    def getCardinality(): Long
}

object Relation {
    var ID = 0

    def getNewRelationId(): Int = {
        ID += 1
        ID
    }
}
