package sqlplus.graph

import sqlplus.expression.Variable

class BagRelation(bag: Set[Relation]) extends Relation {
    val inside: List[Relation] = bag.toList.sortBy(r => r.getRelationId())

    val variableList: List[Variable] = inside.flatMap(r => r.getNodes()).distinct.sortBy(v => v.name)

    override def getTableName(): String = inside.map(r => r.getTableName()).mkString("Bag(", ",", ")")

    override def getTableDisplayName(): String = s"bag${relationId}"

    override def getVariableList(): List[Variable] = variableList

    def getInternalRelations: List[Relation] = inside

    override def toString: String = {
        val internal = inside.map(r => r.getTableDisplayName()).mkString(",")
        val columns = variableList.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"BagRelation[id=${getRelationId()}][internal=$internal][cols=$columns]"
    }

    override def getPrimaryKeys(): Set[Variable] = Set.empty

    override def replaceVariables(map: Map[Variable, Variable]): Relation = throw new UnsupportedOperationException()

    override def getCardinality(): Long = 0
}

object BagRelation {
    def createFrom(relations: Set[Relation]): BagRelation = {
        // we don't allow nested bag relations
        assert(relations.forall(r => !r.isInstanceOf[BagRelation]))
        new BagRelation(relations)
    }
}
