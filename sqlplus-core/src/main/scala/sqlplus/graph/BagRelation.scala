package sqlplus.graph

import sqlplus.expression.Variable

class BagRelation(bag: Set[Relation]) extends Relation {
    val inside: List[Relation] = bag.toList.sortBy(r => r.getRelationId())

    val variableList: List[Variable] = inside.flatMap(r => r.getNodes()).distinct.sortBy(v => v.name)

    override def getTableName(): String = inside.map(r => r.getTableName()).mkString("Bag(", ",", ")")

    override def getTableDisplayName(): String = s"bag${relationId}"

    override def getVariableList(): List[Variable] = variableList

    def getInternalRelations: List[Relation] = inside
}

object BagRelation {
    def createFrom(relations: Set[Relation]): BagRelation =
        new BagRelation(relations)
}
