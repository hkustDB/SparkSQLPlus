package sqlplus.compile

import scala.collection.mutable

class RelationVariableNamesManager {
    private val source: mutable.HashMap[String, String] = mutable.HashMap.empty
    private val raw: mutable.HashMap[Int, String] = mutable.HashMap.empty
    private val keyed: mutable.HashMap[Int, (List[Int], String)] = mutable.HashMap.empty
    private val grouped: mutable.HashMap[Int, (List[Int], String)] = mutable.HashMap.empty

    def getSourceRelation(tableName: String): String = {
        source(tableName)
    }

    def getRawRelation(relationId: Int): String = {
        raw(relationId)
    }

    def getKeyByAnyKeyRelation(relationId: Int): Option[String] = {
        keyed.get(relationId).map(t => t._2)
    }

    def getKeyByRelation(relationId: Int, keyByIndices: List[Int]): Option[String] = {
        keyed.get(relationId).filter(t => t._1 == keyByIndices).map(t => t._2)
    }

    def getGroupByRelation(relationId: Int, groupByIndices: List[Int]): Option[String] = {
        grouped.get(relationId).filter(t => t._1 == groupByIndices).map(t => t._2)
    }

    private def clear(relationId: Int): Unit = {
        keyed.remove(relationId)
        grouped.remove(relationId)
    }

    def setSourceRelation(tableName: String, variableName: String): Unit = {
        assert(!source.contains(tableName))
        source.put(tableName, variableName)
    }

    def setRawRelation(relationId: Int, variableName: String): Unit = {
        assert(!raw.contains(relationId))
        raw.put(relationId, variableName)
    }

    def setKeyByRelation(relationId: Int, keyByIndices: List[Int], variableName: String): Unit = {
        clear(relationId)
        keyed.put(relationId, (keyByIndices, variableName))
    }

    // update the variable name for given relation without changing the keyIndices
    // this is useful in appending computation extra column
    def updateKeyByRelation(relationId: Int, variableName: String): Unit = {
        val (keyIndices, _) = keyed(relationId)
        keyed.put(relationId, (keyIndices, variableName))
    }

    def setGroupByRelation(relationId: Int, groupByIndices: List[Int], variableName: String): Unit = {
        grouped.put(relationId, (groupByIndices, variableName))
    }
}
