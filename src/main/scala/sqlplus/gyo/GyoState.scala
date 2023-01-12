package sqlplus.gyo

import sqlplus.graph.RelationalHyperGraph

class GyoState(private val graph: RelationalHyperGraph, private val forest: Forest) {
    val graphSignature = graph.getSignature()
    val forestSignature = forest.getSignature()

    def getRelationalHyperGraph: RelationalHyperGraph = graph

    def getForest: Forest = forest

    override def hashCode(): Int = (graphSignature, forestSignature).##

    override def equals(obj: Any): Boolean = {
        obj match {
            case gs: GyoState => gs.graphSignature == graphSignature && gs.forestSignature == forestSignature
            case _ => false
        }
    }
}
