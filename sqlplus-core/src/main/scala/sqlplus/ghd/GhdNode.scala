package sqlplus.ghd

import sqlplus.graph.Relation

class GhdNode(val relations: Set[Relation], val childrenNode: Set[GhdNode]) {
    lazy val score: Double =
        GhdScoreAssigner.assign(this).max(if (childrenNode.nonEmpty) childrenNode.toList.map(n => n.score).max else 0)
}
