package sqlplus.ghd

import sqlplus.graph.{Relation}

class GhdNode(val relations: Set[Relation], val childrenNode: Set[GhdNode], val assigner: GhdScoreAssigner) {
    // score is composed of 2 parts, the solution of the LP, and the number of levels and the number of descendants + 1
    lazy val score: (Double, Int) = {
        val sln = assigner.assign(this)
        if (childrenNode.nonEmpty) {
            val childrenScores = childrenNode.map(c => c.score)
            val maxSln = sln.max(childrenScores.map(t => t._1).max)
            val cnt = childrenScores.map(t => t._2).sum + 1
            (maxSln, cnt)
        } else {
            (sln, 1)
        }
    }
}
