package sqlplus.ghd

import org.apache.commons.math3.optim.linear.{LinearConstraint, LinearConstraintSet, LinearObjectiveFunction, NonNegativeConstraint, Relationship, SimplexSolver}
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import sqlplus.expression.Variable
import sqlplus.graph.Relation

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class GhdScoreAssigner {
    private val dict = mutable.HashMap.empty[Set[Relation], Double]

    def clear(): Unit =
        dict.clear()

    private def loop(effective: ListBuffer[(Set[Variable], Set[Variable])]): Unit = {
        val size = effective.size
        for (i <- 0.until(size - 1)) {
            for (j <- (i + 1).until(size)) {
                val e1 = effective(i)
                val e2 = effective(j)
                if (e2._1 subsetOf e1._2) {
                    // e2.pk is in e1.cols
                    // add e2.cols into e1.cols
                    if (!e2._2.subsetOf(e1._2)) {
                        effective(i) = (e1._1, e1._2.union(e2._2))
                        loop(effective)
                        return
                    }
                } else if (e1._1 subsetOf e2._2) {
                    // e1.pk is in e2.cols
                    // add e1.cols into e2.cols
                    if (!e1._2.subsetOf(e2._2)) {
                        effective(j) = (e2._1, e2._2.union(e1._2))
                        loop(effective)
                        return
                    }
                }
            }
        }
    }

    def assign(node: GhdNode): Double = {
        if (dict.contains(node.relations)) {
            dict(node.relations)
        } else {
            val relations = node.relations.toList
            val effective = mutable.ListBuffer.empty[(Set[Variable], Set[Variable])]
            relations.foreach(r => effective.append(
                (if (r.getPrimaryKeys().nonEmpty) r.getPrimaryKeys() else r.getVariableList().toSet,
                    r.getVariableList().toSet)))
            loop(effective)

            val variables = effective.flatMap(e => e._2).distinct.toList

            val objectiveFunction = new LinearObjectiveFunction(Array.fill(effective.size)(1.0), 0)
            val constraints = new util.ArrayList[LinearConstraint]
            variables.foreach(v => {
                val row = Array.tabulate(effective.size)(i => if (effective(i)._2.contains(v)) 1.0 else 0.0)
                constraints.add(new LinearConstraint(row, Relationship.GEQ,  1.0))
            })
            val constraintSet = new LinearConstraintSet(constraints)
            val solver = new SimplexSolver
            val solution = solver.optimize(objectiveFunction,
                constraintSet, GoalType.MINIMIZE, new NonNegativeConstraint(true))
            val sln = solution.getValue
            dict(node.relations) = sln
            sln
        }
    }
}
