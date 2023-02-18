package sqlplus.ghd

import org.apache.commons.math3.optim.linear.{LinearConstraint, LinearConstraintSet, LinearObjectiveFunction, NonNegativeConstraint, Relationship, SimplexSolver}
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import sqlplus.graph.Relation

import java.util
import scala.collection.mutable

object GhdScoreAssigner {
    private val dict = mutable.HashMap.empty[Set[Relation], Double]

    def clear(): Unit =
        dict.clear()

    def assign(node: GhdNode): Double = {
        if (dict.contains(node.relations)) {
            dict(node.relations)
        } else {
            val relations = node.relations.toList
            val variables = node.relations.flatMap(r => r.getVariableList()).toList

            val objectiveFunction = new LinearObjectiveFunction(Array.fill(relations.size)(1.0), 0)
            val constraints = new util.ArrayList[LinearConstraint]
            variables.foreach(v => {
                val row = Array.tabulate(relations.size)(i => if (relations(i).getVariableList().contains(v)) 1.0 else 0.0)
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
