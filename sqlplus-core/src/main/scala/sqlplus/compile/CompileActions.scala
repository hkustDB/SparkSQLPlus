package sqlplus.compile

import sqlplus.expression.Variable

sealed trait ExtraColumn {
    def getVariable(): Variable
}
case class CommonExtraColumn(columnVariable: Variable, joinVariables: List[Variable]) extends ExtraColumn {
    override def getVariable(): Variable = columnVariable
}
case class ComparisonExtraColumn(columnVariable: Variable, joinVariables: List[Variable], comparisonInfo: ComparisonInfo) extends ExtraColumn {
    override def getVariable(): Variable = columnVariable
}

sealed trait ReduceAction
case class CreateCommonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int], compareKeyIndex: Int, func: String) extends ReduceAction
case class CreateTransparentCommonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int]) extends ReduceAction
case class CreateComparisonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int], compareKeyIndex1: Int, compareKeyIndex2: Int, func1: String, func2: String) extends ReduceAction
case class CreateComputationExtraColumnAction(relationId: Int, columnVariable: Variable, keyIndices: List[Int], functionGenerator: String => String) extends ReduceAction
case class AppendCommonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int]) extends ReduceAction
case class AppendComparisonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int], compareKeyIndex: Int, func: String) extends ReduceAction
case class ApplySemiJoinAction(currentRelationId: Int, childRelationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyIndicesInChild: List[Int]) extends ReduceAction
case class ApplySelfComparisonAction(relationId: Int, keyIndices: List[Int], functionGenerator: String => String) extends ReduceAction
case class EndOfReductionAction(relationId: Int) extends ReduceAction

sealed trait EnumerateAction
case class EnumerateWithoutComparisonAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyIndicesInIntermediateResult: List[Int], extractIndicesInCurrent: List[Int], extractIndicesInIntermediateResult: List[Int]) extends EnumerateAction
case class EnumerateWithOneComparisonAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyIndicesInIntermediateResult: List[Int], compareKeyIndexInCurrent: Int, compareKeyIndexInIntermediateResult: Int, func: String, extractIndicesInCurrent: List[Int], extractIndicesInIntermediateResult: List[Int]) extends EnumerateAction
case class EnumerateWithTwoComparisonsAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyIndicesInIntermediateResult: List[Int], compareKeyIndexInIntermediateResult1: Int, compareKeyIndexInIntermediateResult2: Int, extractIndicesInCurrent: List[Int], extractIndicesInIntermediateResult: List[Int]) extends EnumerateAction
case class FinalOutputAction(outputVariableIndices: List[Int]) extends EnumerateAction