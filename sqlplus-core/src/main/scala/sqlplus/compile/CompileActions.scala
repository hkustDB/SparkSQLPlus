package sqlplus.compile

import sqlplus.expression.Variable
import sqlplus.types.DataType

sealed trait ExtraColumn {
    def getVariable(): Variable
}
case class CommonExtraColumn(columnVariable: Variable, joinVariables: List[Variable]) extends ExtraColumn {
    override def getVariable(): Variable = columnVariable
}
case class ComparisonExtraColumn(columnVariable: Variable, joinVariables: List[Variable],
                                 comparisonInfo: ComparisonInfo) extends ExtraColumn {
    override def getVariable(): Variable = columnVariable
}

sealed trait ReduceAction
case class CreateCommonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int], joinKeyTypes: List[DataType],
                                         compareKeyIndex: Int, func: String, typeParameters: String) extends ReduceAction
case class CreateTransparentCommonExtraColumnAction(relationId: Int, extraColumnVariable: Variable,
                                                    joinKeyIndices: List[Int], joinKeyTypes: List[DataType]) extends ReduceAction
case class CreateComparisonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int], joinKeyTypes: List[DataType],
                                             compareKeyIndex1: Int, compareKeyIndex2: Int, func1: String,
                                             func2: String, typeParameters: String) extends ReduceAction
case class CreateComputationExtraColumnAction(relationId: Int, columnVariable: Variable, keyIndices: List[Int], keyTypes: List[DataType],
                                              functionGenerator: String => String) extends ReduceAction
case class AppendCommonExtraColumnAction(relationId: Int, extraColumnVariable: Variable,
                                         joinKeyIndices: List[Int], joinKeyTypes: List[DataType]) extends ReduceAction
case class AppendComparisonExtraColumnAction(relationId: Int, extraColumnVariable: Variable, joinKeyIndices: List[Int], joinKeyTypes: List[DataType],
                                             compareKeyIndex: Int, func: String, compareTypeParameter: String) extends ReduceAction
case class ApplySemiJoinAction(currentRelationId: Int, childRelationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyTypesInCurrent: List[DataType],
                               joinKeyIndicesInChild: List[Int], joinKeyTypesInChild: List[DataType]) extends ReduceAction
case class ApplySelfComparisonAction(relationId: Int, keyIndices: List[Int], keyTypes: List[DataType],
                                     functionGenerator: String => String) extends ReduceAction

case class MaterializeBagRelationAction(relationId: Int, tableScanRelationNames: List[String],
                                        relationCount: Int, variableCount: Int, sourceTableIndexToRelations: String, redirects: String, variableIndices: String) extends ReduceAction

case class MaterializeAuxiliaryRelationAction(relationId: Int, supportingRelationId: Int, projectIndices: List[Int], projectTypes: List[DataType]) extends ReduceAction

case class MaterializeAggregatedRelationAction(relationId: Int, tableName: String, groupIndices: List[Int], aggregateFunction: String) extends ReduceAction

case class EndOfReductionAction(relationId: Int) extends ReduceAction

sealed trait EnumerateAction
case class RootPrepareEnumerationAction(relationId: Int, joinKeyIndices: List[Int], joinKeyTypes: List[DataType],
                                        extractIndicesInCurrent: List[Int]) extends EnumerateAction
case class EnumerateWithoutComparisonAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyTypesInCurrent: List[DataType],
                                            extractIndicesInCurrent: List[Int],
                                            extractIndicesInIntermediateResult: List[Int],
                                            optResultKeyIsInIntermediateResultAndIndicesTypes: Option[List[(Boolean, Int, DataType)]]
                                           ) extends EnumerateAction
case class EnumerateWithOneComparisonAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyTypesInCurrent: List[DataType],
                                            compareKeyIndexInCurrent: Int, compareKeyIndexInIntermediateResult: Int,
                                            func: String, extractIndicesInCurrent: List[Int],
                                            extractIndicesInIntermediateResult: List[Int],
                                            optResultKeyIsInIntermediateResultAndIndicesTypes: Option[List[(Boolean, Int, DataType)]],
                                            typeParameters: String
                                           ) extends EnumerateAction
case class EnumerateWithTwoComparisonsAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyTypesInCurrent: List[DataType],
                                             compareKeyIndexInIntermediateResult1: Int,
                                             compareKeyIndexInIntermediateResult2: Int,
                                             extractIndicesInCurrent: List[Int],
                                             extractIndicesInIntermediateResult: List[Int],
                                             optResultKeyIsInIntermediateResultAndIndicesTypes: Option[List[(Boolean, Int, DataType)]],
                                             compareAndResultTypeParameters: String
                                            ) extends EnumerateAction

case class EnumerateWithMoreThanTwoComparisonsAction(relationId: Int, joinKeyIndicesInCurrent: List[Int], joinKeyTypesInCurrent: List[DataType],
                                                     compareKeyIndexInCurrent: Int, compareKeyIndexInIntermediateResult: Int,
                                                     func: String, extraFilters: List[String],
                                                     extractIndicesInCurrent: List[Int],
                                                     extractIndicesInIntermediateResult: List[Int],
                                                     optResultKeyIsInIntermediateResultAndIndicesTypes: Option[List[(Boolean, Int, DataType)]],
                                                     typeParameters: String
                                                    ) extends EnumerateAction

sealed trait FinalAction

case class FormatResultAction(formatters: List[String => String]) extends FinalAction
case object CountResultAction extends FinalAction