package sqlplus.compile

import sqlplus.types.DataType

sealed trait SetupAction

case class ReadSourceTableAction(variableName: String, path: String, columnIndices: List[Int], columnTypes: List[DataType]) extends SetupAction
case class ComputeAggregatedRelationAction(variableName: String, fromVariableName: String, groupIndices: List[Int], aggregateFunction: String) extends SetupAction
case class FunctionDefinitionAction(func: List[String]) extends SetupAction

sealed trait ReduceAction
case class KeyByAction(variableName: String, fromVariableName: String, keyIndices: List[Int], keyExtractFuncs: List[String => String], isReKey: Boolean) extends ReduceAction

case class GroupByAction(variableName: String, fromVariableName: String) extends ReduceAction

case class CreateCommonExtraColumnAction(columnVariableName: String, sortedVariableName: String, fromVariableName: String,
                                         compareKeyIndex: Int, compareFunc: String, typeParameters: String) extends ReduceAction
case class CreateTransparentCommonExtraColumnAction(columnVariableName: String, fromVariableName: String,
                                                    joinKeyIndices: List[Int], joinKeyExtractFuncs: List[String => String]) extends ReduceAction
case class CreateComparisonExtraColumnAction(columnVariableName: String, treeLikeArrayVariableName: String, fromVariableName: String,
                                             compareKeyIndex1: Int, compareKeyIndex2: Int, compareFunc1: String,
                                             compareFunc2: String, typeParameters: String) extends ReduceAction
case class CreateComputationExtraColumnAction(resultVariableName: String, fromVariableName: String,
                                              functionGenerator: String => String) extends ReduceAction
case class AppendCommonExtraColumnAction(appendedVariableName: String, fromVariableName: String, columnVariableName: String) extends ReduceAction
case class AppendComparisonExtraColumnAction(appendedVariableName: String, fromVariableName: String, columnVariableName: String,
                                             compareKeyIndex: Int, compareFunc: String, typeParameters: String) extends ReduceAction
case class ApplySemiJoinAction(variableName: String, fromVariableName: String, childVariableName: String) extends ReduceAction
case class ApplySelfComparisonAction(variableName: String, fromVariableName: String, functionGenerator: String => String) extends ReduceAction

case class MaterializeBagRelationAction(variableName: String, tableScanRelationVariableNames: String,
                                        relationCount: Int, variableCount: Int, sourceTableIndexToRelations: String, redirects: String, variableIndices: String) extends ReduceAction

case class MaterializeAuxiliaryRelationAction(variableName: String, supportingVariableName: String, projectIndices: List[Int]) extends ReduceAction

sealed trait EnumerateAction
case class RootPrepareEnumerationAction(variableName: String, fromVariableName: String,
                                        joinKeyIndices: List[Int], joinKeyExtractFuncs: List[String => String],
                                        extractIndicesInCurrent: List[Int]) extends EnumerateAction
case class EnumerateWithoutComparisonAction(newVariableName: String, currentVariableName: String, intermediateResultVariableName: String,
                                            extractIndicesInCurrent: List[Int],
                                            extractIndicesInIntermediateResult: List[Int],
                                            resultKeySelectors: List[(String, String) => String]
                                           ) extends EnumerateAction
case class EnumerateWithOneComparisonAction(newVariableName: String, currentVariableName: String, intermediateResultVariableName: String,
                                            compareKeyIndexInCurrent: Int, compareKeyIndexInIntermediateResult: Int,
                                            compareFunc: String,
                                            extractIndicesInCurrent: List[Int],
                                            extractIndicesInIntermediateResult: List[Int],
                                            resultKeySelectors: List[(String, String) => String],
                                            typeParameters: String
                                           ) extends EnumerateAction
case class EnumerateWithTwoComparisonsAction(newVariableName: String, currentVariableName: String, intermediateResultVariableName: String,
                                             compareKeyIndexInIntermediateResult1: Int,
                                             compareKeyIndexInIntermediateResult2: Int,
                                             extractIndicesInCurrent: List[Int],
                                             extractIndicesInIntermediateResult: List[Int],
                                             resultKeySelectors: List[(String, String) => String],
                                             typeParameters: String
                                            ) extends EnumerateAction

case class EnumerateWithMoreThanTwoComparisonsAction(newVariableName: String, currentVariableName: String, intermediateResultVariableName: String,
                                                     compareKeyIndexInCurrent: Int,
                                                     compareKeyIndexInIntermediateResult: Int,
                                                     compareFunc: String, extraFilters: List[String],
                                                     extractIndicesInCurrent: List[Int],
                                                     extractIndicesInIntermediateResult: List[Int],
                                                     resultKeySelectors: List[(String, String) => String],
                                                     typeParameters: String
                                                    ) extends EnumerateAction

case class FormatResultAction(variableName: String, intermediateResultVariableName: String, formatters: List[String => String]) extends EnumerateAction
case class CountResultAction(intermediateResultVariableName: String) extends EnumerateAction