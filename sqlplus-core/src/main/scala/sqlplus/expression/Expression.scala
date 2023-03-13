package sqlplus.expression

import sqlplus.types.{DataType, DoubleDataType, IntDataType, IntervalDataType, LongDataType, StringDataType, TimestampDataType}

sealed trait Expression {
    def getType(): DataType
    def getVariables(): Set[Variable]
}

sealed trait ComputeExpression extends Expression {
    def getComputeFunction(variables: List[Variable], cast: Boolean = false): String => String
}

sealed trait LiteralExpression extends Expression {
    override def getVariables(): Set[Variable] = Set()
    def getLiteral(): String
}

case class SingleVariableExpression(variable: Variable) extends ComputeExpression {
    override def getType(): DataType = variable.dataType

    override def getVariables(): Set[Variable] = Set(variable)

    override def getComputeFunction(variables: List[Variable], cast: Boolean = false): String => String = x => {
        val raw = x + "(" + variables.indexOf(variable) + ")"
        if (cast) getType().castFromAny(raw) else raw
    }
}

abstract class BinaryExpression(left: Expression, right: Expression, operator: String, returnType: DataType) extends ComputeExpression{
    override def getType(): DataType = returnType

    override def getVariables(): Set[Variable] = left.getVariables() ++ right.getVariables()

    override def getComputeFunction(variables: List[Variable], cast: Boolean = false): String => String = x => {
        val leftOperand = left match {
            case v: ComputeExpression => v.getComputeFunction(variables, true)(x)
            case l: LiteralExpression => l.getLiteral()
        }
        val rightOperand = right match {
            case v: ComputeExpression => v.getComputeFunction(variables, true)(x)
            case l: LiteralExpression => l.getLiteral()
        }
        val raw = s"($leftOperand $operator $rightOperand)"
        if (cast) getType().castFromAny(raw) else raw
    }
}

case class IntPlusIntExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", IntDataType)
case class LongPlusLongExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", LongDataType)
case class TimestampPlusIntervalExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", TimestampDataType)
case class DoublePlusDoubleExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "+", DoubleDataType)

case class IntTimesIntExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "*", IntDataType)
case class LongTimesLongExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "*", LongDataType)
case class DoubleTimesDoubleExpression(left: Expression, right: Expression) extends BinaryExpression(left, right, "*", DoubleDataType)

case class StringLiteralExpression(lit: String) extends LiteralExpression {
    override def getLiteral(): String = "\"" + lit + "\""

    override def getType(): DataType = StringDataType
}

case class IntLiteralExpression(lit: Int) extends LiteralExpression {
    override def getLiteral(): String = s"$lit"

    override def getType(): DataType = IntDataType
}

case class DoubleLiteralExpression(lit: Double) extends LiteralExpression {
    override def getLiteral(): String = s"${lit}d"

    override def getType(): DataType = DoubleDataType
}

case class IntervalLiteralExpression(lit: Long) extends LiteralExpression {
    val ms = s"${lit}L"
    override def getLiteral(): String = ms

    override def getType(): DataType = IntervalDataType
}
