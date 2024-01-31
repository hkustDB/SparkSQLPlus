package sqlplus.convert

import sqlplus.expression.{Expression, LiteralExpression, StringLiteralExpression}

sealed trait Condition

case class LessThanCondition(leftOperand: Expression, rightOperand: Expression) extends Condition
case class LessThanOrEqualToCondition(leftOperand: Expression, rightOperand: Expression) extends Condition
case class GreaterThanCondition(leftOperand: Expression, rightOperand: Expression) extends Condition
case class GreaterThanOrEqualToCondition(leftOperand: Expression, rightOperand: Expression) extends Condition

case class EqualToLiteralCondition(operand: Expression, literal: LiteralExpression) extends Condition {
    override def toString: String = s"(${operand.format()} = ${literal.format()})"
}
case class NotEqualToLiteralCondition(operand: Expression, literal: LiteralExpression) extends Condition
case class LikeCondition(operand: Expression, s: StringLiteralExpression, isNeg: Boolean) extends Condition
case class InCondition(operand: Expression, literals: List[LiteralExpression], isNeg: Boolean) extends Condition

sealed trait ExtraCondition extends Condition
case class OrCondition(conditions: List[Condition]) extends ExtraCondition {
    override def toString: String = conditions.map(c => c.toString).mkString("(", " OR ", ")")
}

case class AndCondition(conditions: List[Condition]) extends ExtraCondition {
    override def toString: String = conditions.map(c => c.toString).mkString("(", " AND ", ")")
}

case class EqualToCondition(left: Expression, right: Expression) extends ExtraCondition {
    override def toString: String = s"${left.format()} = ${right.format()}"
}