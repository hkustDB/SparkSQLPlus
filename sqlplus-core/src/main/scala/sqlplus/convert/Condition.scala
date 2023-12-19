package sqlplus.convert

import sqlplus.expression.{Expression, LiteralExpression, StringLiteralExpression}

sealed trait Condition

case class LessThanCondition(leftOperand: Expression, rightOperand: Expression) extends Condition
case class LessThanOrEqualToCondition(leftOperand: Expression, rightOperand: Expression) extends Condition
case class GreaterThanCondition(leftOperand: Expression, rightOperand: Expression) extends Condition
case class GreaterThanOrEqualToCondition(leftOperand: Expression, rightOperand: Expression) extends Condition

case class EqualToLiteralCondition(operand: Expression, literal: LiteralExpression) extends Condition
case class NotEqualToLiteralCondition(operand: Expression, literal: LiteralExpression) extends Condition
case class LikeCondition(operand: Expression, s: StringLiteralExpression, isNeg: Boolean) extends Condition
case class InCondition(operand: Expression, literals: List[LiteralExpression], isNeg: Boolean) extends Condition