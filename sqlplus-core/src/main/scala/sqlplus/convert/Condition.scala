package sqlplus.convert

import sqlplus.expression.{Expression, LiteralExpression, StringLiteralExpression}

sealed trait Condition

case class LessThanCondition(leftOperand: Expression, rightOperand: Expression) extends Condition {
    override def toString: String = s"(${leftOperand.format()} < ${rightOperand.format()})"
}

case class LessThanOrEqualToCondition(leftOperand: Expression, rightOperand: Expression) extends Condition {
    override def toString: String = s"(${leftOperand.format()} <= ${rightOperand.format()})"
}

case class GreaterThanCondition(leftOperand: Expression, rightOperand: Expression) extends Condition {
    override def toString: String = s"(${leftOperand.format()} > ${rightOperand.format()})"
}

case class GreaterThanOrEqualToCondition(leftOperand: Expression, rightOperand: Expression) extends Condition {
    override def toString: String = s"(${leftOperand.format()} >= ${rightOperand.format()})"
}

case class EqualToLiteralCondition(operand: Expression, literal: LiteralExpression) extends Condition {
    override def toString: String = s"(${operand.format()} = ${literal.format()})"
}

case class NotEqualToLiteralCondition(operand: Expression, literal: LiteralExpression) extends Condition {
    override def toString: String = s"(${operand.format()} <> ${literal.format()})"
}

case class LikeCondition(operand: Expression, s: StringLiteralExpression, isNeg: Boolean) extends Condition {
    override def toString: String = {
        val op = if (isNeg) "NOT LIKE" else "LIKE"
        s"(${operand} ${op} '${s.lit}')"
    }
}

case class InCondition(operand: Expression, literals: List[LiteralExpression], isNeg: Boolean) extends Condition {
    override def toString: String = {
        val set = literals.map(s => s"'$s'").mkString("(", ",", ")")
        val op = if (isNeg) "NOT IN" else "IN"
        s"(${operand} ${op} ${set})"
    }
}

case class OrCondition(conditions: List[Condition], involved: Set[Expression]) extends Condition {
    override def toString: String = conditions.map(c => c.toString).mkString("(", " OR ", ")")
}

case class AndCondition(conditions: List[Condition], involved: Set[Expression]) extends Condition {
    override def toString: String = conditions.map(c => c.toString).mkString("(", " AND ", ")")
}

case class IsNullCondition(operand: Expression) extends Condition {
    override def toString: String = s"${operand} IS NULL"
}

case class IsNotNullCondition(operand: Expression) extends Condition {
    override def toString: String = s"${operand} IS NOT NULL"
}

sealed trait ExtraCondition extends Condition
case class ExtraOrCondition(conditions: List[Condition]) extends ExtraCondition {
    override def toString: String = conditions.map(c => c.toString).mkString("(", " OR ", ")")
}

case class ExtraEqualToCondition(left: Expression, right: Expression) extends ExtraCondition {
    override def toString: String = s"${left.format()} = ${right.format()}"
}