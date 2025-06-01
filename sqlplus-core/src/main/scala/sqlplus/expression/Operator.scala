package sqlplus.expression

import sqlplus.convert.Condition
import sqlplus.types.{DataType, DataTypeCasting, DateDataType, DoubleDataType, IntDataType, LongDataType, StringDataType, TimestampDataType}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

sealed trait Operator {
    def getFuncName(): String
    def getFuncDefinition(): List[String]
    def getFuncLiteral(isReverse: Boolean = false): String
    def isNegated(): Boolean
    def format(expressions: List[Expression]): String
}

sealed trait UnaryOperator extends Operator {
    final def apply(x: String): String =
        s"${getFuncName()}($x)"
}

sealed trait BinaryOperator extends Operator {
    final def apply(x: String, y: String): String =
        s"${getFuncName()}($x, $y)"

    def leftTypeName: String

    def rightTypeName: String
}

object Operator {
    def getOperator(op: String, left: Expression, right: Expression, isNeg: Boolean): Operator = {
        op match {
            case "<" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericLessThanImplementation(left.getType(), right.getType())
            case "<=" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericLessThanOrEqualToImplementation(left.getType(), right.getType())
            case ">" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericGreaterThanImplementation(left.getType(), right.getType())
            case ">=" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericGreaterThanOrEqualToImplementation(left.getType(), right.getType())
            case "<" if (left.getType() == StringDataType && right.getType() == StringDataType) =>
                StringLessThan
            case "<=" if (left.getType() == StringDataType && right.getType() == StringDataType) =>
                StringLessThanOrEqualTo
            case ">" if (left.getType() == StringDataType && right.getType() == StringDataType) =>
                StringGreaterThan
            case ">=" if (left.getType() == StringDataType && right.getType() == StringDataType) =>
                StringGreaterThanOrEqualTo
            case "<" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongLessThan
            case "<=" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongLessThanOrEqualTo
            case ">" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongGreaterThan
            case ">=" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongGreaterThanOrEqualTo
            case "<" if (left.getType() == DateDataType && right.getType() == DateDataType) =>
                LongLessThan
            case "<=" if (left.getType() == DateDataType && right.getType() == DateDataType) =>
                LongLessThanOrEqualTo
            case ">" if (left.getType() == DateDataType && right.getType() == DateDataType) =>
                LongGreaterThan
            case ">=" if (left.getType() == DateDataType && right.getType() == DateDataType) =>
                LongGreaterThanOrEqualTo
            case "LIKE" if (left.getType() == StringDataType && left.isInstanceOf[ComputeExpression] && right.isInstanceOf[StringLiteralExpression]) =>
                StringMatch(right.asInstanceOf[StringLiteralExpression].lit, isNeg)
            case "=" if right.isInstanceOf[LiteralExpression] && !left.isInstanceOf[LiteralExpression] =>
                selectEqualToLiteralImplementation(left.getType(), right.asInstanceOf[LiteralExpression])
            case "<>" if right.isInstanceOf[LiteralExpression] && !left.isInstanceOf[LiteralExpression] =>
                selectNotEqualToLiteralImplementation(left.getType(), right.asInstanceOf[LiteralExpression])
            case _ => throw new UnsupportedOperationException(s"Operator $op is not applicable" +
                s" with ${left.getType()} and ${right.getType()}.")
        }
    }

    def getOperator(op: String, head: Expression, tail: List[Expression], isNeg: Boolean): Operator = {
        op match {
            case "IN" => selectInLiteralsImplementation(head.getType(), tail, isNeg)
        }
    }

    def getOperator(op: String): Operator = {
        op match {
            case "IS NULL" => IsNull
            case "IS NOT NULL" => IsNotNull
        }
    }

    private def selectNumericLessThanImplementation(leftType: DataType, rightType: DataType): NumericLessThan[_] = {
        DataTypeCasting.promote(leftType, rightType) match {
            case DoubleDataType => DoubleLessThan
            case LongDataType => LongLessThan
            case IntDataType => IntLessThan
        }
    }

    private def selectNumericLessThanOrEqualToImplementation(leftType: DataType, rightType: DataType): NumericLessThanOrEqualTo[_] = {
        DataTypeCasting.promote(leftType, rightType) match {
            case DoubleDataType => DoubleLessThanOrEqualTo
            case LongDataType => LongLessThanOrEqualTo
            case IntDataType => IntLessThanOrEqualTo
        }
    }

    private def selectNumericGreaterThanImplementation(leftType: DataType, rightType: DataType): NumericGreaterThan[_] = {
        DataTypeCasting.promote(leftType, rightType) match {
            case DoubleDataType => DoubleGreaterThan
            case LongDataType => LongGreaterThan
            case IntDataType => IntGreaterThan
        }
    }

    private def selectNumericGreaterThanOrEqualToImplementation(leftType: DataType, rightType: DataType): NumericGreaterThanOrEqualTo[_] = {
        DataTypeCasting.promote(leftType, rightType) match {
            case DoubleDataType => DoubleGreaterThanOrEqualTo
            case LongDataType => LongGreaterThanOrEqualTo
            case IntDataType => IntGreaterThanOrEqualTo
        }
    }

    private def selectEqualToLiteralImplementation(dataType: DataType, lit: LiteralExpression): EqualToLiteral[_] = {
        dataType match {
            case IntDataType => IntEqualToLiteral(lit.asInstanceOf[IntLiteralExpression].lit, false)
            case LongDataType => LongEqualToLiteral(lit.asInstanceOf[LongLiteralExpression].lit, false)
            case DoubleDataType => DoubleEqualToLiteral(lit.asInstanceOf[DoubleLiteralExpression].lit, false)
            case StringDataType => StringEqualToLiteral(lit.asInstanceOf[StringLiteralExpression].lit, false)
        }
    }

    private def selectNotEqualToLiteralImplementation(dataType: DataType, lit: LiteralExpression): EqualToLiteral[_] = {
        dataType match {
            case IntDataType => IntEqualToLiteral(lit.asInstanceOf[IntLiteralExpression].lit, true)
            case LongDataType => LongEqualToLiteral(lit.asInstanceOf[LongLiteralExpression].lit, true)
            case DoubleDataType => DoubleEqualToLiteral(lit.asInstanceOf[DoubleLiteralExpression].lit, true)
            case StringDataType => StringEqualToLiteral(lit.asInstanceOf[StringLiteralExpression].lit, true)
        }
    }

    private def selectInLiteralsImplementation(dataType: DataType, literals: List[Expression], isNeg: Boolean): InLiterals[_] = {
        dataType match {
            case IntDataType => IntInLiterals(literals.map(e => e.asInstanceOf[IntLiteralExpression].lit), isNeg)
            case LongDataType => LongInLiterals(literals.map(e => e.asInstanceOf[LongLiteralExpression].lit), isNeg)
            case DoubleDataType => DoubleInLiterals(literals.map(e => e.asInstanceOf[DoubleLiteralExpression].lit), isNeg)
            case StringDataType => StringInLiterals(literals.map(e => e.asInstanceOf[StringLiteralExpression].lit), isNeg)
        }
    }
}

class NumericLessThan[T: TypeTag] extends NumericBinaryOperator[T]("LessThan", "<")
class NumericLessThanOrEqualTo[T: TypeTag] extends NumericBinaryOperator[T]("LessThanOrEqualTo", "<=")
class NumericGreaterThan[T: TypeTag] extends NumericBinaryOperator[T]("GreaterThan", ">")
class NumericGreaterThanOrEqualTo[T: TypeTag] extends NumericBinaryOperator[T]("GreaterThanOrEqualTo", ">=")

class NumericBinaryOperator[T: TypeTag](suffix: String, relationalOperator: String) extends BinaryOperator {
    private val typeName = typeOf[T].toString

    override def getFuncName(): String = s"${typeName.toLowerCase}${suffix}"

    override def getFuncDefinition(): List[String] =
        List(s"val ${getFuncName()} = (x: $typeName, y: $typeName) => x ${relationalOperator} y")

    override def getFuncLiteral(isReverse: Boolean): String = {
        s"(x: $typeName, y: $typeName) => ${if (!isReverse) apply("x", "y") else  apply("y", "x")}"
    }

    override def leftTypeName: String = typeName

    override def rightTypeName: String = typeName

    override def isNegated(): Boolean = false

    override def format(expressions: List[Expression]): String = s"(${expressions(0).format()} ${relationalOperator} ${expressions(1).format()})"
}

case object IntLessThan extends NumericLessThan[Int]
case object LongLessThan extends NumericLessThan[Long]
case object DoubleLessThan extends NumericLessThan[Double]

case object IntLessThanOrEqualTo extends NumericLessThanOrEqualTo[Int]
case object LongLessThanOrEqualTo extends NumericLessThanOrEqualTo[Long]
case object DoubleLessThanOrEqualTo extends NumericLessThanOrEqualTo[Double]

case object IntGreaterThan extends NumericGreaterThan[Int]
case object LongGreaterThan extends NumericGreaterThan[Long]
case object DoubleGreaterThan extends NumericGreaterThan[Double]

case object IntGreaterThanOrEqualTo extends NumericGreaterThanOrEqualTo[Int]
case object LongGreaterThanOrEqualTo extends NumericGreaterThanOrEqualTo[Long]
case object DoubleGreaterThanOrEqualTo extends NumericGreaterThanOrEqualTo[Double]

case class StringMatch(pattern: String, isNeg: Boolean) extends UnaryOperator {
    val id = UnaryOperatorSuffix.newSuffix()
    val patternName = s"pattern$id"
    val funcName = s"match$id"
    val regexString = "^" + pattern.replace("%", ".*") + "$"
    override def getFuncName(): String = funcName

    override def getFuncDefinition(): List[String] = {
        val regex = "\"" + regexString + "\""
        List(
            s"val ${patternName} = $regex.toPattern",
            s"val ${getFuncName()} = (s: String) => ${patternName}.matcher(s).matches()"
        )
    }

    override def getFuncLiteral(isReverse: Boolean): String = {
        assert(!isReverse)
        s"(s: String) => ${apply("s")}"
    }

    override def isNegated(): Boolean = isNeg

    override def format(expressions: List[Expression]): String = {
        val op = if (isNeg) "NOT LIKE" else "LIKE"
        s"(${expressions(0)} ${op} '${pattern}')"
    }
}

abstract class EqualToLiteral[T: TypeTag](private val isNeg: Boolean) extends UnaryOperator {
    val id = UnaryOperatorSuffix.newSuffix()
    private val typeName = typeOf[T].toString

    override def getFuncName(): String = s"${typeName.toLowerCase}EqualToLiteral${id}"

    override def getFuncDefinition(): List[String] = throw new UnsupportedOperationException()

    override def getFuncLiteral(isReverse: Boolean): String = throw new UnsupportedOperationException()

    override def isNegated(): Boolean = isNeg
}

case class StringEqualToLiteral(lit: String, isNeg: Boolean) extends EqualToLiteral[String](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val op = if (isNeg) "<>" else "="
        s"(${expressions(0)} ${op} '${lit}')"
    }
}

case class IntEqualToLiteral(lit: Int, isNeg: Boolean) extends EqualToLiteral[Int](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val op = if (isNeg) "<>" else "="
        s"(${expressions(0)} ${op} ${lit})"
    }
}

case class LongEqualToLiteral(lit: Long, isNeg: Boolean) extends EqualToLiteral[Long](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val op = if (isNeg) "<>" else "="
        s"(${expressions(0)} ${op} ${lit})"
    }
}

case class DoubleEqualToLiteral(lit: Double, isNeg: Boolean) extends EqualToLiteral[Double](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val op = if (isNeg) "<>" else "="
        s"(${expressions(0)} ${op} ${lit})"
    }
}

abstract class InLiterals[T: TypeTag](private val isNeg: Boolean) extends UnaryOperator {
    val id = UnaryOperatorSuffix.newSuffix()
    private val typeName = typeOf[T].toString

    override def getFuncName(): String = s"${typeName.toLowerCase}InLiterals${id}"

    override def getFuncDefinition(): List[String] = throw new UnsupportedOperationException()

    override def getFuncLiteral(isReverse: Boolean): String = throw new UnsupportedOperationException()

    override def isNegated(): Boolean = isNeg
}

case class StringInLiterals(literals: List[String], isNeg: Boolean) extends InLiterals[String](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val set = literals.map(s => s"'$s'").mkString("(", ",", ")")
        val op = if (isNeg) "NOT IN" else "IN"
        s"(${expressions(0)} ${op} ${set})"
    }
}

case class IntInLiterals(literals: List[Int], isNeg: Boolean) extends InLiterals[Int](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val set = literals.mkString("(", ",", ")")
        val op = if (isNeg) "NOT IN" else "IN"
        s"(${expressions(0)} ${op} ${set})"
    }
}

case class LongInLiterals(literals: List[Long], isNeg: Boolean) extends InLiterals[Long](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val set = literals.mkString("(", ",", ")")
        val op = if (isNeg) "NOT IN" else "IN"
        s"(${expressions(0)} ${op} ${set})"
    }
}

case class DoubleInLiterals(literals: List[Double], isNeg: Boolean) extends InLiterals[Double](isNeg) {
    override def format(expressions: List[Expression]): String = {
        val set = literals.mkString("(", ",", ")")
        val op = if (isNeg) "NOT IN" else "IN"
        s"(${expressions(0)} ${op} ${set})"
    }
}

case object IsNull extends UnaryOperator {
    override def getFuncName(): String = "IsNull"

    override def getFuncDefinition(): List[String] = throw new UnsupportedOperationException()

    override def getFuncLiteral(isReverse: Boolean): String = throw new UnsupportedOperationException()

    override def isNegated(): Boolean = throw new UnsupportedOperationException()

    override def format(expressions: List[Expression]): String = s"(${expressions(0)} IS NULL)"
}

case object IsNotNull extends UnaryOperator {
    override def getFuncName(): String = "IsNotNull"

    override def getFuncDefinition(): List[String] = throw new UnsupportedOperationException()

    override def getFuncLiteral(isReverse: Boolean): String = throw new UnsupportedOperationException()

    override def isNegated(): Boolean = throw new UnsupportedOperationException()

    override def format(expressions: List[Expression]): String = s"(${expressions(0)} IS NOT NULL)"
}

object UnaryOperatorSuffix {
    private var suffix = 0
    def newSuffix(): Int = {
        val result = suffix
        suffix += 1
        result
    }
}

abstract class StringBinaryOperator(relationalOperator: String) extends BinaryOperator {
    override def leftTypeName: String = "String"
    override def rightTypeName: String = "String"
    override def getFuncDefinition(): List[String] = throw new UnsupportedOperationException()
    override def getFuncLiteral(isReverse: Boolean): String = throw new UnsupportedOperationException()
    override def isNegated(): Boolean = false

    override def format(expressions: List[Expression]): String = s"(${expressions(0).format()} ${relationalOperator} ${expressions(1).format()})"
}

case object StringLessThan extends StringBinaryOperator("<") {
    override def getFuncName(): String = "stringLessThan"
}

case object StringLessThanOrEqualTo extends StringBinaryOperator("<=") {
    override def getFuncName(): String = "stringLessThanOrEqualTo"
}

case object StringGreaterThan extends StringBinaryOperator(">") {
    override def getFuncName(): String = "stringGreaterThan"
}

case object StringGreaterThanOrEqualTo extends StringBinaryOperator(">=") {
    override def getFuncName(): String = "stringGreaterThanOrEqualTo"
}

case class OrOperator(conditions: List[Condition]) extends Operator {
    override def getFuncName(): String = "or"
    override def getFuncDefinition(): List[String] = throw new UnsupportedOperationException()
    override def getFuncLiteral(isReverse: Boolean): String = throw new UnsupportedOperationException()
    override def isNegated(): Boolean = false

    override def format(expressions: List[Expression]): String = conditions.map(c => c.toString).mkString("(", " OR ", ")")
}