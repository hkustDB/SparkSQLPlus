package sqlplus.expression

import sqlplus.types.{DataType, DataTypeCasting, DoubleDataType, IntDataType, LongDataType, StringDataType, TimestampDataType}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

sealed trait Operator {
    def getFuncName(): String
    def getFuncDefinition(): List[String]
    def getFuncLiteral(isReverse: Boolean = false): String
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
    def getOperator(op: String, left: Expression, right: Expression): Operator = {
        op match {
            case "<" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericLessThanImplementation(left.getType(), right.getType())
            case "<=" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericLessThanOrEqualToImplementation(left.getType(), right.getType())
            case ">" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericGreaterThanImplementation(left.getType(), right.getType())
            case ">=" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericGreaterThanOrEqualToImplementation(left.getType(), right.getType())
            case "<" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongLessThan
            case "<=" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongLessThanOrEqualTo
            case ">" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongGreaterThan
            case ">=" if (left.getType() == TimestampDataType && right.getType() == TimestampDataType) =>
                LongGreaterThanOrEqualTo
            case "LIKE" if (left.getType() == StringDataType && left.isInstanceOf[ComputeExpression] && right.isInstanceOf[StringLiteralExpression]) =>
                StringMatch(right.asInstanceOf[StringLiteralExpression].lit)
            case _ => throw new UnsupportedOperationException(s"Operator $op is not applicable" +
                s" with ${left.getType()} and ${right.getType()}.")
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

case class StringMatch(pattern: String) extends UnaryOperator {
    val id = StringMatch.newSuffix()
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
}

object StringMatch {
    private var suffix = 0
    def newSuffix(): Int = {
        val result = suffix
        suffix += 1
        result
    }
}