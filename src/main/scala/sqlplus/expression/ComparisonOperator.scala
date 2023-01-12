package sqlplus.expression

import sqlplus.types.{DataType, LongDataType}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

sealed trait ComparisonOperator {
    def getFuncName(): String

    def getFuncDefinition(): String

    def getFuncExpression(isReverse: Boolean = false): String

    final def applyTo(x: String, y: String): String =
        s"${getFuncName()}($x, $y)"
}

object ComparisonOperator {
    def getComparisonOperator(op: String, left: Expression, right: Expression): ComparisonOperator = {
        op match {
            case "<" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericLessThanImplementation(left.getType(), right.getType())
            case "<=" if (DataType.isNumericType(left.getType()) && DataType.isNumericType(right.getType())) =>
                selectNumericLessThanImplementation(left.getType(), right.getType())
            case _ => throw new UnsupportedOperationException(s"Operator $op is not applicable" +
                s" with ${left.getType()} and ${right.getType()}.")
        }
    }

    private def selectNumericLessThanImplementation(leftType: DataType[_], rightType: DataType[_]): NumericLessThan[_] = {
        if (leftType == LongDataType || rightType == LongDataType)
            LongLessThan
        else
            IntLessThan
    }

    private def selectNumericLessThanOrEqualToImplementation(leftType: DataType[_], rightType: DataType[_]): NumericLessThan[_] = {
        if (leftType == LongDataType || rightType == LongDataType)
            LongLessThanOrEqualTo
        else
            IntLessThanOrEqualTo
    }
}

class NumericLessThan[T: TypeTag] extends ComparisonOperator {
    private val typeName = typeOf[T].toString

    override def getFuncName(): String = s"${typeName.toLowerCase}LessThan"

    override def getFuncDefinition(): String =
        s"val ${getFuncName()} = (x: $typeName, y: $typeName) => x < y"

    override def getFuncExpression(isReverse: Boolean): String = {
        s"(x: $typeName, y: $typeName) => ${if (!isReverse) applyTo("x", "y") else  applyTo("y", "x")}"
    }
}

class NumericLessThanOrEqualTo[T: TypeTag] extends ComparisonOperator {
    private val typeName = typeOf[T].toString

    override def getFuncName(): String = s"${typeName.toLowerCase}LessThan"

    override def getFuncDefinition(): String =
        s"val ${getFuncName()} = (x: $typeName, y: $typeName) => x <= y"

    override def getFuncExpression(isReverse: Boolean): String = {
        s"(x: $typeName, y: $typeName) => ${if (!isReverse) applyTo("x", "y") else  applyTo("y", "x")}"
    }
}

case object IntLessThan extends NumericLessThan[Int]
case object LongLessThan extends NumericLessThan[Long]

case object IntLessThanOrEqualTo extends NumericLessThan[Int]
case object LongLessThanOrEqualTo extends NumericLessThan[Long]
