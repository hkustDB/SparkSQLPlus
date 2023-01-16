package sqlplus.expression

import sqlplus.types.{DataType, IntDataType}

sealed trait Expression {
    def getType(): DataType[_]
    def getVariables(): Set[Variable]
    def createFunctionGenerator(variables: List[Variable]): String => String
}

case class SingleVariableExpression(variable: Variable) extends Expression {
    override def getType(): DataType[_] = variable.dataType

    override def getVariables(): Set[Variable] = Set(variable)

    override def createFunctionGenerator(variables: List[Variable]): String => String = arr => arr + "(" + variables.indexOf(variable) + ")"
}

case class IntPlusExpression(left: Expression, right: Expression) extends Expression {
    assert(left.getType() == IntDataType && right.getType() == IntDataType)

    override def getType(): DataType[_] = IntDataType

    override def getVariables(): Set[Variable] = left.getVariables() ++ right.getVariables()

    override def createFunctionGenerator(variables: List[Variable]): String => String = arr => {
        val leftFuncApply = left.createFunctionGenerator(variables)(arr)
        val rightFuncApply = right.createFunctionGenerator(variables)(arr)
        s"($leftFuncApply.asInstanceOf[Int] + $rightFuncApply.asInstanceOf[Int])"
    }
}
