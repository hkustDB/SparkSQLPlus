package sqlplus.expression

import org.junit.Assert.assertTrue
import org.junit.Test
import sqlplus.types.IntDataType

class ExpressionTest {
    @Test
    def testComputeExpressionToString(): Unit = {
        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)
        val v7 = variableManager.getNewVariable(IntDataType)
        val v8 = variableManager.getNewVariable(IntDataType)

        // v1 * (v2 + v3 * (v4 + v5 + v6) * v7) + v8
        val expr = IntPlusIntExpression(
            IntTimesIntExpression(
                SingleVariableExpression(v1),
                IntPlusIntExpression(
                    SingleVariableExpression(v2),
                    IntTimesIntExpression(
                        SingleVariableExpression(v3),
                        IntTimesIntExpression(
                            IntPlusIntExpression(
                                SingleVariableExpression(v4),
                                IntPlusIntExpression(
                                    SingleVariableExpression(v5),
                                    SingleVariableExpression(v6)
                                )
                            ),
                            SingleVariableExpression(v7)
                        )
                    )
                )
            ),
            SingleVariableExpression(v8)
        )

        assertTrue(expr.toString == "((v1 * (v2 + (v3 * ((v4 + (v5 + v6)) * v7)))) + v8)")
    }

    @Test
    def testLiteralExpressionToString(): Unit = {
        val stringLitExpr = StringLiteralExpression("Hello,World!")
        val intLitExpr = IntLiteralExpression(42)
        val doubleLitExpr = DoubleLiteralExpression(37.3)
        val intervalLitExpr = IntervalLiteralExpression(1555200000L)
        val dateLitExpr = DateLiteralExpression(851990400000L)

        assertTrue(stringLitExpr.toString == "'Hello,World!'")
        assertTrue(intLitExpr.toString == "42")
        assertTrue(doubleLitExpr.toString == "37.3")
        assertTrue(intervalLitExpr.toString == "INTERVAL '18' DAY")
        assertTrue(dateLitExpr.toString == "DATE '1996-12-31'")
    }
}
