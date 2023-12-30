package sqlplus.convert

import sqlplus.expression.Variable

case class TopK(sortBy: Variable, isDesc: Boolean, limit: Int)
