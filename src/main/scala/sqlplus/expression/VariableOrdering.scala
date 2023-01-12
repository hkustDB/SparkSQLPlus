package sqlplus.expression

object VariableOrdering {
    implicit def ordering[A <: Variable]: Ordering[A] = Ordering.by(_.name)
}
