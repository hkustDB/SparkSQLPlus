package sqlplus.expression

import scala.collection.mutable

/**
 * A DisjointSet for determining which variables should be the same(convert to natural join form).
 * For example, when we have R(a,b) x S(c,d) x T(e,f) on b=c and d=e.
 * With this DisjointSet, we can easily assign a set contains a single element for each column(a to f).
 * Then everytime we encounter a 'left = right' condition, merge the left set and the right set.
 * Finally, replace all the column variables with their Representatives.
 * In this example, we will get something like R(Var1, Var3) x S(Var3, Var5) x T(Var5, Var6)
 */
class VariableDisjointSet {
    val variableToParentDict: mutable.HashMap[Variable, Variable] = mutable.HashMap.empty

    def makeNewSet(v: Variable): Variable = {
        assert(!variableToParentDict.contains(v))
        variableToParentDict(v) = null
        v
    }

    def getRepresentative(v: Variable): Variable = {
        var current = v
        while (variableToParentDict(current) != null) {
            current = variableToParentDict(current)
        }
        current
    }

    def merge(left: Variable, right: Variable): Unit = {
        val l = getRepresentative(left)
        val r = getRepresentative(right)
        variableToParentDict(r) = l
    }
}
