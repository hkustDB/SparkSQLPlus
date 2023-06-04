package sqlplus.utils

import scala.collection.mutable

/**
 * A DisjointSet for determining which variables should be the same(convert to natural join form).
 * For example, when we have R(a,b) x S(c,d) x T(e,f) on b=c and d=e.
 * With this DisjointSet, we can easily assign a set contains a single element for each column(a to f).
 * Then everytime we encounter a 'left = right' condition, merge the left set and the right set.
 * Finally, replace all the column variables with their Representatives.
 * In this example, we will get something like R(Var1, Var3) x S(Var3, Var5) x T(Var5, Var6)
 */
class DisjointSet[T] {
    private val parent: mutable.HashMap[T, T] = mutable.HashMap.empty
    private val height: mutable.HashMap[T, Int] = mutable.HashMap.empty

    def makeNewSet(t: T): T = {
        assert(!parent.contains(t))
        parent(t) = t
        height(t) = 0
        t
    }

    def getRepresentative(t: T): T = {
        var c = t
        while (parent(c) != c) {
            c = parent(c)
        }
        c
    }

    def merge(left: T, right: T): Unit = {
        val l = getRepresentative(left)
        val r = getRepresentative(right)

        // merge left and right only when they belongs to different sets
        if (l != r) {
            val hl = height(l)
            val hr = height(r)
            if (hl < hr)
                parent(l) = r
            else if (hl > hr)
                parent(r) = l
            else {
                height(l) = height(l) + 1
                parent(r) = l
            }
        }
    }
}
