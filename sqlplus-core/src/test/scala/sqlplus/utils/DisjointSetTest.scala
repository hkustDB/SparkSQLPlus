package sqlplus.utils

import org.junit.Assert.assertTrue
import org.junit.Test

class DisjointSetTest {
    @Test
    def testDisjointSet(): Unit = {
        val s = new DisjointSet[Int]
        val list = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
        list.foreach(i => s.makeNewSet(i))
        assertTrue(list.forall(i => s.getRepresentative(i) == i))

        s.merge(3, 3)

        s.merge(0, 9)
        s.merge(1, 5)
        assertTrue(s.getRepresentative(0) == s.getRepresentative(9))
        assertTrue(s.getRepresentative(5) == s.getRepresentative(1))

        s.merge(1, 5)
        s.merge(5, 1)
        assertTrue(s.getRepresentative(0) == s.getRepresentative(9))
        assertTrue(s.getRepresentative(5) == s.getRepresentative(1))

        s.merge(2, 5)
        assertTrue(s.getRepresentative(2) == s.getRepresentative(5))
        assertTrue(s.getRepresentative(5) == s.getRepresentative(1))

        s.merge(9, 1)
        assertTrue(s.getRepresentative(0) == s.getRepresentative(1))
        assertTrue(s.getRepresentative(1) == s.getRepresentative(2))
        assertTrue(s.getRepresentative(2) == s.getRepresentative(5))
        assertTrue(s.getRepresentative(5) == s.getRepresentative(9))

        assertTrue(s.getRepresentative(7) == 7)
        assertTrue(s.getRepresentative(8) == 8)
    }
}
