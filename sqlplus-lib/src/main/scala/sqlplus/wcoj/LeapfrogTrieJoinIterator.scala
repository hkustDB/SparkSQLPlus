package sqlplus.wcoj

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

class LeapfrogTrieJoinIterator(relations: Array[Iterable[Array[Int]]], variableRelatedRelations: Array[Array[Int]]) extends Iterator[Array[Any]] {
    var nextTuple: Array[Any] = _
    var depth = -1
    val maxDepth = variableRelatedRelations.length - 1
    var partialTuple: ArrayBuffer[Any] = _
    var trieJoins: Array[LeapfrogJoinIterator] = _

    def init(): Unit = {
        val relationTrieIterators = relations.map(it => {
            val array = it.toArray
            Sorting.quickSort(array)(LeapfrogTrieJoinIterator.ORDERING)
            new RelationTrieIterator(array, array.head.length - 1)
        })

        trieJoins = variableRelatedRelations.map(vector => {
            new LeapfrogJoinIterator(vector.map(i => relationTrieIterators(i)))
        })

        partialTuple = ArrayBuffer.fill(variableRelatedRelations.length)(0)

        depth = 0
        trieJoins(0).open()
        trieJoins(0).init()
        compute()
    }

    override def hasNext: Boolean = nextTuple != null

    override def next(): Array[Any] = {
        val result = nextTuple
        compute()
        result
    }

    private def compute(): Unit = {
        while (true) {
            if (depth < 0) {
                nextTuple = null
                return
            } else if (depth == maxDepth) {
                val trieJoin = trieJoins(depth)
                if (trieJoin.atEnd()) {
                    trieJoin.up()
                    depth -= 1
                    trieJoins(depth).next()
                } else {
                    partialTuple(depth) = trieJoin.key()
                    nextTuple = partialTuple.toArray
                    trieJoin.next()
                    return
                }
            } else {
                val trieJoin = trieJoins(depth)
                if (trieJoin.atEnd()) {
                    trieJoin.up()
                    depth -= 1
                    if (depth >= 0)
                        trieJoins(depth).next()
                } else {
                    partialTuple(depth) = trieJoin.key()
                    depth += 1
                    val nextTrieJoin = trieJoins(depth)
                    nextTrieJoin.open()
                    nextTrieJoin.init()
                }
            }
        }
    }
}

object LeapfrogTrieJoinIterator {
    final val ORDERING = new Ordering[Array[Int]] {
        override def compare(x: Array[Int], y: Array[Int]): Int = {
            if (x(0) < y(0))
                -1
            else if (x(0) > y(0))
                1
            else
                x(1) - y(1)
        }
    }
}
