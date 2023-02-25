package sqlplus.wcoj

class LeapfrogJoinIterator(val iterators: Array[RelationTrieIterator]) {
    var isAtEnd = false
    var keyValue: Int = _
    var p = 0

    def init(): Unit = {
        p = 0
        isAtEnd = false

        if (iterators.exists(iterator => iterator.atEnd())) {
            isAtEnd = true
        } else {
            sortIterators()
            search()
        }
    }

    private def sortIterators(): Unit = {
        // use insertion sort since the size of iterators is very small
        val length = iterators.length
        for (i <- 1 until length) {
            val tmp = iterators(i)
            var j = i
            while (j > 0 && tmp.key() < iterators(j - 1).key()) {
                iterators(j) = iterators(j - 1)
                j -= 1
            }

            if (j != i) {
                iterators(j) = tmp
            }
        }
    }

    private def search(): Unit = {
        var maxKey = iterators((p + 1) % 2).key()
        while (true) {
            val x = iterators(p).key()
            if (x == maxKey) {
                keyValue = x
                return
            } else {
                iterators(p).seek(maxKey)
                if (iterators(p).atEnd()) {
                    isAtEnd = true
                    return
                } else {
                    maxKey = iterators(p).key()
                    p = (p + 1) % 2
                }
            }
        }
    }

    def next(): Unit = {
        iterators(p).next()
        if (iterators(p).atEnd()) {
            isAtEnd = true
        } else {
            p = (p + 1) % 2
            search()
        }
    }

    private def seek(x: Int): Unit = {
        iterators(p).seek(x)
        if (iterators(p).atEnd()) {
            isAtEnd = true
        } else {
            p = (p + 1) % 2
            search()
        }
    }

    def key(): Int = keyValue

    def open(): Unit = {
        iterators.foreach(it => it.open())
    }

    def up(): Unit = {
        iterators.foreach(it => it.up())
    }

    def atEnd(): Boolean = isAtEnd
}
