package sqlplus.wcoj

class RelationTrieIterator(private val relation: Array[Array[Int]], private val maxDepth: Int) {
    var depth = -1
    var index = -1
    var indexStack = List.empty[Int]
    var maxIndex = -1
    var maxIndexStack = List.empty[Int]

    def open(): Unit = {
        if (depth < 0) {
            index = 0
            maxIndex = relation.length
            depth = 0
        } else {
            indexStack = index :: indexStack
            maxIndexStack = maxIndex :: maxIndexStack
            maxIndex = binarySearch(index, maxIndex, key() + 1)
            depth += 1
        }
    }

    def up(): Unit = {
        if (depth == 0) {
            depth = -1
        } else {
            depth -= 1
            index = indexStack.head
            indexStack = indexStack.tail
            maxIndex = maxIndexStack.head
            maxIndexStack = maxIndexStack.tail
        }
    }

    def key(): Int = relation(index)(depth)

    def next(): Unit = {
        if (depth == maxDepth) {
            index += 1
        } else {
            seek(key() + 1)
        }
    }

    def seek(target: Int): Unit = {
        index = binarySearch(index, maxIndex, target)
    }

    def atEnd(): Boolean = {
        index >= maxIndex
    }

    private def binarySearch(begin: Int, end: Int, target: Int): Int = {
        var from = begin
        var to = end

        while (from < to) {
            {
                val mid = (from + to) / 2
                val midVal = relation(mid)(depth)
                if (midVal >= target) {
                    to = mid
                } else {
                    from = mid + 1
                }
            }
        }

        from
    }
}
