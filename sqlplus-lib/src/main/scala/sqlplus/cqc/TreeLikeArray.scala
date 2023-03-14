package sqlplus.cqc

/**
 * @param input the input Array to build the TreeLikeArray
 * @param keyIndex1
 * @param keyIndex2
 * @param func1
 * @param func2
 * @tparam C1 type of arguments in the first compare function
 * @tparam C2 type of arguments in the second compare function
 * @tparam T1 actual type of the location keyIndex1 of each row in input
 * @tparam T2 actual type of the location keyIndex2 of each row in input
 */
class TreeLikeArray[C1, C2, T1, T2](input: Array[Array[Any]],
                                    val keyIndex1: Int,
                                    val keyIndex2: Int,
                                    val func1: (C1, C1) => Boolean,
                                    val func2: (C2, C2) => Boolean)(implicit val f1: T1 => C1, val f2: T2 => C2) extends java.io.Serializable {
    // sort by the first compare function on position keyIndex1. The value located at keyIndex1 is T1(may not = C1).
    // Use the implicit f1 to convert it into type C1 and compare
    val content = input.sortWith((x, y) => func1(x(keyIndex1).asInstanceOf[T1], y(keyIndex1).asInstanceOf[T1]))

    class TreeLikeArrayIterator(value1: C1, value2: C2)(implicit val f1: T1 => C1, val f2: T2 => C2) extends Iterator[Array[Any]] {
        var keyValue1: C1 = value1
        var keyValue2: C2 = value2
        private var currentRowIndex: Int = 0
        private var nextElement: Array[Any] = _

        override def hasNext: Boolean = {
            if (nextElement != null)
                true
            else {
                while (currentRowIndex < content.length && nextElement == null) {
                    val row = content(currentRowIndex)
                    currentRowIndex = currentRowIndex + 1
                    if (func1(row(keyIndex1).asInstanceOf[T1], keyValue1)) {
                        if (func2(row(keyIndex2).asInstanceOf[T2], keyValue2)) {
                            nextElement = row
                        }
                    } else {
                        currentRowIndex = content.length
                    }
                }

                nextElement != null
            }
        }

        override def next(): Array[Any] = {
            if (!hasNext)
                Iterator.empty.next()
            else {
                val result = nextElement
                nextElement = null
                result
            }
        }
    }

    def iterator(keyValue1: C1, keyValue2: C2): Iterator[Array[Any]] = new TreeLikeArrayIterator(keyValue1, keyValue2)

    def toDictionary: Array[(T1, T2)] = {
        val size = content.length
        val result = Array.ofDim[(T1, T2)](size)

        result(0) = (content(0)(keyIndex1).asInstanceOf[T1], content(0)(keyIndex2).asInstanceOf[T2])
        for (i <- 1 until size) {
            if (func2(result(i - 1)._2, content(i)(keyIndex2).asInstanceOf[C2])) {
                result(i) = (content(i)(keyIndex1).asInstanceOf[T1], result(i - 1)._2)
            } else {
                result(i) = (content(i)(keyIndex1).asInstanceOf[T1], content(i)(keyIndex2).asInstanceOf[T2])
            }
        }

        result
    }
}
