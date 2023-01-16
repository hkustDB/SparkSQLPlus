package sqlplus.helper

import org.apache.spark.rdd.RDD
import sqlplus.cqc.{ComparisonJoins, TreeLikeArray}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Implicit conversions that allow the compiler and user to write code like
 *
 * ...
 * val v12 = v2.keyBy(x => x(2).asInstanceOf[Int])
 * val v13 = v12.appendExtraColumn(v11)
 * ...
 *
 * The newly added operations of SparkCQC act just like normal RDD operations.
 */
object ImplicitConversions {
    implicit def convertRDDToGroupByTreeLikeArrayRDD[K: ClassTag,K1,K2](input: RDD[(K, TreeLikeArray[K1, K2])]) =
        new GroupByTreeLikeArrayRDD[K,K1,K2](input)

    implicit def convertRDDToGroupByRDD[K: ClassTag](input: RDD[(K, Array[Array[Any]])]) = new GroupByRDD[K](input)

    implicit def convertRDDToKeyByRDD[K: ClassTag](input: RDD[(K, Array[Any])]): KeyByRDD[K] = new KeyByRDD[K](input)

    def bag(rdd1: RDD[Array[Any]], keySelector1: Array[Any] => Product,
            rdd2: RDD[Array[Any]], keySelector2: Array[Any] => Product,
            keySelector12: Array[Any] => Product,
            rdd3: RDD[Array[Any]], keySelector3: Array[Any] => Product,
            finalIndices: Array[Int]
           ): RDD[Array[Any]] = {
        val keyedRdd1 = rdd1.keyBy(row => keySelector1(row))
        val keyedRdd2 = rdd2.keyBy(row => keySelector2(row))
        val keyedRdd1x2 = keyedRdd1.join(keyedRdd2).map(t => t._2._1 ++ t._2._2).keyBy(row => keySelector12(row))
        val keyedRdd3 = rdd3.keyBy(row => keySelector3(row))

        val finalRdd = keyedRdd1x2.join(keyedRdd3).map(t => {
            val total = t._2._1 ++ t._2._2
            finalIndices.map(i => total(i))
        })

        finalRdd
    }
}

/**
 * Wrapper class of a keyed RDD.
 * @param input the inner RDD
 * @tparam K the type of the key
 */
class KeyByRDD[K: ClassTag](input: RDD[(K, Array[Any])]) extends Serializable {
    /**
     * Group the rows by the current key.
     * All the rows(in type Array[Any]) with the same key will be arranged in a Array.
     * @return a grouped RDD
     */
    def groupBy(): RDD[(K, Array[Array[Any]])] =
        input.groupByKey().mapValues(x => x.toArray)

    /**
     * Key the RDD with another key.
     * @param keySelector the function that selects an new key from the row
     * @tparam T the type of the new key
     * @return a keyed RDD with new key
     */
    def reKeyBy[T](keySelector: Array[Any] => T): RDD[(T, Array[Any])] =
        input.map(x => (keySelector(x._2), x._2))

    /**
     * Append the extra columns(the mf* fields) to the current RDD. The current RDD and the extra RDD
     * must be keyed by the join key. This method is used when the child relation has only one incident
     * comparison.
     * @param extra the RDD that contains the extra column
     * @return a keyed RDD with an extra field on each row
     */
    def appendExtraColumn(extra: RDD[(K, Any)]): RDD[(K, Array[Any])] = {
        input.cogroup(extra).flatMap(value => {
            val left = value._2._1.iterator
            val right = value._2._2
            val result = for {
                l <- left
                r <- right
            } yield (value._1, l :+ r)
            result
        })
    }

    /**
     * Append the extra columns(the mf* fields) to the current RDD. The current RDD and the extra RDD
     * must be keyed by the join key. This method is used when the child relation has more than one incident
     * comparison.
     * @param extra the RDD that contains the extra column
     * @param keyIndex the index of the column for comparison in current RDD.
     * @param func the function for comparison
     * @tparam K1 the type of columns in the first comparison
     * @tparam K2 the type of columns in the second comparison
     * @return a keyed RDD with an extra field on each row
     */
    def appendExtraColumn[K1,K2](extra: RDD[(K, Array[(K1, K2)])], keyIndex: Int, func: (K1, K1) => Boolean): RDD[(K, Array[Any])] = {
        input.cogroup(extra).flatMap(value => {
            val left = value._2._1.toArray
            val right = value._2._2
            for {
                l <- left
                r <- right
                if func(r.head._1, l(keyIndex).asInstanceOf[K1])
            } yield (value._1, l :+ binarySearchInDictionary[K1,K2](r, l(keyIndex).asInstanceOf[K1], func))
        })
    }

    /**
     * Append an extra column by computation on each row.
     * @param func the function that accepts a row and yields an extra field
     * @tparam T the type of the extra column
     * @return a keyed RDD with an extra field on each row
     */
    def appendExtraColumn[T](func: Array[Any] => T): RDD[(K, Array[Any])] = {
        input.mapValues(value => value :+ func(value))
    }

    /**
     * Keep only the tuples that can join with some other tuple in the target RDD.
     * @param that the target RDD
     * @return a keyed RDD without dangling tuples
     */
    def semiJoin(that: RDD[(K, Array[Any])]): RDD[(K, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._2.nonEmpty).flatMapValues(x => x._1)
    }

    /**
     * Enumerate the target RDD and construct a bigger RDD representing the intermediate result. The current RDD
     * must be keyed by the join key. The target RDD must be grouped by the join key. This method is used when
     * the child relation has no incident comparison.
     * @param that the target RDD to be enumerated
     * @param extractIndices1 the indices that indicates which fields in current RDD will be preserved after enumeration
     * @param extractIndices2 the indices that indicates which fields in target RDD will be preserved after enumeration
     * @return
     */
    def enumerate(that: RDD[(K, Array[Array[Any]])], extractIndices1: Array[Int], extractIndices2: Array[Int]): RDD[(K, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightArray.toIterator
                    .map(right => (key, extractFields(left, right, extractIndices1, extractIndices2))))
        }))
    }

    /**
     * Enumerate the target RDD and construct a bigger RDD representing the intermediate result. The current RDD
     * must be keyed by the join key. The target RDD must be grouped by the join key and sorted by the column
     * in the comparison. This method is used when the child relation has only one incident comparison.
     * @param that the target RDD to be enumerated
     * @param keyIndex1 the index of the column in comparison in the current RDD
     * @param keyIndex2 the index of the column in comparison in the target RDD
     * @param func the function for comparison
     * @param extractIndices1 the indices that indicates which fields in current RDD will be preserved after enumeration
     * @param extractIndices2 the indices that indicates which fields in target RDD will be preserved after enumeration
     * @tparam T the type of column in comparison
     * @return
     */
    def enumerate[T](that: RDD[(K, Array[Array[Any]])], keyIndex1: Int, keyIndex2: Int, func: (T, T) => Boolean,
                     extractIndices1: Array[Int], extractIndices2: Array[Int]): RDD[(K, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightArray.toIterator.takeWhile(right =>
                    func(left(keyIndex1).asInstanceOf[T], right(keyIndex2).asInstanceOf[T]))
                    .map(right => (key, extractFields(left, right, extractIndices1, extractIndices2))))
        }))
    }

    /**
     * Enumerate the target RDD and construct a bigger RDD representing the intermediate result. The current RDD
     * must be keyed by the join key. The target RDD must be a TreeLikeArray keyed by the join key. This method
     * is used when the child relation has more than one incident comparison.
     * @param that the target RDD to be enumerated
     * @param keyIndex1 the index of the column in the first comparison in the current RDD
     * @param keyIndex2 the index of the column in the second comparison in the current RDD
     * @param extractIndices1 the indices that indicates which fields in current RDD will be preserved after enumeration
     * @param extractIndices2 the indices that indicates which fields in target RDD will be preserved after enumeration
     * @tparam K1 the type of column in the first comparison
     * @tparam K2 the type of column in the second comparison
     * @return
     */
    def enumerate[K1,K2](that: RDD[(K, TreeLikeArray[K1, K2])], keyIndex1: Int, keyIndex2: Int,
                         extractIndices1: Array[Int], extractIndices2: Array[Int]): RDD[(K, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightTreeLikeArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightTreeLikeArray.enumerationIterator(left(keyIndex1).asInstanceOf[K1], left(keyIndex2).asInstanceOf[K2])
                    .map(right => (key, extractFields(left, right, extractIndices1, extractIndices2))))
        }))
    }

    private def extractFields(array1: Array[Any], array2: Array[Any], indices1: Array[Int], indices2: Array[Int]): Array[Any] = {
        val buffer = Array.ofDim[Any](indices1.length + indices2.length)
        var current = 0
        for (i <- indices1) {
            buffer(current) = array1(i)
            current += 1
        }

        for (i <- indices2) {
            buffer(current) = array2(i)
            current += 1
        }

        buffer
    }

    private def binarySearchInDictionary[K1,K2](dict: Array[(K1, K2)], key: K1, func: (K1, K1) => Boolean): K2 = {
        ComparisonJoins.binarySearch(dict, key, func)
    }
}

/**
 * Wrapper class of a grouped RDD.
 * @param input the inner RDD
 * @tparam K the type of the key
 */
class GroupByRDD[K: ClassTag](input: RDD[(K, Array[Array[Any]])]) {
    /**
     * Sort every groups in the current RDD by the column in keyIndex and the given function.
     * @param keyIndex the index of the column
     * @param func the sort function
     * @tparam T the type of the sort column
     * @return a sorted and grouped RDD
     */
    def sortValuesWith[T](keyIndex: Int, func: (T, T) => Boolean): RDD[(K, Array[Array[Any]])] =
        input.mapValues(value => value.sortWith((left, right) =>
            func(left(keyIndex).asInstanceOf[T], right(keyIndex).asInstanceOf[T])))

    /**
     * Pick the field at fieldIndex in the head element of each group. This method is used when we
     * want to create a RDD that contains an extra column for the parent. The RDD must be sorted by
     * the field at fieldIndex before calling this method.
     * @param fieldIndex the index of the field to be extracted
     * @return a RDD that contains a tuple (Key, Minimum) for each group
     */
    def extractFieldInHeadElement(fieldIndex: Int): RDD[(K, Any)] =
        input.mapValues(value => value.head(fieldIndex))

    /**
     * Construct a TreeLikeArray structure for each group.
     * @param keyIndex1 the index of the column in the first comparison in the current RDD
     * @param keyIndex2 the index of the column in the second comparison in the current RDD
     * @param func1 the function for the first comparison
     * @param func2 the function for the second comparison
     * @tparam K1 the type of column in the first comparison
     * @tparam K2 the type of column in the second comparison
     * @return a RDD that contains a TreeLikeArray for every group
     */
    def constructTreeLikeArray[K1,K2](keyIndex1: Int, keyIndex2: Int, func1: (K1, K1) => Boolean,
                                      func2: (K2, K2) => Boolean): RDD[(K, TreeLikeArray[K1, K2])] = {
        input.mapValues(x => new TreeLikeArray[K1,K2](x, keyIndex1, keyIndex2, func1, func2))
    }
}

/**
 * Wrapper class of a grouped TreeLikeArray RDD.
 * @param input the inner RDD
 * @tparam K the type of the key
 * @tparam K1 the type of column in the first comparison
 * @tparam K2 the type of column in the second comparison
 */
class GroupByTreeLikeArrayRDD[K: ClassTag,K1,K2](input: RDD[(K, TreeLikeArray[K1, K2])]) {
    /**
     * Create a array of (K1, K2) values from each TreeLikeArray. The K1 and K2 fields are in ascending order(based on
     * the compare functions in TreeLikeArray). This method is used when we want to create a RDD that contains
     * an extra column for the parent.
     * @return the dict
     */
    def createDictionary(): RDD[(K, Array[(K1, K2)])] =
        input.mapValues(value => value.toSmall)
}