package sqlplus.helper

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import sqlplus.cqc.{ComparisonJoins, TreeLikeArray}
import sqlplus.wcoj.LeapfrogTrieJoinIterator

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.regex.Pattern
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

    implicit def convertSparkContextToLeapfrogTrieJoinAlgorithm(sc: SparkContext) = new LeapfrogTrieJoinAlgorithm(sc)

    implicit def convertStringToTimestampParser(s: String) = new TimestampFormatter.parser(s)

    implicit def convertTimestampToTimestampPrinter(ts: Long) = new TimestampFormatter.printer(ts)

    implicit def convertStringToRegexPattern(r: String) = new RegexPattern(r)
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
     * @param resultKeySelector a function that extract key fields
     * @tparam T the type of key fields after enumerate
     * @return
     */
    def enumerateWithoutComparison[T](that: RDD[(K, Array[Array[Any]])], extractIndices1: Array[Int], extractIndices2: Array[Int],
                  resultKeySelector: (Array[Any], Array[Any]) => T = null): RDD[(T, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightArray.toIterator
                    .map(right => {
                        val extracted = extractFields(left, right, extractIndices1, extractIndices2)
                        if (resultKeySelector == null)
                            (key.asInstanceOf[T], extracted)
                        else
                            (resultKeySelector(left, right), extracted)
                    }))
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
     * @param resultKeySelector a function that extract key fields
     * @tparam T the type of key fields after enumerate
     * @tparam P the type of column in comparison
     * @return
     */
    def enumerateWithOneComparison[T,P](that: RDD[(K, Array[Array[Any]])], keyIndex1: Int, keyIndex2: Int, func: (P, P) => Boolean,
                                        extractIndices1: Array[Int], extractIndices2: Array[Int],
                                        resultKeySelector: (Array[Any], Array[Any]) => T = null): RDD[(T, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightArray.toIterator.takeWhile(right =>
                    func(left(keyIndex1).asInstanceOf[P], right(keyIndex2).asInstanceOf[P]))
                    .map(right => {
                        val extracted = extractFields(left, right, extractIndices1, extractIndices2)
                        if (resultKeySelector == null)
                            (key.asInstanceOf[T], extracted)
                        else
                            (resultKeySelector(left, right), extracted)
                    }))
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
     * @param resultKeySelector a function that extract key fields
     * @tparam K1 the type of column in the first comparison
     * @tparam K2 the type of column in the second comparison
     * @tparam T the type of key fields after enumerate
     * @return
     */
    def enumerateWithTwoComparisons[K1,K2,T](that: RDD[(K, TreeLikeArray[K1, K2])], keyIndex1: Int, keyIndex2: Int,
                                             extractIndices1: Array[Int], extractIndices2: Array[Int],
                                             resultKeySelector: (Array[Any], Array[Any]) => T = null): RDD[(T, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightTreeLikeArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightTreeLikeArray.enumerationIterator(left(keyIndex1).asInstanceOf[K1], left(keyIndex2).asInstanceOf[K2])
                    .map(right => {
                        val extracted = extractFields(left, right, extractIndices1, extractIndices2)
                        if (resultKeySelector == null)
                            (key.asInstanceOf[T], extracted)
                        else
                            (resultKeySelector(left, right), extracted)
                    }))
        }))
    }

    def enumerateWithMoreThanTwoComparisons[T, P](that: RDD[(K, Array[Array[Any]])], keyIndex1: Int, keyIndex2: Int, func: (P, P) => Boolean,
                                                  extraFilter: (Array[Any], Array[Any]) => Boolean,
                                                  extractIndices1: Array[Int], extractIndices2: Array[Int],
                                                  resultKeySelector: (Array[Any], Array[Any]) => T = null): RDD[(T, Array[Any])] = {
        input.cogroup(that).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions(iterator => iterator.flatMap(t => {
            val key = t._1
            val leftIterator = t._2._1.toIterator
            assert(t._2._2.size == 1)
            val rightArray = t._2._2.head
            leftIterator.flatMap(left =>
                rightArray.toIterator.takeWhile(right =>
                    func(left(keyIndex1).asInstanceOf[P], right(keyIndex2).asInstanceOf[P])).filter(right => extraFilter(left, right))
                    .map(right => {
                        val extracted = extractFields(left, right, extractIndices1, extractIndices2)
                        if (resultKeySelector == null)
                            (key.asInstanceOf[T], extracted)
                        else
                            (resultKeySelector(left, right), extracted)
                    }))
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

class LeapfrogTrieJoinAlgorithm(sc: SparkContext) {
    def lftj(rdds: Array[RDD[Array[Int]]], relationCount: Int,  variableCount: Int,
             rddIndexToRelations: Array[Array[Int]], relationVariableRedirects: Array[Array[(Int, Int)]],
             relationVariableIndices: Array[Array[Int]]): RDD[Array[Any]] = {
        // TODO: broadcast vs cogroup
        val broadcasts = rdds.map(rdd => {
            sc.broadcast(rdd.collect())
        })

        val parallelism = sc.defaultParallelism
        // TODO: support parallelisms which are not a cube number
        val p = Math.cbrt(parallelism).toInt
        val hypercubeConfiguration = Array(p,p,p)

        val variableRelatedRelationBuffers = Array.fill(variableCount)(ArrayBuffer.empty[Int])
        for (t <- relationVariableRedirects.zipWithIndex) {
            val redirects = t._1
            val relationIndex = t._2
            for (redirect <- redirects)
                variableRelatedRelationBuffers(redirect._2).append(relationIndex)
        }
        val variableRelatedRelations = variableRelatedRelationBuffers.map(b => b.sorted.toArray)

        def getPartitionLocations(partition: Int): Array[Int] = {
            var remain = partition
            val base = hypercubeConfiguration.tail.foldRight(List(1))((size, list) => (list.head * size) :: list)
            val result = Array.fill(hypercubeConfiguration.length)(0)
            for (i <- hypercubeConfiguration.indices) {
                result(i) = remain / base(i)
                remain = remain % base(i)
            }
            result
        }

        def compute(partition: Int, iterator: Iterator[Int]): Iterator[Array[Any]] = {
            val locations = getPartitionLocations(partition)
            val relations = Array.fill(relationCount)(ArrayBuffer.empty[Array[Int]])
            broadcasts.zip(rddIndexToRelations).foreach(t => {
                val broadcast = t._1
                val relationIndices = t._2
                val content = broadcast.value
                for (tuple <- content) {
                    for (relationIndex <- relationIndices) {
                        if (relationVariableRedirects(relationIndex).forall(redirect =>
                            tuple(redirect._1) % hypercubeConfiguration(redirect._2) == locations(redirect._2))) {
                            // TODO: no need to create a new array if all fields are needed in order
                            relations(relationIndex).append(relationVariableIndices(relationIndex).map(i => tuple(i)))
                        }
                    }
                }
            })

            val iterator = new LeapfrogTrieJoinIterator(relations.map(b => b), variableRelatedRelations)
            iterator.init()
            iterator
        }

        sc.parallelize(0 until parallelism, parallelism).mapPartitionsWithIndex(compute)
    }
}

object TimestampFormatter {
    val DATE_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]")

    class parser(s: String) {
        def parseToTimestamp: Long = LocalDateTime.parse(s, DATE_TIME_FORMATTER).atZone(ZoneOffset.UTC).toInstant.toEpochMilli
    }

    class printer(ts: Long) {
        def printAsString: String = DATE_TIME_FORMATTER.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("UTC")))
    }
}

class RegexPattern(r: String) {
    def toPattern: Pattern = Pattern.compile(r)
}