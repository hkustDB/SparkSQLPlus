package sqlplus.cqc

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import scala.util.control.Breaks.{break, breakable}

/**
 * Implementation of the CQC Algorithm from https://github.com/hkustDB/SparkCQC.
 */
class ComparisonJoins extends java.io.Serializable{
  // A customer Iterator for enumeration function.
  class CustomIterator[K : ClassTag, K1](iter: Iterator[(K, (Iterable[Array[Any]], Iterable[Array[Array[Any]]]))],
                                     apos : Array[Int],
                                     bpos : Array[Int],
                                     comparison1 : (Int, Int),
                                     comparison2 : (Int, Int),
                                     key : Int,
                                     comparisonFunction : (K1, K1) => Boolean) extends Iterator[(K, Array[Any])] {
    private var nextEle : (K, Array[Any]) = null
    private var initial : Boolean = false
    private var currentKey : (K, (Iterable[Array[Any]], Iterable[Array[Array[Any]]])) = _
    private var iterA : Iterator[Array[Any]] = _
    private var iterB : Array[Array[Any]] = _
    private var Bcount : Int = 0
    private var eleA : Array[Any] = _
    private def testOutput : Boolean = {
      val c = getComparisonConditions(eleA, iterB(Bcount), comparison1, comparison2)
      if (comparisonFunction(c._1, c._2)) {
        val t = constructOutput(apos, bpos, eleA, iterB(Bcount))
        nextEle = (t(key).asInstanceOf[K], t)
        Bcount = Bcount + 1
        true
      } else {
        false
      }
    }
    override def hasNext: Boolean = {
      if (nextEle != null) return true
      if (!initial) {
        initial = true
        currentKey = iter.next()
        iterA = currentKey._2._1.toIterator
        if (currentKey._2._2.size > 1) throw new Exception("Current B is not unique!")
        iterB = currentKey._2._2.head
        eleA = iterA.next()
        Bcount = 0
        if (!testOutput) throw new Exception("The ele has no output")
        else true
      } else {
          if (Bcount < iterB.length) {
            if (!testOutput) {
              Bcount = iterB.length
            } else {
              return true
            }
          }
          if (Bcount >= iterB.length) {
            if (iterA.hasNext) {
              eleA = iterA.next()
              Bcount = 0
              if (!testOutput) { throw new Exception(s"The ele has no output, ${eleA.mkString("Array(", ", ", ")")}, ${iterB(Bcount).mkString("Array(", ", ", ")")}")}
              else true
            } else {
              if (iter.hasNext) {
                currentKey = iter.next()
                iterA = currentKey._2._1.toIterator
                eleA = iterA.next()
                iterB = currentKey._2._2.head
                if (currentKey._2._2.size > 1) throw new Exception("Current B is not unique!")
                Bcount = 0
                if (!testOutput) throw new Exception("The ele has no output")
                else true
              } else {
                false
              }
            }
          } else {
            throw new Exception("unknown error")
          }
      }
    }

    override def next(): (K, Array[Any]) = {
      if (nextEle == null) throw new Exception("next ele not set")
      val t = nextEle
      nextEle = null
      t
    }
  }

  // A customer Iterator for enumerate tree like array.
  class CustomTreeLikeArrayIterator[K : ClassTag, K1, K2](iter : (Iterable[Array[Any]], Iterable[TreeLikeArray[K1, K2]]),
                                             apos : Array[Int],
                                             bpos : Array[Int],
                                             comparison1 : Int,
                                             comparison2 : Int,
                                             key : Int
                                           ) extends Iterator[(K, Array[Any])] {
    private var initial : Boolean = false
    private var leftIter : Iterator[Array[Any]] = null
    private var rightIter : Iterator[Array[Any]] = null
    private var currentLeft : Array[Any] = null
    private var nextOutput : (K, Array[Any]) = null
    override def hasNext : Boolean = {
      if (nextOutput != null) return true
      if (!initial) {
        initial = true
        leftIter = iter._1.toIterator
        currentLeft = leftIter.next()
        rightIter = iter._2.head.enumerationIterator(currentLeft(comparison1).asInstanceOf[K1],
          currentLeft(comparison2).asInstanceOf[K2])
        if (rightIter.hasNext) {
          val j = rightIter.next()
          val t = constructOutput(apos, bpos, currentLeft, j)
          nextOutput = (t(key).asInstanceOf[K], t)
          return true
        } else throw new Exception(s"Initial Error! Right Iter empty! ${currentLeft.mkString("Array(", ", ", ")")}")
      }
      if (rightIter.hasNext) {
        val j = rightIter.next()
        val t = constructOutput(apos, bpos, currentLeft, j)
        nextOutput = (t(key).asInstanceOf[K], t)
        return true
      } else {
        if (leftIter.hasNext) {
          currentLeft = leftIter.next()
          rightIter = iter._2.head.enumerationIterator(currentLeft(comparison1).asInstanceOf[K1],
            currentLeft(comparison2).asInstanceOf[K2])
          if (rightIter.hasNext) {
            val j = rightIter.next()
            val t = constructOutput(apos, bpos, currentLeft, j)
            nextOutput = (t(key).asInstanceOf[K], t)
            return true
          } else throw new Exception(s"Right Iter empty! ${currentLeft.mkString("Array(", ", ", ")")}")
        }
      }
      false
    }

    override def next() : (K, Array[Any]) = {
      if (nextOutput == null) throw new Exception("next Output empty!")
      val t = nextOutput
      nextOutput = null
      t
    }
  }

  // Select output attributes for given position information
  def constructOutput(apos : Array[Int],
                      bpos : Array[Int],
                      i : Array[Any],
                      j : Array[Any]
                     ) : Array[Any] = {
    val tempArray = Array.fill[Any](apos.length+bpos.length)(0)
    var count = 0
    while (count < apos.length) {
      tempArray(count) = i(apos(count))
      count = count+1
    }
    while (count < apos.length+bpos.length) {
      tempArray(count) = j(bpos(count-apos.length))
      count = count+1
    }
    tempArray
  }

  // Performing binary search over a sorted list based on the given comparison function
  def binarySearch[K1, K2](array : Array[(K1, K2)],
                                   value : K1,
                                   comparisonFunction : (K1, K1) => Boolean
                                  ) : K2 = {
    var left = 0
    var right = array.length
    while (left < right) {
      val mid = (left + right) / 2
      if (comparisonFunction(array(mid)._1, value)) {
        left = mid+1
      } else {
        right = mid
      }
    }
    if (left == 0) throw new Exception("The first one is not minimal")
    array(left-1)._2
  }

  // Standard semijoin, not used in this experiment
  def semijoin[K : ClassTag](RDD1 : RDD[(K, Array[Any])],
               RDD2 : RDD[(K, Array[Any])],
               key : Int) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMap(pair => {
      val result = ArrayBuffer[(K, Array[Any])]()
      if (pair._2._2.nonEmpty) {
        for (i<- pair._2._1) {
          result.append((i(key).asInstanceOf[K], i))
        }
      }
      result
    })
  }

  // A semijoin implementation for the case with degree 2 edge without using 2D structure.
  def semijoin[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[(K1, K2)])],
                                     RDD2 : RDD[(K, Array[Any])],
                                     comparison1 : Int,
                                     comparison2 : Int,
                                     comparisonFunction1 : (K1, K1) => Boolean,
                                     comparisonFunction2 : (K2, K2) => Boolean) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMapValues(x => {
      val result = ListBuffer[Array[Any]]()
      if (x._1.size > 1) throw new Exception("Error! GroupBy Size error")
      if (x._1.size == 1) {
        for (i <- x._2) {
          val j = x._1.head
          if (comparisonFunction1(j.head._1,i(comparison1).asInstanceOf[K1])) {
            val t = binarySearch(j,i(comparison1).asInstanceOf[K1], comparisonFunction1)
            if (comparisonFunction2(t, i(comparison2).asInstanceOf[K2])) {
              result.append(i)
            }
          }
        }
      }
      result
    })
  }

  // A semijoin implementation for the case with degree 1 edge.
  def semijoin[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
               RDD2 : RDD[(K, Array[Any])],
               comparison : Int,
               key : Int,
               comparisonFunction : (K1, K1) => Boolean) : RDD[(K,Array[Array[Any]])] = {
    semijoinWithSort(RDD1, RDD2, comparison, comparisonFunction, key)
  }

  // A semijoin implementation for the case with degree 2 edge without using 2D structure.
  def semijoin[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                                     RDD2 : RDD[(K, Array[(K1, K2)])],
                                     comparison1 : Int,
                                     comparisonFunction : (K1, K1) => Boolean,
                                     key : Int) : RDD[(K, Array[Array[Any]])] = {
    RDD1.cogroup(RDD2).flatMap( pair => {
      val result = ListBuffer[(K, Array[Any])]()
      for (i <- pair._2._1) {
        if (pair._2._2.size > 1) throw new Exception("TreeLikeArray Index size error! Group By not unique!")
        if (pair._2._2.size == 1) {
          val j = pair._2._2.head
          if (comparisonFunction(j(0)._1, i(comparison1).asInstanceOf[K1])) {
            val append = binarySearch(j, i(comparison1).asInstanceOf[K1], comparisonFunction)
            val tempArray = i :+ append
            val k : K = if (key == -1) pair._1
            else i(key).asInstanceOf[K]
            result.append((k, tempArray))
          }
        }
      }
      result
    }).groupByKey.mapValues(x => x.toArray.sortWith((i, j) =>
        // seems buggy here? the last element of each row should be in type K2(return type of binarySearch)?
        // so the asInstanceOf[K1] may throw exception when K1 != K2?
        // Also, it seems that we don't to sort the result in any order actually
      comparisonFunction(i.last.asInstanceOf[K1], j.last.asInstanceOf[K1])))
  }

  // the semijoin at line 236 seems buggy
  // this version drops the groupByKey and sortWith at the last
  def semijoin2[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                                     RDD2 : RDD[(K, Array[(K1, K2)])],
                                     comparison1 : Int,
                                     comparisonFunction : (K1, K1) => Boolean,
                                      key : Int) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMap( pair => {
      val result = ListBuffer[(K, Array[Any])]()
      for (i <- pair._2._1) {
        if (pair._2._2.size > 1) throw new Exception("TreeLikeArray Index size error! Group By not unique!")
        if (pair._2._2.size == 1) {
          val j = pair._2._2.head
          if (comparisonFunction(j(0)._1, i(comparison1).asInstanceOf[K1])) {
            val append = binarySearch(j, i(comparison1).asInstanceOf[K1], comparisonFunction)
            val tempArray = i :+ append
            val k : K = if (key == -1) pair._1
            else i(key).asInstanceOf[K]
            result.append((k, tempArray))
          }
        }
      }
      result
    })
  }

  // A semijoin implementation for the case with degree 2 edge using 2D structure.
  def semijoin[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                             RDD2 : RDD[(K, Array[Any])],
                             comparison : Int,
                             comparison1 : Int,
                             comparison2 : Int,
                             comparisonFunction1 : (K1, K1) => Boolean,
                             comparisonFunction2 : (K2, K2) => Boolean,
                             key : Int = -1) : RDD[(K,TreeLikeArray[K1, K2])] = {
    semijoinWithSort(RDD1, RDD2, comparison, comparison1, comparison2, comparisonFunction1, comparisonFunction2, key)
  }

  // Internal function for getting comparison conditions.
  private def getComparisonConditions[K1](i : Array[Any],
                                      j : Array[Any],
                                      comparison1 : (Int, Int),
                                      comparison2 : (Int, Int)

                                     ) : (K1, K1) = {
    val c1 : K1 = if (comparison1._1 == 1)
      i(comparison1._2).asInstanceOf[K1]
    else
      j(comparison1._2).asInstanceOf[K1]
    val c2 : K1 = if (comparison2._1 == 1)
      i(comparison2._2).asInstanceOf[K1]
    else
      j(comparison2._2).asInstanceOf[K1]
    (c1, c2)
  }

  // A customer Iteraror for semijoin functions.
  class semiJoinIterator[K : ClassTag, K1](iter : (K, (Iterable[Array[Any]], Iterable[Array[Any]])),
                                           comparison : Int,
                                           key : Int = -1
                                          ) extends Iterator[(K, Array[Any])] {
    private var initial : Boolean = false
    private var iterA : Iterator[Array[Any]] = _
    private var valueB : Array[Any] = _
    override def hasNext: Boolean = {
      if (!initial) {
        initial = true
        iterA = iter._2._1.toIterator
        if (iter._2._2.size > 1) throw new Exception("The second RDD is not distinct!")
        if (iter._2._2.nonEmpty) valueB = iter._2._2.head
      }
      if (iter._2._2.isEmpty) false
      else iterA.hasNext
    }

    override def next(): (K, Array[Any]) = {
      if (iterA.hasNext) {
        val valueA = iterA.next()
        val tempArray = valueA :+ valueB(comparison)
        val k : K = if (key == -1) iter._1
        else valueA(key).asInstanceOf[K]
        (k, tempArray)
      } else {
        throw new Exception("Required output while no next element")
      }

    }
  }

  // Semijoin implementation with sorted array output.
  private def semijoinWithSort[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                                             RDD2 : RDD[(K, Array[Any])],
                                             comparison : Int,
                                             comparisonFunction : (K1, K1) => Boolean,
                                             key : Int = -1) : RDD[(K,Array[Array[Any]])] = {
    RDD1.cogroup(RDD2).flatMap(pair => new semiJoinIterator(pair, comparison, key)).groupByKey.mapValues(x =>
      x.toArray.sortWith((i, j) =>
        comparisonFunction(i.last.asInstanceOf[K1], j.last.asInstanceOf[K1])))
  }

  // Semijoin implementation without sorted array output.
  def semijoinWithoutSort[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                                                    RDD2 : RDD[(K, Array[Any])],
                                                    resultKey : Int) : RDD[(K,Array[Any])] = {
    RDD1.cogroup(RDD2).flatMap(pair => new semiJoinIterator(pair, 0, resultKey))
  }

  // semijoin implementation with 2D structure output
  private def semijoinWithSort[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                                                 RDD2 : RDD[(K, Array[Any])],
                                                 comparison : Int,
                                                 comparison1 : Int,
                                                 comparison2 : Int,
                                                 comparisonFunction1 : (K1, K1) => Boolean,
                                                 comparisonFunction2 : (K2, K2) => Boolean,
                                                 key : Int) : RDD[(K,TreeLikeArray[K1, K2])] = {
    RDD1.cogroup(RDD2).flatMap(
      pair => new semiJoinIterator(pair, comparison, key)
    ).groupByKey.mapValues(
      x => new TreeLikeArray[K1, K2](x.toArray,
        comparison1, comparison2,
        comparisonFunction1, comparisonFunction2))
  }

    // A groupBy operator for 1D cases
  def groupBy[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                      comparison : Int,
                      comparisonFunction : (K1, K1) => Boolean,
                      key : Int = -1) : RDD[(K, Array[Array[Any]])] ={
    val T = if (key == -1 ) RDD1.mapPartitions(x => {
      val t = mutable.HashMap[K, ArrayBuffer[Array[Any]]]()
      for (i <- x) {
        val k = t.getOrElse(i._1, new ArrayBuffer[Array[Any]]())
        k.append(i._2)
        t.put(i._1, k)
      }
      val y = t.toIterator
      new Iterator[(K, Array[Array[Any]])]{
        override def hasNext: Boolean = y.hasNext

        override def next(): (K, Array[Array[Any]]) = {
          val i = y.next()
          (i._1, i._2.toArray.sortWith(
            (i, j) => comparisonFunction(i(comparison).asInstanceOf[K1], j(comparison).asInstanceOf[K1])))
        }
      }
    })
    else RDD1.map(x => (x._2(key).asInstanceOf[K], x._2)).groupByKey().mapValues( x =>
      x.toArray.sortWith((i, j) =>
        comparisonFunction(i(comparison).asInstanceOf[K1], j(comparison).asInstanceOf[K1]))
    )
    T

  }

  // Obtain Minimal for a given comparison atom
  def getMinimal[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                                comparison : Int,
                                comparisonFunction : (K1, K1) => Boolean) : RDD[(K, Array[Any])] ={
    RDD1.mapPartitions(x => {
        val t = mutable.HashMap[K, K1]()
        for (i <- x) {
          if (t.contains(i._1)) {
            val k : K1 = t.getOrElse(i._1, throw new Exception("Unknown Exception"))
            if (comparisonFunction(i._2(comparison).asInstanceOf[K1], k)) {
              t.put(i._1, i._2(comparison).asInstanceOf[K1])
            }
          } else {
            t.put(i._1, i._2(comparison).asInstanceOf[K1])
          }
        }
        val y = t.toIterator
        new Iterator[(K, Array[Any])]{
          override def hasNext: Boolean = y.hasNext

          override def next(): (K, Array[Any]) = {
            val t = y.next()
            (t._1, Array[Any](t._2))
          }
        }
      }, preservesPartitioning = true)
  }

  // A groupBy operator for 2D cases.
  def groupBy[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
              comparison1 : Int,
              comparison2 : Int,
              comparisonFunction1 : (K1, K1) => Boolean,
              comparisonFunction2 : (K2, K2) => Boolean) : RDD[(K, TreeLikeArray[K1, K2])] = {
    RDD1.groupByKey().mapValues(
      x => new TreeLikeArray[K1, K2](x.toArray,
        comparison1, comparison2,
        comparisonFunction1, comparisonFunction2)
    )
  }

  // Getting the minimal value for a sorted list.
  def semijoinSortToMax[K : ClassTag](RDD1 : RDD[(K, Array[Array[Any]])]) : RDD[(K, Array[Any])] = {
    RDD1.mapValues(x => {
      x(0).last
    }).map(x => (x._1, Array[Any](x._2)))
  }

  // A enumeration function for key-key 1D case.
  def enumeration1[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                   RDD2 : RDD[(K, Array[Any])],
                   apos : Array[Int],
                   bpos : Array[Int],
                   comparison1 : (Int, Int),
                   comparison2 : (Int, Int),
                   key : Int,
                   comparisonFunction : (K1, K1) => Boolean) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMap(pair => {
      val result = ListBuffer[(K, Array[Any])]()
      for (i <- pair._2._1; j <- pair._2._2) {
        breakable{

            val c = getComparisonConditions(i, j, comparison1, comparison2)
            if (comparisonFunction(c._1, c._2)) {

              val tempArray = constructOutput(apos, bpos, i, j)
              if (key != -1) result.append((tempArray(key).asInstanceOf[K], tempArray))
              else result.append((pair._1, tempArray))
            } else break
          }
        }
      result
    })
  }

  // A enumeration function for key-key 1D case, without redistribution based on new key.
  def enumerationnp[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                                     RDD2 : RDD[(K, Array[Any])],
                                     apos : Array[Int],
                                     bpos : Array[Int],
                                     comparison1 : (Int, Int),
                                     comparison2 : (Int, Int),
                                     comparisonFunction : (K1, K1) => Boolean) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMapValues(pair => {
      val result = ListBuffer[Array[Any]]()
      for (i <- pair._1; j <- pair._2) {
        breakable{
          val c = getComparisonConditions(i, j, comparison1, comparison2)
          if (comparisonFunction(c._1, c._2)) {

            val tempArray = constructOutput(apos, bpos, i, j)
            result.append(tempArray)
          } else break
        }
      }
      result
    })
  }

  // Enumeration function for 2D cases without using 2D structure
  def enumeration[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                                 RDD2 : RDD[(K, Array[Array[Any]])],
                                 apos : Array[Int],
                                 bpos : Array[Int],
                                 comparison1 : Int,
                                 comparison2 : Int,
                                 comparisonFunction1 : (K1, K1) => Boolean,
                                 comparisonFunction2 : (K2, K2) => Boolean,
                                 key : Int = -1) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMap(pair => {
      if (pair._2._2.nonEmpty && pair._2._1.nonEmpty) {
        if (pair._2._2.size != 1) throw new Exception(s"The grouped relation has multiple elements, ${pair._2._2.size}.")
        new Iterator[(K, Array[Any])] {
          private var hasOutput = true
          private var iterA: Iterator[Array[Any]] = _
          private var iterB: Iterator[Array[Any]] = _
          private var i: Array[Any] = _
          private var initial = false
          private var nextOutput: Array[Any] = null

          override def hasNext: Boolean = {
            if (nextOutput != null) return true
            if (!initial) {
              hasOutput = false
              initial = true
              iterA = pair._2._1.toIterator
              while (!hasOutput && (iterA.hasNext || iterB.hasNext)) {
                i = iterA.next
                iterB = pair._2._2.head.toIterator
                while (!hasOutput && iterB.hasNext) {
                  val j = iterB.next
                  if (comparisonFunction1(j(comparison1).asInstanceOf[K1], i(comparison1).asInstanceOf[K1])) {
                    if (comparisonFunction2(j(comparison2).asInstanceOf[K2], i(comparison2).asInstanceOf[K2])) {
                      nextOutput = constructOutput(apos, bpos, i, j)
                      hasOutput = true
                      return true
                    }
                  } else {
                    iterB = Iterator.empty
                  }
                }
              }
            }
            if (hasOutput) {
              hasOutput = false
              while (!hasOutput && (iterA.hasNext || iterB.hasNext)) {
                if (!iterB.hasNext) {
                  iterB = pair._2._2.head.toIterator
                  i = iterA.next()
                }
                while (!hasOutput && iterB.hasNext) {
                  val j = iterB.next
                  if (comparisonFunction1(j(comparison1).asInstanceOf[K1], i(comparison1).asInstanceOf[K1])) {
                    if (comparisonFunction2(j(comparison2).asInstanceOf[K2], i(comparison2).asInstanceOf[K2])) {
                      nextOutput = constructOutput(apos, bpos, i, j)
                      hasOutput = true
                      return true
                    }
                  } else {
                    iterB = Iterator.empty
                  }
                }
              }
              hasOutput
            } else false
          }

          override def next(): (K, Array[Any]) = {
            if (!initial) {
              if (this.hasNext) {
                val t = nextOutput
                nextOutput = null
                if (key == -1) (pair._1, t)
                else (t(key).asInstanceOf[K], t)
              } else {
                null
              }
            } else {
              val t = nextOutput
              nextOutput = null
              if (key == -1) (pair._1, t)
              else (t(key).asInstanceOf[K], t)
            }
          }
        }
      } else {
        Iterator.empty
      }
    })
  }

  // Enumeration function for 2D key-key join without 2D structure, not used in the experiment.
  def enumerationt[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                                        RDD2 : RDD[(K, Array[Any])],
                                        apos : Array[Int],
                                        bpos : Array[Int],
                                        comparison1 : Int,
                                        comparison2 : Int,
                                        comparisonFunction1 : (K1, K1) => Boolean,
                                        comparisonFunction2 : (K2, K2) => Boolean) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).flatMapValues(pair => {
      new Iterator[Array[Any]] {
        private var hasOutput = true
        private var iterA : Iterator[Array[Any]] = _
        private var iterB : Iterator[Array[Any]] = _
        private var i : Array[Any] = _
        private var initial = false
        private var nextOutput : Array[Any] = null
        override def hasNext: Boolean = {
          if (nextOutput != null) return true
          if (!initial) {
            hasOutput = false
            initial = true
            iterA = pair._1.toIterator
            while (!hasOutput && (iterA.hasNext || iterB.hasNext)) {
              i = iterA.next
              iterB = pair._2.toIterator
              while (!hasOutput && iterB.hasNext) {
                val j = iterB.next
                if (comparisonFunction1(i(comparison1).asInstanceOf[K1], j(comparison1).asInstanceOf[K1]))
                  if (comparisonFunction2(i(comparison2).asInstanceOf[K2], j(comparison2).asInstanceOf[K2])) {
                    nextOutput =constructOutput(apos, bpos, i, j)
                    hasOutput = true
                    return true
                  }
              }
            }
          }
          if (hasOutput) {
            hasOutput = false
            while (!hasOutput && (iterA.hasNext || iterB.hasNext)) {
              if (!iterB.hasNext) {
                iterB = pair._2.toIterator
                i = iterA.next()
              }
              while (!hasOutput && iterB.hasNext) {
                val j = iterB.next
                if (comparisonFunction1(i(comparison1).asInstanceOf[K1], j(comparison1).asInstanceOf[K1]))
                  if (comparisonFunction2(i(comparison2).asInstanceOf[K2], j(comparison2).asInstanceOf[K2])) {
                    nextOutput =constructOutput(apos, bpos, i, j)
                    hasOutput = true
                    return true
                  }

              }
            }
            hasOutput
          } else false
        }

        override def next(): Array[Any] = {
          if (!initial) {
            if (this.hasNext) {
              val t = nextOutput
              nextOutput = null
              t
            } else {
              throw new Exception("No output error!")
            }
          } else {
            if (nextOutput != null) {
              val t = nextOutput
              nextOutput = null
              t
            } else {
              throw new Exception("No output error!")
            }
          }
        }
      }
    })
  }

  // Enumeration function with 2D data structure support
  def enumeration[K : ClassTag, K1, K2](RDD1 : RDD[(K, Array[Any])],
                                        RDD2 : RDD[(K, TreeLikeArray[K1, K2])],
                                        apos : Array[Int],
                                        bpos : Array[Int],
                                        comparison1 : Int,
                                        comparison2 : Int,
                                        key : Int
                                       ) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).filter(x => (x._2._1.nonEmpty && x._2._2.nonEmpty)).flatMap(
      pairs => {
        new CustomTreeLikeArrayIterator(pairs._2, apos, bpos, comparison1, comparison2, key)
      }
    )
  }

  // enumeration function for 1D non key-key join
  def enumeration[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                  RDD2 : RDD[(K, Array[Array[Any]])],
                  apos : Array[Int],
                  bpos : Array[Int],
                  comparison1 : (Int, Int),
                  comparison2 : (Int, Int),
                  key : Int,
                  comparisonFunction : (K1, K1) => Boolean) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions( x => {
      new CustomIterator[K, K1](x, apos, bpos, comparison1, comparison2, key, comparisonFunction)
    })

  }

  // enumeration function for 1D non key-key join without comparison
  def enumeration[K : ClassTag, K1](RDD1 : RDD[(K, Array[Any])],
                                    RDD2 : RDD[(K, Array[Array[Any]])],
                                    apos : Array[Int],
                                    bpos : Array[Int],
                                    key : Int) : RDD[(K, Array[Any])] = {
    RDD1.cogroup(RDD2).filter(x => x._2._1.nonEmpty && x._2._2.nonEmpty).mapPartitions( x => x.flatMap(
      y => {
        val leftIterator = y._2._1
        val rightGroup = y._2._2
        for {
          leftRow <- leftIterator
          rightIterator <- rightGroup
          rightRow <- rightIterator
        } yield {
          val buffer = ArrayBuffer.empty[Any]
          for (i <- apos) {
            buffer.append(leftRow(i))
          }
          for (i <- bpos) {
            buffer.append(rightRow(i))
          }
          (buffer(key).asInstanceOf[K], buffer.toArray)
        }
      }.toArray.iterator))

  }
}



object ComparisonJoins extends ComparisonJoins {

}