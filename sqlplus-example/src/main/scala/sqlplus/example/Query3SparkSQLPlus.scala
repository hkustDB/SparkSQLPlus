package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query3SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query3SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(s"${args.head}/graph.dat").map(row => {
			val f = row.split(",")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		v1.count()
		val v2 = v1.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v1.map(f => (f(1), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()
		val longLessThan = (x: Long, y: Long) => x < y

		val v4 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v5 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v6 = v5.appendExtraColumn(v4)
		val v7 = v6.reKeyBy(x => x(1).asInstanceOf[Int])
		val v8 = v7.groupBy()
		val v9 = v8.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v10 = v9.extractFieldInHeadElement(2)
		val v11 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v12 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v13 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v14 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v15 = v14.appendExtraColumn(v12)
		val v16 = v15.appendExtraColumn(v13)
		val v17 = v16.reKeyBy(x => x(0).asInstanceOf[Int])
		val v18 = v17.groupBy()
		val v19 = v18.constructTreeLikeArray[Long, Long, Long, Long](3, 2, (x: Long, y: Long) => longLessThan(y, x), (x: Long, y: Long) => longLessThan(y, x))
		val v20 = v19.createDictionary()
		val v21 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v22 = v21.appendExtraColumn(v10)
		val v23 = v22.appendExtraColumn(v11)
		val v24 = v23.reKeyBy(x => x(1).asInstanceOf[Int])
		val v25 = v24.appendExtraColumn[Long, Long, Long, Long, Long](v20, 2, (x: Long, y: Long) => longLessThan(y, x))
		val v26 = v25.reKeyBy(x => x(0).asInstanceOf[Int])
		val v27 = v26.filter(x => longLessThan(x._2(3).asInstanceOf[Long], x._2(4).asInstanceOf[Long]))

		val v28 = v27.map(t => (t._2(1).asInstanceOf[Int], Array(t._2(0), t._2(2), t._2(3))))
		val v29 = v28.enumerateWithTwoComparisons[Long, Long, Long, Long, Long, Long, Int](v19, 1, 2, Array(2), Array(0, 1, 2, 3), (l, r) => (l(0).asInstanceOf[Int]))
		val v30 = v29.enumerateWithOneComparison[Long, Long, Long, Long, Int](v9, 4, 2, (x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2, 3, 4), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v30.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query3-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
