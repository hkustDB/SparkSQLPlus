package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query3SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query3SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val longLessThan = (x: Long, y: Long) => x < y

		val v1 = spark.sparkContext.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		}).persist()
		v1.count()

		val v2 = v1.map(fields => ((fields(0)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v4 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v5 = v4.appendExtraColumn(v3)
		val v6 = v5.reKeyBy(x => x(1).asInstanceOf[Int])
		val v7 = v6.groupBy()
		val v8 = v7.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v9 = v8.extractFieldInHeadElement(2)
		val v10 = v1.map(fields => ((fields(0)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v10.count()
		val v11 = v10.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v12 = v1.map(fields => ((fields(0)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v12.count()
		val v13 = v12.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v14 = v4.appendExtraColumn(v9)
		val v15 = v14.appendExtraColumn(v13)
		val v16 = v15.reKeyBy(x => x(1).asInstanceOf[Int])
		val v17 = v16.groupBy()
		val v18 = v17.constructTreeLikeArray[Long, Long, Long, Long](2, 3, (x: Long, y: Long) => longLessThan(x, y), (x: Long, y: Long) => longLessThan(x, y))
		val v19 = v18.createDictionary()
		val v20 = v1.map(fields => ((fields(1)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v20.count()
		val v21 = v20.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v22 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v23 = v22.appendExtraColumn(v11)
		val v24 = v23.reKeyBy(x => x(0).asInstanceOf[Int])
		val v25 = v24.appendExtraColumn[Long, Long, Long, Long, Long](v19, 2, (x: Long, y: Long) => longLessThan(x, y))
		val v26 = v25.reKeyBy(x => x(1).asInstanceOf[Int])
		val v27 = v26.appendExtraColumn(v21)
		val v28 = v27.reKeyBy(x => x(0).asInstanceOf[Int])
		val v29 = v28.filter(x => longLessThan(x._2(3).asInstanceOf[Long], x._2(4).asInstanceOf[Long]))

		val v30 = v29.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1), t._2(2), t._2(4))))
		val v31 = v30.enumerateWithTwoComparisons[Long, Long, Long, Long, Long, Long, Int](v18, 1, 2, Array(0, 1, 2), Array(1, 3), (l, r) => (r(0).asInstanceOf[Int]))
		val v32 = v31.enumerateWithOneComparison[Long, Long, Long, Long, Int](v8, 1, 2, (x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2, 3, 4), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v32.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query3-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
