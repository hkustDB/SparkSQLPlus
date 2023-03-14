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

		val v3 = v1.map(fields => ((fields(0)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()

		val v4 = v1.map(fields => ((fields(1)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v4.count()

		val v5 = v1.map(fields => ((fields(0)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v5.count()

		val v6 = v5.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v7 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v8 = v7.appendExtraColumn(v6)
		val v9 = v8.reKeyBy(x => x(1).asInstanceOf[Int])
		val v10 = v9.groupBy()
		val v11 = v10.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v12 = v11.extractFieldInHeadElement(2)
		val v13 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v14 = v4.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v15 = v7.appendExtraColumn(v12)
		val v16 = v15.appendExtraColumn(v13)
		val v17 = v16.reKeyBy(x => x(1).asInstanceOf[Int])
		val v18 = v17.groupBy()
		val v19 = v18.constructTreeLikeArray[Long, Long, Long, Long](3, 2, (x: Long, y: Long) => longLessThan(x, y), (x: Long, y: Long) => longLessThan(x, y))
		val v20 = v19.createDictionary()
		val v21 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v22 = v21.appendExtraColumn(v14)
		val v23 = v22.reKeyBy(x => x(0).asInstanceOf[Int])
		val v24 = v23.appendExtraColumn[Long, Long, Long, Long, Long](v20, 2, (x: Long, y: Long) => longLessThan(x, y))
		val v25 = v24.reKeyBy(x => x(1).asInstanceOf[Int])
		val v26 = v25.groupBy()
		val v27 = v26.sortValuesWith[Long, Long, Long, Long](3, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v28 = v27.extractFieldInHeadElement(3)
		val v29 = v2.keyBy(x => x(0).asInstanceOf[Int])
		val v30 = v29.appendExtraColumn(v28)
		val v31 = v30.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(1).asInstanceOf[Long]))

		val v32 = v31.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1))))
		val v33 = v32.enumerateWithOneComparison[Long, Long, Long, Long, Int](v27, 0, 3, (x: Long, y: Long) => longLessThan(y, x), Array(0), Array(1, 2), (l, r) => (r(0).asInstanceOf[Int]))
		val v34 = v33.enumerateWithTwoComparisons[Long, Long, Long, Long, Long, Long, Int](v19, 2, 0, Array(0, 1, 2), Array(1, 3), (l, r) => (r(0).asInstanceOf[Int]))
		val v35 = v34.enumerateWithOneComparison[Long, Long, Long, Long, Int](v11, 0, 2, (x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2, 3, 4), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v35.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query3-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
