package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query6SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query6SparkSQLPlus")
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

		val v4 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v5 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v6 = v5.appendExtraColumn(v4)
		val v7 = v6.reKeyBy(x => x(1).asInstanceOf[Int])
		val v8 = v7.groupBy()
		val v9 = v8.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v10 = v9.extractFieldInHeadElement(2)
		val v11 = v5.appendExtraColumn(v10)
		val v12 = v11.reKeyBy(x => x(1).asInstanceOf[Int])
		val v13 = v12.groupBy()
		val v14 = v13.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v15 = v14.extractFieldInHeadElement(2)
		val v16 = v5.appendExtraColumn(v15)
		val v17 = v16.reKeyBy(x => x(1).asInstanceOf[Int])
		val v18 = v17.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(1).asInstanceOf[Int]))
		val v19 = v2.keyBy(x => x(0).asInstanceOf[Int])
		val v20 = v19.semiJoin(v18)

		val v21 = v20.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1))))
		val v22 = v18.groupBy()
		val v23 = v21.enumerateWithoutComparison(v22, Array(0), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v24 = v23.enumerateWithOneComparison[Long, Long, Int, Long, Int](v14, 1, 2, (x: Long, y: Long) => longLessThan(y, x), Array(0, 1), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v25 = v24.enumerateWithOneComparison[Long, Long, Int, Long, Int](v9, 1, 2, (x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v25.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query6-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query6-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
