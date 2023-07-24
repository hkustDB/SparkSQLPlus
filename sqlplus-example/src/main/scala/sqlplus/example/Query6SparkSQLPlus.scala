package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query6SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query6SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(s"${args.head}/graph.dat").map(row => {
			val f = row.split(",")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		v1.count()
		val v2 = v1.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val longLessThan = (x: Long, y: Long) => x < y

		val v3 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v4 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v5 = v4.appendExtraColumn(v3)
		val v6 = v5.reKeyBy(x => x(1).asInstanceOf[Int])
		val v7 = v6.groupBy()
		val v8 = v7.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v9 = v8.extractFieldInHeadElement(2)
		val v10 = v2.keyBy(x => x(0).asInstanceOf[Int])
		val v11 = v10.groupBy()
		val v12 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v13 = v12.semiJoin(v10)
		val v14 = v13.reKeyBy(x => x(0).asInstanceOf[Int])
		val v15 = v14.groupBy()
		val v16 = v15.sortValuesWith[Long, Long, Int, Int](1, (x: Long, y: Long) => longLessThan(y, x)).persist()
		val v17 = v16.extractFieldInHeadElement(1)
		val v18 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v19 = v18.appendExtraColumn(v9)
		val v20 = v19.reKeyBy(x => x(1).asInstanceOf[Int])
		val v21 = v20.appendExtraColumn(v17)
		val v22 = v21.reKeyBy(x => x(0).asInstanceOf[Int])
		val v23 = v22.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(3).asInstanceOf[Int]))

		val v24 = v23.map(t => (t._2(1).asInstanceOf[Int], Array(t._2(0), t._2(2))))
		val v25 = v24.enumerateWithOneComparison[Long, Long, Long, Int, Int](v16, 1, 1, (x: Long, y: Long) => longLessThan(x, y), Array(0), Array(0), (l, r) => (r(1).asInstanceOf[Int]))
		val v26 = v25.enumerateWithoutComparison(v11, Array(1), Array(0, 1), (l, r) => (l(0).asInstanceOf[Int]))
		val v27 = v26.enumerateWithOneComparison[Long, Long, Int, Long, Int](v8, 1, 2, (x: Long, y: Long) => longLessThan(y, x), Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v27.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query6-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query6-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
