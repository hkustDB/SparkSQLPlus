package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object Query9SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query9SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(s"${args.head}/trade.dat").map(row => {
			val f = row.split(",")
			Array[Any](f(0).toLong, f(1).parseToTimestamp, f(2), f(3), f(4).toLong, f(5).toDouble)
		}).persist()
		v1.count()
		val longLessThanOrEqualTo = (x: Long, y: Long) => x <= y
		val pattern0 = "^.*B.*$".toPattern
		val match0 = (s: String) => pattern0.matcher(s).matches()
		val doubleLessThan = (x: Double, y: Double) => x < y
		val pattern1 = "^.*S.*$".toPattern
		val match1 = (s: String) => pattern1.matcher(s).matches()
		val longGreaterThanOrEqualTo = (x: Long, y: Long) => x >= y

		val v2 = v1.keyBy(x => (x(3).asInstanceOf[String], x(4).asInstanceOf[Long]))
		val v3 = v2.filter(x => match1(x._2(2).asInstanceOf[String]))
		val v4 = v3.groupBy()
		val v5 = v4.sortValuesWith[Long, Long, Long, Long](1, (x: Long, y: Long) => longLessThanOrEqualTo(y, x)).persist()
		val v6 = v5.extractFieldInHeadElement(1)
		val v7 = v1.keyBy(x => (x(3).asInstanceOf[String], x(4).asInstanceOf[Long]))
		val v8 = v7.appendExtraColumn(v6)
		val v9 = v8.reKeyBy(x => x(0).asInstanceOf[Long])
		val v10 = v9.filter(x => longLessThanOrEqualTo(x._2(1).asInstanceOf[Long], x._2(6).asInstanceOf[Long]))
		val v11 = v10.filter(x => match0(x._2(2).asInstanceOf[String]))

		val v12 = v11.map(t => ((t._2(3).asInstanceOf[String], t._2(4).asInstanceOf[Long]), Array(t._2(0), t._2(1), t._2(2), t._2(5))))
		val v13 = v12.enumerateWithMoreThanTwoComparisons[Long, Long, Long, Long, (String, Long)](v5, 1, 1, (x: Long, y: Long) => longLessThanOrEqualTo(x, y), (l, r) => (longGreaterThanOrEqualTo((l(1).asInstanceOf[Long] + 7776000000L).asInstanceOf[Long], r(1).asInstanceOf[Long]) && doubleLessThan((l(3).asInstanceOf[Double] * 1.2d).asInstanceOf[Double], r(5).asInstanceOf[Double])), Array(0, 1, 2, 3), Array(0, 1, 2, 3, 4, 5))

		val ts1 = System.currentTimeMillis()
		val cnt = v13.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query9-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query9-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
