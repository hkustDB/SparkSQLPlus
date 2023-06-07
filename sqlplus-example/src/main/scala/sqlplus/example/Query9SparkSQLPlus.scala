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

		val longLessThanOrEqualTo = (x: Long, y: Long) => x <= y

		val pattern2 = "^.*B.*$".toPattern
		val match2 = (s: String) => pattern2.matcher(s).matches()

		val doubleLessThan = (x: Double, y: Double) => x < y

		val pattern3 = "^.*S.*$".toPattern
		val match3 = (s: String) => pattern3.matcher(s).matches()

		val longGreaterThanOrEqualTo = (x: Long, y: Long) => x >= y

		val v1 = spark.sparkContext.textFile(s"${args.head}/trade.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toLong, fields(1).parseToTimestamp, fields(2), fields(3), fields(4).toLong, fields(5).toDouble)
		}).persist()
		v1.count()

		val v2 = v1.keyBy(x => (x(3).asInstanceOf[String], x(4).asInstanceOf[Long]))
		val v3 = v2.filter(x => match3(x._2(2).asInstanceOf[String]))
		val v4 = v3.groupBy()
		val v5 = v4.sortValuesWith[Long, Long, Long, Long](1, (x: Long, y: Long) => longLessThanOrEqualTo(y, x)).persist()
		val v6 = v5.extractFieldInHeadElement(1)
		val v7 = v2.appendExtraColumn(v6)
		val v8 = v7.reKeyBy(x => x(0).asInstanceOf[Long])
		val v9 = v8.filter(x => longLessThanOrEqualTo(x._2(1).asInstanceOf[Long], x._2(6).asInstanceOf[Long]))
		val v10 = v9.filter(x => match2(x._2(2).asInstanceOf[String]))

		val v11 = v10.map(t => ((t._2(3).asInstanceOf[String], t._2(4).asInstanceOf[Long]), Array(t._2(0), t._2(1), t._2(2), t._2(5))))
		val v12 = v11.enumerateWithMoreThanTwoComparisons[Long, Long, Long, Long, (String, Long)](v5, 1, 1, (x: Long, y: Long) => longLessThanOrEqualTo(x, y), (l, r) => (longGreaterThanOrEqualTo((l(1).asInstanceOf[Long] + 7776000000L).asInstanceOf[Long], r(1).asInstanceOf[Long]) && doubleLessThan((l(3).asInstanceOf[Double] * 1.2d).asInstanceOf[Double], r(5).asInstanceOf[Double])), Array(0, 1, 2, 3), Array(0, 1, 2, 3, 4, 5))

		val ts1 = System.currentTimeMillis()
		val cnt = v12.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query9-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query9-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
