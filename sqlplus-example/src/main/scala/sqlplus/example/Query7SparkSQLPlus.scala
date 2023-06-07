package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query7SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query7SparkSQLPlus")
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
		val v7 = v6.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(1).asInstanceOf[Int]))
		val v8 = v4.semiJoin(v7)
		val v9 = v8.reKeyBy(x => x(1).asInstanceOf[Int])
		val v10 = v4.semiJoin(v9)
		val v11 = v10.reKeyBy(x => x(1).asInstanceOf[Int])
		val v12 = v11.groupBy()
		val v13 = v12.sortValuesWith[Long, Long, Int, Int](0, (x: Long, y: Long) => longLessThan(y, x)).persist()
		val v14 = v13.extractFieldInHeadElement(0)
		val v15 = v1.map(fields => ((fields(0)), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v15.count()
		val v16 = v15.keyBy(x => x(0).asInstanceOf[Int])
		val v17 = v16.appendExtraColumn(v14)
		val v18 = v17.filter(x => longLessThan(x._2(1).asInstanceOf[Long], x._2(2).asInstanceOf[Int]))

		val v19 = v18.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1))))
		val v20 = v19.enumerateWithOneComparison[Long, Long, Long, Int, Int](v13, 0, 0, (x: Long, y: Long) => longLessThan(x, y), Array(0), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v21 = v9.groupBy()
		val v22 = v20.enumerateWithoutComparison(v21, Array(0, 1), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v23 = v7.groupBy()
		val v24 = v22.enumerateWithoutComparison(v23, Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v24.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query7-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query7-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
