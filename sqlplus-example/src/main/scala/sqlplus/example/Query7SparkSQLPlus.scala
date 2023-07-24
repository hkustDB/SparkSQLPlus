package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query7SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query7SparkSQLPlus")
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
		val v4 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v5 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v6 = v5.appendExtraColumn(v4)
		val v7 = v6.reKeyBy(x => x(0).asInstanceOf[Int])
		val v8 = v7.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(0).asInstanceOf[Int]))
		val v9 = v8.groupBy()
		val v10 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v11 = v10.appendExtraColumn(v3)
		val v12 = v11.reKeyBy(x => x(1).asInstanceOf[Int])
		val v13 = v12.filter(x => longLessThan(x._2(2).asInstanceOf[Long], x._2(1).asInstanceOf[Int]))
		val v14 = v13.groupBy()
		val v15 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v16 = v15.semiJoin(v8)
		val v17 = v16.reKeyBy(x => x(0).asInstanceOf[Int])
		val v18 = v17.semiJoin(v13)

		val v19 = v18.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1))))
		val v20 = v19.enumerateWithoutComparison(v14, Array(), Array(0, 1, 2), (l, r) => (l(0).asInstanceOf[Int]))
		val v21 = v20.enumerateWithoutComparison(v9, Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v21.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query7-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query7-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
