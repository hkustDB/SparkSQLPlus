package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query4SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query4SparkSQLPlus")
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
		val v5 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v6 = v5.appendExtraColumn(v4)
		val v7 = v6.reKeyBy(x => x(1).asInstanceOf[Int])
		val v8 = v7.groupBy()
		val v9 = v8.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v10 = v9.extractFieldInHeadElement(2)
		val v11 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v12 = v11.appendExtraColumn(v10)
		val v13 = v12.reKeyBy(x => x(1).asInstanceOf[Int])
		val v14 = v13.groupBy()
		val v15 = v14.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v16 = v15.extractFieldInHeadElement(2)
		val v17 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v18 = v17.appendExtraColumn(v3)
		val v19 = v18.reKeyBy(x => x(0).asInstanceOf[Int])
		val v20 = v19.appendExtraColumn(v16)
		val v21 = v20.filter(x => longLessThan(x._2(3).asInstanceOf[Long], x._2(2).asInstanceOf[Long]))

		val ts1 = System.currentTimeMillis()
		val cnt = v21.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query4-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query4-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
