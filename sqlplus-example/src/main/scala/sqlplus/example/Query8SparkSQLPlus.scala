package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query8SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query8SparkSQLPlus")
		val sc = new SparkContext(conf)
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		}).persist()
		v1.count()

		val v2 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v3 = v2.groupBy()
		val v4 = v3.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v5 = v4.extractFieldInHeadElement(1)
		val v6 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v7 = v6.appendExtraColumn(v5)
		val v8 = v7.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v9 = v8.groupBy()
		val v10 = v9.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v11 = v10.extractFieldInHeadElement(2)
		val v12 = v6.appendExtraColumn(v11)
		val v13 = v12.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v14 = v13.groupBy()
		val v15 = v14.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v16 = v15.extractFieldInHeadElement(2)
		val v17 = v6.appendExtraColumn(v16)
		val v18 = v17.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v19 = v18.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))
		val v20 = v19.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(2).asInstanceOf[Int]))

		val v21 = v20.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v22 = v21.enumerate(v15, 0, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0, 1), Array(1))
		val v23 = v22.reKeyBy(x => (x(2).asInstanceOf[Int]))
		val v24 = v23.enumerate(v10, 0, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0, 1, 2), Array(1))
		val v25 = v24.reKeyBy(x => (x(3).asInstanceOf[Int]))
		val v26 = v25.enumerate(v4, 0, 1, (x: Int, y: Int) => intLessThan(x, y), Array(0, 1, 2, 3), Array(1))
		val v27 = v26.map(t => Array(t._2(0), t._2(1), t._2(2), t._2(3), t._2(4)))

		val ts1 = System.currentTimeMillis()
		val cnt = v27.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query8-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query8-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
