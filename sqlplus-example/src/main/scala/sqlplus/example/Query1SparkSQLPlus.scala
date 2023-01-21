package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query1SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query1SparkSQLPlus")
		val sc = new SparkContext(conf)
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		}).persist()
		v1.count()
		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()

		val v4 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v5 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v6 = v5.appendExtraColumn(v4)
		val v7 = v6.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v8 = v7.groupBy()
		val v9 = v8.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v10 = v9.extractFieldInHeadElement(2)
		val v11 = v5.appendExtraColumn(v10)
		val v12 = v11.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v13 = v12.groupBy()
		val v14 = v13.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v15 = v14.extractFieldInHeadElement(2)
		val v16 = v5.appendExtraColumn(v15)
		val v17 = v16.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v18 = v17.groupBy()
		val v19 = v18.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v20 = v19.extractFieldInHeadElement(2)
		val v21 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v22 = v21.appendExtraColumn(v20)
		val v23 = v22.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))

		val v24 = v23.map(t => ((t._2(0).asInstanceOf[Int]), Array(t._2(1))))
		val v25 = v24.enumerateWithOneComparison(v19, 0, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v26 = v25.enumerateWithOneComparison(v14, 0, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v27 = v26.enumerateWithOneComparison(v9, 0, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v27.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query1-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query1-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
