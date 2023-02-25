package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query2SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query2SparkSQLPlus")
		val sc = new SparkContext(conf)
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		}).persist()
		v1.count()

		val v2 = v1.map(fields => Array(fields(0).asInstanceOf[Int], fields(1).asInstanceOf[Int])).cache()
		v2.count()
		val v3 = sc.lftj(Array(v2), 3, 3,
			Array(Array(0, 1, 2)),
			Array(Array((0, 0), (1, 1)), Array((0, 1), (1, 2)), Array((0, 2), (1, 0))),
			Array(Array(0, 1), Array(0, 1), Array(1, 0))).cache()
		val v4 = sc.lftj(Array(v2), 3, 3,
			Array(Array(0, 1, 2)),
			Array(Array((0, 1), (1, 2)), Array((0, 2), (1, 0)), Array((0, 0), (1, 1))),
			Array(Array(0, 1), Array(1, 0), Array(0, 1))).cache()

		val v5 = v3.keyBy(x => (x(1).asInstanceOf[Int]))
		val v6 = v5.appendExtraColumn(x => ((x(0).asInstanceOf[Int] + x(1).asInstanceOf[Int]).asInstanceOf[Int] + x(2).asInstanceOf[Int]))
		val v7 = v6.groupBy()
		val v8 = v7.sortValuesWith(3, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v9 = v8.extractFieldInHeadElement(3)
		val v10 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v11 = v10.appendExtraColumn(v9)
		val v12 = v11.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v13 = v12.groupBy()
		val v14 = v13.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v15 = v14.extractFieldInHeadElement(2)
		val v16 = v4.keyBy(x => (x(1).asInstanceOf[Int]))
		val v17 = v16.appendExtraColumn(v15)
		val v18 = v17.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v19 = v18.appendExtraColumn(x => ((x(1).asInstanceOf[Int] + x(2).asInstanceOf[Int]).asInstanceOf[Int] + x(0).asInstanceOf[Int]))
		val v20 = v19.filter(x => intLessThan(x._2(3).asInstanceOf[Int], x._2(4).asInstanceOf[Int]))

		val v21 = v20.map(t => ((t._2(1).asInstanceOf[Int]), Array(t._2(0), t._2(2), t._2(4))))
		val v22 = v21.enumerateWithOneComparison(v14, 2, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1, 2), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v23 = v22.enumerateWithOneComparison(v8, 2, 3, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1, 3), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v23.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query2-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query2-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
