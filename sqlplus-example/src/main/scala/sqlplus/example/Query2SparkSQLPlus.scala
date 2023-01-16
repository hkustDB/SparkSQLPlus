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
		sc.defaultParallelism
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt, fields(2).toInt)
		})

		val v2 = bag(v1, x => Tuple1(x(1)), v1, x => Tuple1(x(0)), x => (x(0).asInstanceOf[Int], x(4).asInstanceOf[Int]), v1, x => (x(1).asInstanceOf[Int], x(0).asInstanceOf[Int]), Array(7, 2, 3, 5, 6, 8))
		val v3 = bag(v1, x => Tuple1(x(1)), v1, x => Tuple1(x(0)), x => (x(0).asInstanceOf[Int], x(4).asInstanceOf[Int]), v1, x => (x(1).asInstanceOf[Int], x(0).asInstanceOf[Int]), Array(7, 2, 3, 5, 6, 8))

		val v4 = v2.keyBy(x => (x(2).asInstanceOf[Int]))
		val v5 = v4.appendExtraColumn(x => ((x(1).asInstanceOf[Int] + x(3).asInstanceOf[Int]).asInstanceOf[Int] + x(5).asInstanceOf[Int]))
		val v6 = v5.groupBy()
		val v7 = v6.sortValuesWith(6, (x: Int, y: Int) => intLessThan(x, y))
		val v8 = v7.extractFieldInHeadElement(6)
		val v9 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v10 = v9.appendExtraColumn(v8)
		val v11 = v10.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v12 = v11.groupBy()
		val v13 = v12.sortValuesWith(3, (x: Int, y: Int) => intLessThan(x, y))
		val v14 = v13.extractFieldInHeadElement(3)
		val v15 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v16 = v15.appendExtraColumn(v14)
		val v17 = v16.appendExtraColumn(x => ((x(1).asInstanceOf[Int] + x(3).asInstanceOf[Int]).asInstanceOf[Int] + x(5).asInstanceOf[Int]))
		val v18 = v17.filter(x => intLessThan(x._2(6).asInstanceOf[Int], x._2(7).asInstanceOf[Int]))

		val v19 = v18.enumerate(v13, 7, 3, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1, 2, 3, 4, 5, 7), Array(0, 2, 3))
		val v20 = v19.reKeyBy(x => (x(7).asInstanceOf[Int]))
		val v21 = v20.enumerate(v7, 6, 6, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1, 2, 3, 4, 5, 6, 7, 8), Array(0, 1, 3, 4, 5, 6))
		val v22 = v21.map(t => Array(t._2(9), t._2(7), t._2(10), t._2(7), t._2(12), t._2(11), t._2(12), t._2(9), t._2(13), t._2(0), t._2(2), t._2(1), t._2(2), t._2(4), t._2(3), t._2(4), t._2(0), t._2(5), t._2(7), t._2(0), t._2(8)))

		val ts1 = System.currentTimeMillis()
		val cnt = v22.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query2-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query2-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
