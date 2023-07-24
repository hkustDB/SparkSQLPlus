package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query2SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query2SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(s"${args.head}/graph.dat").map(row => {
			val f = row.split(",")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		v1.count()
		val intLessThan = (x: Int, y: Int) => x < y

		val v2 = spark.sparkContext.lftj(Array(v1.map(a => a.map(i => i.asInstanceOf[Int]))), 3, 3,
			Array(Array(0, 1, 2)),
			Array(Array((0, 1), (1, 2)), Array((0, 2), (1, 0)), Array((0, 0), (1, 1))),
			Array(Array(0, 1), Array(1, 0), Array(0, 1))).cache()
		val v3 = v2.keyBy(x => x(1).asInstanceOf[Int])
		val v4 = v3.appendExtraColumn(x => ((x(1).asInstanceOf[Int] + x(2).asInstanceOf[Int]).asInstanceOf[Int] + x(0).asInstanceOf[Int]))
		val v5 = v4.groupBy()
		val v6 = v5.sortValuesWith[Int, Int, Int, Int](3, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v7 = v6.extractFieldInHeadElement(3)
		val v8 = spark.sparkContext.lftj(Array(v1.map(a => a.map(i => i.asInstanceOf[Int]))), 3, 3,
			Array(Array(0, 1, 2)),
			Array(Array((0, 2), (1, 0)), Array((0, 0), (1, 1)), Array((0, 1), (1, 2))),
			Array(Array(1, 0), Array(0, 1), Array(0, 1))).cache()
		val v9 = v8.keyBy(x => x(0).asInstanceOf[Int])
		val v10 = v9.appendExtraColumn(x => ((x(2).asInstanceOf[Int] + x(0).asInstanceOf[Int]).asInstanceOf[Int] + x(1).asInstanceOf[Int]))
		val v11 = v10.groupBy()
		val v12 = v11.sortValuesWith[Int, Int, Int, Int](3, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v13 = v12.extractFieldInHeadElement(3)
		val v14 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v15 = v14.appendExtraColumn(v7)
		val v16 = v15.reKeyBy(x => x(0).asInstanceOf[Int])
		val v17 = v16.appendExtraColumn(v13)
		val v18 = v17.filter(x => intLessThan(x._2(3).asInstanceOf[Int], x._2(2).asInstanceOf[Int]))

		val v19 = v18.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1), t._2(2))))
		val v20 = v19.enumerateWithOneComparison[Int, Int, Int, Int, Int](v12, 1, 3, (x: Int, y: Int) => intLessThan(y, x), Array(), Array(0, 1, 2, 3), (l, r) => (l(0).asInstanceOf[Int]))
		val v21 = v20.enumerateWithOneComparison[Int, Int, Int, Int, Int](v6, 3, 3, (x: Int, y: Int) => intLessThan(x, y), Array(0, 1, 2), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v21.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query2-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query2-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
