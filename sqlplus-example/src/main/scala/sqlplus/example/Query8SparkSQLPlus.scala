package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query8SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query8SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(s"${args.head}/graph.dat").map(row => {
			val f = row.split(",")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		v1.count()
		val intLessThan = (x: Int, y: Int) => x < y

		val v2 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v3 = v2.groupBy()
		val v4 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v5 = v4.groupBy()
		val v6 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v7 = v6.semiJoin(v4)
		val v8 = v7.reKeyBy(x => x(1).asInstanceOf[Int])
		val v9 = v8.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))
		val v10 = v9.groupBy()
		val v11 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v12 = v11.semiJoin(v2)
		val v13 = v12.reKeyBy(x => x(0).asInstanceOf[Int])
		val v14 = v13.semiJoin(v9)
		val v15 = v14.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))

		val v16 = v15.map(t => (t._2(0).asInstanceOf[Int], Array(t._2(1))))
		val v17 = v16.enumerateWithoutComparison(v10, Array(0), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v18 = v17.enumerateWithoutComparison(v5, Array(1), Array(0, 1), (l, r) => (l(0).asInstanceOf[Int]))
		val v19 = v18.enumerateWithoutComparison(v3, Array(0, 1, 2), Array(0, 1))

		val ts1 = System.currentTimeMillis()
		val cnt = v19.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query8-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query8-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
