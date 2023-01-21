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
		val v3 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v4 = v2.semiJoin(v3)
		val v5 = v4.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v6 = v5.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))
		val v7 = v2.semiJoin(v6)
		val v8 = v7.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v9 = v8.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))
		val v10 = v2.semiJoin(v9)

		val v11 = v10.map(t => ((t._2(0).asInstanceOf[Int]), Array(t._2(1))))
		val v12 = v9.groupBy()
		val v13 = v11.enumerateWithoutComparison(v12, Array(0), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v14 = v6.groupBy()
		val v15 = v13.enumerateWithoutComparison(v14, Array(0, 1), Array(1), (l, r) => (r(0).asInstanceOf[Int]))
		val v16 = v3.groupBy()
		val v17 = v15.enumerateWithoutComparison(v16, Array(0, 1, 2), Array(0, 1))

		val ts1 = System.currentTimeMillis()
		val cnt = v17.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query8-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query8-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
