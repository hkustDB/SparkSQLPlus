package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query8SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query8SparkSQLPlus")
		val sc = new SparkContext(conf)
		sc.defaultParallelism
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})


		val v2 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v3 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v4 = v2.semiJoin(v3)
		val v5 = v4.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v6 = v5.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))
		val v7 = v3.semiJoin(v2)
		val v8 = v7.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v9 = v8.semiJoin(v6)
		val v10 = v9.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))

		val v11 = v6.groupBy()
		val v12 = v10.enumerate(v11, Array(0,1), Array(0))
		val v13 = v12.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v14 = v2.groupBy()
		val v15 = v13.enumerate(v14, Array(0,1,2), Array(1))
		val v16 = v15.reKeyBy(x => (x(2).asInstanceOf[Int]))
		val v17 = v3.groupBy()
		val v18 = v16.enumerate(v17, Array(0,1,2,3), Array(0))
		val v19 = v18.map(t => Array(t._2(4), t._2(2), t._2(0), t._2(1), t._2(3)))

		val ts1 = System.currentTimeMillis()
		v19.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query8-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
