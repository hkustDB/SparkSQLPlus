package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query7SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query7SparkSQLPlus")
		val sc = new SparkContext(conf)
		sc.defaultParallelism
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))

		val v4 = v2.keyBy(x => (x(0).asInstanceOf[Int]))
		val v5 = v4.groupBy()
		val v6 = v5.sortValuesWith(1, (x: Int, y: Int) => intLessThan(x, y))
		val v7 = v6.extractFieldInHeadElement(1)
		val v8 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v9 = v8.appendExtraColumn(v7)
		val v10 = v9.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v11 = v10.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(0).asInstanceOf[Int]))
		val v12 = v8.semiJoin(v11)
		val v13 = v12.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v14 = v8.semiJoin(v13)
		val v15 = v14.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v16 = v15.groupBy()
		val v17 = v16.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v18 = v17.extractFieldInHeadElement(1)
		val v19 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v20 = v19.appendExtraColumn(v18)
		val v21 = v20.filter(x => intLessThan(x._2(1).asInstanceOf[Int], x._2(2).asInstanceOf[Int]))

		val v22 = v21.enumerate(v17, 1, 1, (x: Int, y: Int) => intLessThan(x, y), Array(0,1), Array(1))
		val v23 = v22.reKeyBy(x => (x(2).asInstanceOf[Int]))
		val v24 = v13.groupBy()
		val v25 = v23.enumerate(v24, Array(0,1,2), Array(1))
		val v26 = v25.reKeyBy(x => (x(3).asInstanceOf[Int]))
		val v27 = v11.groupBy()
		val v28 = v26.enumerate(v27, Array(0,1,2,3), Array(1,2))
		val v29 = v28.reKeyBy(x => (x(4).asInstanceOf[Int]))
		val v30 = v29.enumerate(v6, 3, 1, (x: Int, y: Int) => intLessThan(y, x), Array(0,1,2,3,4), Array(1))
		val v31 = v30.map(t => Array(t._2(1), t._2(0), t._2(2), t._2(3), t._2(4), t._2(5)))

		val ts1 = System.currentTimeMillis()
		v31.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query7-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
