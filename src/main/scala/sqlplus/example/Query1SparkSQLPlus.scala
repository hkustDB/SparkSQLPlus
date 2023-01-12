package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query1SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query1SparkSQLPlus")
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
		val v6 = v5.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v7 = v6.extractFieldInHeadElement(1)
		val v8 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v9 = v8.appendExtraColumn(v7)
		val v10 = v9.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v11 = v10.groupBy()
		val v12 = v11.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v13 = v12.extractFieldInHeadElement(2)
		val v14 = v8.appendExtraColumn(v13)
		val v15 = v14.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v16 = v15.groupBy()
		val v17 = v16.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v18 = v17.extractFieldInHeadElement(2)
		val v19 = v8.appendExtraColumn(v18)
		val v20 = v19.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v21 = v20.groupBy()
		val v22 = v21.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v23 = v22.extractFieldInHeadElement(2)
		val v24 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v25 = v24.appendExtraColumn(v23)
		val v26 = v25.filter(x => intLessThan(x._2(1).asInstanceOf[Int], x._2(2).asInstanceOf[Int]))

		val v27 = v26.enumerate(v22, 1, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1), Array(1,2))
		val v28 = v27.reKeyBy(x => (x(2).asInstanceOf[Int]))
		val v29 = v28.enumerate(v17, 1, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2), Array(1,2))
		val v30 = v29.reKeyBy(x => (x(3).asInstanceOf[Int]))
		val v31 = v30.enumerate(v12, 1, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2,3), Array(1,2))
		val v32 = v31.reKeyBy(x => (x(4).asInstanceOf[Int]))
		val v33 = v32.enumerate(v6, 1, 1, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2,3,4), Array(1))
		val v34 = v33.map(t => Array(t._2(1), t._2(0), t._2(2), t._2(3), t._2(4), t._2(5)))

		val ts1 = System.currentTimeMillis()
		v34.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query1-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
