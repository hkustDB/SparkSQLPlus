package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query4SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query4SparkSQLPlus")
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

		val v4 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v5 = v4.groupBy()
		val v6 = v5.sortValuesWith(1, (x: Int, y: Int) => intLessThan(x, y))
		val v7 = v6.extractFieldInHeadElement(1)
		val v8 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v9 = v8.appendExtraColumn(v7)
		val v10 = v9.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v11 = v10.groupBy()
		val v12 = v11.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v13 = v12.extractFieldInHeadElement(2)
		val v14 = v2.keyBy(x => (x(0).asInstanceOf[Int]))
		val v15 = v14.groupBy()
		val v16 = v15.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v17 = v16.extractFieldInHeadElement(1)
		val v18 = v8.appendExtraColumn(v13)
		val v19 = v18.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v20 = v19.groupBy()
		val v21 = v20.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v22 = v21.extractFieldInHeadElement(2)
		val v23 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v24 = v23.appendExtraColumn(v17)
		val v25 = v24.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v26 = v25.appendExtraColumn(v22)
		val v27 = v26.filter(x => intLessThan(x._2(3).asInstanceOf[Int], x._2(2).asInstanceOf[Int]))

		val v28 = v27.map(t => Array(t._2(0), t._2(1)))

		val ts1 = System.currentTimeMillis()
		v28.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query4-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
