package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query3SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query3SparkSQLPlus")
		val sc = new SparkContext(conf)
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		}).persist()
		v1.count()
		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v4 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v5 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))

		val v6 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v7 = v4.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v8 = v5.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v9 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v10 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v11 = v10.appendExtraColumn(v8)
		val v12 = v11.appendExtraColumn(v9)
		val v13 = v12.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v14 = v13.groupBy()
		val v15 = v14.constructTreeLikeArray(2, 3, (x: Int, y: Int) => intLessThan(y, x), (x: Int, y: Int) => intLessThan(y, x))
		val v16 = v15.createDictionary()
		val v17 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v18 = v17.appendExtraColumn(v7)
		val v19 = v18.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v20 = v19.appendExtraColumn(v16, 2, (x: Int, y: Int) => intLessThan(y, x))
		val v21 = v20.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v22 = v21.groupBy()
		val v23 = v22.sortValuesWith(3, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v24 = v23.extractFieldInHeadElement(3)
		val v25 = v17.appendExtraColumn(v6)
		val v26 = v25.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v27 = v26.appendExtraColumn(v24)
		val v28 = v27.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v29 = v28.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(3).asInstanceOf[Int]))

		val v30 = v29.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v31 = v30.enumerate(v23, 2, 3, (x: Int, y: Int) => intLessThan(x, y), Array(0, 1, 2), Array(1, 2, 3))
		val v32 = v31.reKeyBy(x => (x(3).asInstanceOf[Int]))
		val v33 = v32.enumerate(v15, 4, 2, Array(0, 1, 2, 3, 4), Array(1, 2, 3))
		val v34 = v33.map(t => Array(t._2(0), t._2(1), t._2(3), t._2(5), t._2(2), t._2(7), t._2(4), t._2(6)))

		val ts1 = System.currentTimeMillis()
		val cnt = v34.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query3-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
