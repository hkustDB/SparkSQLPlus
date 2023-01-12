package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query3SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query3SparkSQLPlus")
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
		val v4 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v5 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))

		val v6 = v5.keyBy(x => (x(0).asInstanceOf[Int]))
		val v7 = v6.groupBy()
		val v8 = v7.sortValuesWith(1, (x: Int, y: Int) => intLessThan(x, y))
		val v9 = v8.extractFieldInHeadElement(1)
		val v10 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v11 = v10.appendExtraColumn(v9)
		val v12 = v11.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v13 = v12.groupBy()
		val v14 = v13.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v15 = v14.extractFieldInHeadElement(2)
		val v16 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v17 = v16.groupBy()
		val v18 = v17.sortValuesWith(1, (x: Int, y: Int) => intLessThan(x, y))
		val v19 = v18.extractFieldInHeadElement(1)
		val v20 = v4.keyBy(x => (x(0).asInstanceOf[Int]))
		val v21 = v20.groupBy()
		val v22 = v21.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v23 = v22.extractFieldInHeadElement(1)
		val v24 = v10.appendExtraColumn(v15)
		val v25 = v24.appendExtraColumn(v19)
		val v26 = v25.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v27 = v26.groupBy()
		val v28 = v27.constructTreeLikeArray(3, 2, (x: Int, y: Int) => intLessThan(x, y), (x: Int, y: Int) => intLessThan(x, y))
		val v29 = v28.createDictionary()
		val v30 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v31 = v30.appendExtraColumn(v23)
		val v32 = v31.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v33 = v32.appendExtraColumn(v29, 2, (x: Int, y: Int) => intLessThan(x, y))
		val v34 = v33.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v35 = v34.groupBy()
		val v36 = v35.sortValuesWith(3, (x: Int, y: Int) => intLessThan(x, y))
		val v37 = v36.extractFieldInHeadElement(3)
		val v38 = v2.keyBy(x => (x(0).asInstanceOf[Int]))
		val v39 = v38.appendExtraColumn(v37)
		val v40 = v39.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(1).asInstanceOf[Int]))

		val v41 = v40.enumerate(v36, 1, 3, (x: Int, y: Int) => intLessThan(y, x), Array(0,1), Array(0,2,3))
		val v42 = v41.reKeyBy(x => (x(2).asInstanceOf[Int]))
		val v43 = v42.enumerate(v28, 3, 1, Array(0,1,2,3), Array(0,2,3))
		val v44 = v43.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v45 = v44.enumerate(v22, 6, 1, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2,4,5,6), Array(1))
		val v46 = v45.reKeyBy(x => (x(3).asInstanceOf[Int]))
		val v47 = v46.enumerate(v18, 6, 1, (x: Int, y: Int) => intLessThan(y, x), Array(0,1,2,3,4,6), Array(1))
		val v48 = v47.enumerate(v14, 1, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0,1,2,3,5,6), Array(0,2))
		val v49 = v48.reKeyBy(x => (x(6).asInstanceOf[Int]))
		val v50 = v49.enumerate(v8, 1, 1, (x: Int, y: Int) => intLessThan(y, x), Array(0,1,2,3,4,5,6), Array(1))
		val v51 = v50.map(t => Array(t._2(6), t._2(3), t._2(2), t._2(0), t._2(7), t._2(1), t._2(5), t._2(4)))

		val ts1 = System.currentTimeMillis()
		v51.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
