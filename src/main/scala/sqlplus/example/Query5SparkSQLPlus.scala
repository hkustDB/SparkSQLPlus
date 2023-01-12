package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query5SparkSQLPlus {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query5SparkSQLPlus")
		val sc = new SparkContext(conf)
		sc.defaultParallelism
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v3 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v4 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v5 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))

		val v6 = v2.keyBy(x => (x(0).asInstanceOf[Int]))
		val v7 = v6.groupBy()
		val v8 = v7.sortValuesWith(1, (x: Int, y: Int) => intLessThan(x, y))
		val v9 = v8.extractFieldInHeadElement(1)
		val v10 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v11 = v10.appendExtraColumn(v9)
		val v12 = v11.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v13 = v12.groupBy()
		val v14 = v13.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v15 = v14.extractFieldInHeadElement(2)
		val v16 = v5.keyBy(x => (x(0).asInstanceOf[Int]))
		val v17 = v16.groupBy()
		val v18 = v17.sortValuesWith(1, (x: Int, y: Int) => intLessThan(x, y))
		val v19 = v18.extractFieldInHeadElement(1)
		val v20 = v10.appendExtraColumn(v19)
		val v21 = v20.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v22 = v21.groupBy()
		val v23 = v22.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v24 = v23.extractFieldInHeadElement(2)
		val v25 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v26 = v25.groupBy()
		val v27 = v26.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v28 = v27.extractFieldInHeadElement(1)
		val v29 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v30 = v29.appendExtraColumn(v28)
		val v31 = v30.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v32 = v31.groupBy()
		val v33 = v32.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v34 = v33.extractFieldInHeadElement(2)
		val v35 = v4.keyBy(x => (x(0).asInstanceOf[Int]))
		val v36 = v35.groupBy()
		val v37 = v36.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v38 = v37.extractFieldInHeadElement(1)
		val v39 = v29.appendExtraColumn(v38)
		val v40 = v39.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v41 = v40.groupBy()
		val v42 = v41.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v43 = v42.extractFieldInHeadElement(2)
		val v44 = v10.appendExtraColumn(v15)
		val v45 = v44.appendExtraColumn(v24)
		val v46 = v45.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v47 = v46.appendExtraColumn(v34)
		val v48 = v47.appendExtraColumn(v43)
		val v49 = v48.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v50 = v49.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(5).asInstanceOf[Int]))
		val v51 = v50.filter(x => intLessThan(x._2(3).asInstanceOf[Int], x._2(4).asInstanceOf[Int]))

		val v52 = v51.map(t => Array(t._2(0), t._2(1)))

		val ts1 = System.currentTimeMillis()
		v52.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query5-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
