package sqlplus.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query5SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query5SparkSQLPlus")
		val sc = new SparkContext(conf)
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile(s"${args.head}/graph.dat").map(line => {
			val fields = line.split(",")
			Array[Any](fields(0).toInt, fields(1).toInt)
		}).persist()
		v1.count()
		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()
		val v4 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v4.count()
		val v5 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v5.count()

		val v6 = v5.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v7 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v8 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v9 = v8.appendExtraColumn(v7)
		val v10 = v9.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v11 = v10.groupBy()
		val v12 = v11.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v13 = v12.extractFieldInHeadElement(2)
		val v14 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v15 = v14.appendExtraColumn(v6)
		val v16 = v15.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v17 = v16.groupBy()
		val v18 = v17.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v19 = v18.extractFieldInHeadElement(2)
		val v20 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v21 = v14.appendExtraColumn(v20)
		val v22 = v21.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v23 = v22.groupBy()
		val v24 = v23.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x)).persist()
		val v25 = v24.extractFieldInHeadElement(2)
		val v26 = v4.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v27 = v8.appendExtraColumn(v26)
		val v28 = v27.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v29 = v28.groupBy()
		val v30 = v29.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v31 = v30.extractFieldInHeadElement(2)
		val v32 = v8.appendExtraColumn(v13)
		val v33 = v32.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v34 = v33.appendExtraColumn(v19)
		val v35 = v34.appendExtraColumn(v25)
		val v36 = v35.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v37 = v36.appendExtraColumn(v31)
		val v38 = v37.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(4).asInstanceOf[Int]))
		val v39 = v38.filter(x => intLessThan(x._2(5).asInstanceOf[Int], x._2(3).asInstanceOf[Int]))

		val ts1 = System.currentTimeMillis()
		val cnt = v39.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query5-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query5-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
