package sqlplus.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sqlplus.helper.ImplicitConversions._

object Query5SparkSQLPlus {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query5SparkSQLPlus")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val v1 = spark.sparkContext.textFile(s"${args.head}/graph.dat").map(row => {
			val f = row.split(",")
			Array[Any](f(0).toInt, f(1).toInt)
		}).persist()
		v1.count()
		val v2 = v1.map(f => (f(0), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v1.map(f => (f(1), 1L)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()
		val longLessThan = (x: Long, y: Long) => x < y

		val v4 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v5 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v6 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v7 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v8 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v9 = v8.appendExtraColumn(v7)
		val v10 = v9.reKeyBy(x => x(0).asInstanceOf[Int])
		val v11 = v10.groupBy()
		val v12 = v11.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(y, x)).persist()
		val v13 = v12.extractFieldInHeadElement(2)
		val v14 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v15 = v14.appendExtraColumn(v4)
		val v16 = v15.reKeyBy(x => x(1).asInstanceOf[Int])
		val v17 = v16.groupBy()
		val v18 = v17.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v19 = v18.extractFieldInHeadElement(2)
		val v20 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v21 = v20.appendExtraColumn(v5)
		val v22 = v21.reKeyBy(x => x(1).asInstanceOf[Int])
		val v23 = v22.groupBy()
		val v24 = v23.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(x, y)).persist()
		val v25 = v24.extractFieldInHeadElement(2)
		val v26 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v27 = v26.appendExtraColumn(v6)
		val v28 = v27.reKeyBy(x => x(0).asInstanceOf[Int])
		val v29 = v28.groupBy()
		val v30 = v29.sortValuesWith[Long, Long, Long, Long](2, (x: Long, y: Long) => longLessThan(y, x)).persist()
		val v31 = v30.extractFieldInHeadElement(2)
		val v32 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v33 = v32.appendExtraColumn(v13)
		val v34 = v33.reKeyBy(x => x(0).asInstanceOf[Int])
		val v35 = v34.appendExtraColumn(v19)
		val v36 = v35.appendExtraColumn(v25)
		val v37 = v36.reKeyBy(x => x(1).asInstanceOf[Int])
		val v38 = v37.appendExtraColumn(v31)
		val v39 = v38.reKeyBy(x => x(0).asInstanceOf[Int])
		val v40 = v39.filter(x => longLessThan(x._2(4).asInstanceOf[Long], x._2(5).asInstanceOf[Long]))
		val v41 = v40.filter(x => longLessThan(x._2(3).asInstanceOf[Long], x._2(2).asInstanceOf[Long]))

		val ts1 = System.currentTimeMillis()
		val cnt = v41.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query5-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query5-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
