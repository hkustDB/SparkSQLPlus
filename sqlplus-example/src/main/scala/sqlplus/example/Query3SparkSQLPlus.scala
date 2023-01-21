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
		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v2.count()
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v3.count()
		val v4 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v4.count()
		val v5 = v1.map(fields => ((fields(1)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2)).persist()
		v5.count()

		val v6 = v2.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v7 = v1.keyBy(x => (x(0).asInstanceOf[Int]))
		val v8 = v7.appendExtraColumn(v6)
		val v9 = v8.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v10 = v9.groupBy()
		val v11 = v10.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y)).persist()
		val v12 = v11.extractFieldInHeadElement(2)
		val v13 = v4.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v14 = v3.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v15 = v5.map(x => (x(0).asInstanceOf[Int], x(1)))
		val v16 = v1.keyBy(x => (x(1).asInstanceOf[Int]))
		val v17 = v16.appendExtraColumn(v14)
		val v18 = v17.appendExtraColumn(v15)
		val v19 = v18.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v20 = v19.groupBy()
		val v21 = v20.constructTreeLikeArray(2, 3, (x: Int, y: Int) => intLessThan(y, x), (x: Int, y: Int) => intLessThan(y, x))
		val v22 = v21.createDictionary()
		val v23 = v7.appendExtraColumn(v12)
		val v24 = v23.appendExtraColumn(v13)
		val v25 = v24.reKeyBy(x => (x(1).asInstanceOf[Int]))
		val v26 = v25.appendExtraColumn(v22, 2, (x: Int, y: Int) => intLessThan(y, x))
		val v27 = v26.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v28 = v27.filter(x => intLessThan(x._2(3).asInstanceOf[Int], x._2(4).asInstanceOf[Int]))

		val v29 = v28.map(t => ((t._2(1).asInstanceOf[Int]), Array(t._2(0), t._2(2), t._2(3))))
		val v30 = v29.enumerateWithTwoComparisons(v21, 1, 2, Array(2), Array(0, 1, 2, 3), (l, r) => (l(0).asInstanceOf[Int]))
		val v31 = v30.enumerateWithOneComparison(v11, 3, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0, 1, 2, 3, 4), Array(0, 1, 2))

		val ts1 = System.currentTimeMillis()
		val cnt = v31.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query3-SparkSQLPlus cnt: " + cnt)
		LOGGER.info("Query3-SparkSQLPlus time: " + (ts2 - ts1) / 1000f)

		sparkSession.close()
	}
}
