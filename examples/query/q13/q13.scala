package cqc.example

import sqlplus.helper.ImplicitConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkCQCExample {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("SparkCQCExample")
		conf.setMaster("local")
		val sc = new SparkContext(conf)
		sc.defaultParallelism
		val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

		val intLessThan = (x: Int, y: Int) => x < y

		val v1 = sc.textFile("examples/data/r3.dat").map(line => {
			val fields = line.split("\\s+")
			Array[Any](fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt)
		})

		val v2 = sc.textFile("examples/data/r2.dat").map(line => {
			val fields = line.split("\\s+")
			Array[Any](fields(0).toInt, fields(1).toInt, fields(2).toInt)
		})

		val v3 = sc.textFile("examples/data/r4.dat").map(line => {
			val fields = line.split("\\s+")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v4 = sc.textFile("examples/data/r5.dat").map(line => {
			val fields = line.split("\\s+")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v5 = sc.textFile("examples/data/r1.dat").map(line => {
			val fields = line.split("\\s+")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v6 = v1.map(x => Array(x(0), x(1), x(2)))

		val v7 = v4.keyBy(x => (x(0).asInstanceOf[Int]))
		val v8 = v7.groupBy()
		val v9 = v8.sortValuesWith(1, (x: Int, y: Int) => intLessThan(y, x))
		val v10 = v9.extractFieldInHeadElement(1)
		val v11 = v6.keyBy(x => (x(0).asInstanceOf[Int]))
		val v12 = v5.keyBy(x => (x(1).asInstanceOf[Int]))
		val v13 = v11.semiJoin(v12)
		val v14 = v13.reKeyBy(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).asInstanceOf[Int]))
		val v15 = v1.keyBy(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).asInstanceOf[Int]))
		val v16 = v14.semiJoin(v15)
		val v17 = v2.keyBy(x => (x(1).asInstanceOf[Int]))
		val v18 = v17.appendExtraColumn(v10)
		val v19 = v3.keyBy(x => (x(0).asInstanceOf[Int]))
		val v20 = v18.semiJoin(v19)
		val v21 = v20.reKeyBy(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]))
		val v22 = v16.reKeyBy(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]))
		val v23 = v21.semiJoin(v22)
		val v24 = v23.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v25 = v24.filter(x => intLessThan(x._2(0).asInstanceOf[Int], x._2(3).asInstanceOf[Int]))

		val v26 = v25.reKeyBy(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]))
		val v27 = v22.groupBy()
		val v28 = v26.enumerate(v27, Array(0,1,2,3), Array(2))
		val v29 = v28.reKeyBy(x => (x(0).asInstanceOf[Int]))
		val v30 = v12.groupBy()
		val v31 = v29.enumerate(v30, Array(0,1,2,3,4), Array(0))
		val v32 = v31.map(t => Array(t._2(5), t._2(0), t._2(1), t._2(4), t._2(2)))

		sparkSession.close()
	}
}
