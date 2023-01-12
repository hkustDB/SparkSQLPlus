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

		val intLessThan = (i1: Int, i2: Int) => i1 < i2

		val v1 = sc.textFile("examples/data/path.dat").map(line => {
			val fields = line.split("\\s+")
			Array[Any](fields(0).toInt, fields(1).toInt)
		})

		val v2 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))
		val v3 = v1.map(fields => ((fields(0)), 1)).reduceByKey(_ + _).map(x => Array[Any](x._1, x._2))

		val v4 = v2.keyBy(x => x(0).asInstanceOf[Int])
		val v5 = v4.appendExtraColumn(x => (x(1).asInstanceOf[Int] + x(0).asInstanceOf[Int]))
		val v6 = v5.groupBy()
		val v7 = v6.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v8 = v7.extractFieldInHeadElement(2)
		val v9 = v1.keyBy(x => x(0).asInstanceOf[Int])
		val v10 = v9.appendExtraColumn(v8)
		val v11 = v10.reKeyBy(x => x(1).asInstanceOf[Int])
		val v12 = v11.groupBy()
		val v13 = v12.sortValuesWith(2, (x: Int, y: Int) => intLessThan(x, y))
		val v14 = v13.extractFieldInHeadElement(2)
		val v15 = v3.keyBy(x => x(0).asInstanceOf[Int])
		val v16 = v15.appendExtraColumn(x => (x(1).asInstanceOf[Int] + x(0).asInstanceOf[Int]))
		val v17 = v16.groupBy()
		val v18 = v17.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v19 = v18.extractFieldInHeadElement(2)
		val v20 = v1.keyBy(x => x(1).asInstanceOf[Int])
		val v21 = v20.appendExtraColumn(v19)
		val v22 = v21.reKeyBy(x => x(0).asInstanceOf[Int])
		val v23 = v22.groupBy()
		val v24 = v23.sortValuesWith(2, (x: Int, y: Int) => intLessThan(y, x))
		val v25 = v24.extractFieldInHeadElement(2)
		val v26 = v9.appendExtraColumn(v14)
		val v27 = v26.reKeyBy(x => x(1).asInstanceOf[Int])
		val v28 = v27.appendExtraColumn(v25)
		val v29 = v28.reKeyBy(x => x(0).asInstanceOf[Int])
		val v30 = v29.filter(x => intLessThan(x._2(2).asInstanceOf[Int], x._2(3).asInstanceOf[Int]))

		val v31 = v30.reKeyBy(x => x(1).asInstanceOf[Int])
		val v32 = v31.enumerate(v24, 2, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2), Array(1,2))
		val v33 = v32.reKeyBy(x => x(3).asInstanceOf[Int])
		val v34 = v33.enumerate(v18, 2, 2, (x: Int, y: Int) => intLessThan(x, y), Array(0,1,2,3), Array(1,2))
		val v35 = v34.reKeyBy(x => x(0).asInstanceOf[Int])
		val v36 = v35.enumerate(v13, 5, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0,1,3,4,5), Array(0,2))
		val v37 = v36.reKeyBy(x => x(5).asInstanceOf[Int])
		val v38 = v37.enumerate(v7, 4, 2, (x: Int, y: Int) => intLessThan(y, x), Array(0,1,3,4), Array(1,2))

		println(v38.count())
		sparkSession.close()
	}
}
