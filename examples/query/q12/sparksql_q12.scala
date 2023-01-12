package cqc.example

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLExampleQ6 extends App {
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("SparkSQLExampleQ6")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

  val lines = sc.textFile("examples/data/path.dat")
  val graph = lines.map(line => {
    val temp = line.split("\\s+")
    (temp(0).toInt, temp(1).toInt)
  })
  graph.cache()
  graph.count()
  val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a+b).cache()
  frequency.count()


  val graphSchemaString = "src dst"
  val graphFields = graphSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
  val graphSchema = StructType(graphFields)

  val countSchemaString = "src cnt"
  val countFields = countSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
  val countSchema = StructType(countFields)

  val graphRow = graph.map(attributes => Row(attributes._1, attributes._2))
  val countRow = frequency.map(attributes => Row(attributes._1, attributes._2))

  val graphDF = spark.createDataFrame(graphRow, graphSchema)
  val countDF = spark.createDataFrame(countRow, countSchema)

  graphDF.createOrReplaceTempView("Graph")
  countDF.createOrReplaceTempView("countDF")

  graphDF.persist()
  countDF.persist()
  graphDF.count()


  val resultDF = spark.sql(
    "SELECT g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt From Graph g1, Graph g2, Graph g3, countDF c1, countDF c2 " +
        "where g1.dst = g2.src and g2.dst = g3.src and c1.src = g1.src and c2.src = g3.dst " +
        "and c1.cnt + g1.src < c2.cnt + g3.dst")

  println(resultDF.count())

  spark.close()
}
