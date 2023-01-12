package sqlplus.example

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query3SparkSQL {
    val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query3SparkSQL")
        val sc = new SparkContext(conf)

        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        val lines = sc.textFile(s"${args.head}/graph.dat")
        val graph = lines.map(line => {
            val temp = line.split(",")
            (temp(0).toInt, temp(1).toInt)
        })
        graph.cache()

        val frequency = graph.map(edge => (edge._1, 1)).reduceByKey((a, b) => a + b).cache()
        frequency.count()
        val frequency0 = graph.map(edge => (edge._2, 1)).reduceByKey((a, b) => a + b).cache()
        frequency0.count()


        val graphSchemaString = "src dst"
        val graphFields = graphSchemaString.split(" ")
            .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
        val graphSchema = StructType(graphFields)

        val countSchemaString = "src cnt"
        val countFields = countSchemaString.split(" ")
            .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
        val countSchema = StructType(countFields)

        val countinvSchemaString = "dst cnt"
        val countinvFields = countinvSchemaString.split(" ")
            .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
        val countinvSchema = StructType(countinvFields)

        val graphRow = graph.map(attributes => Row(attributes._1, attributes._2))
        val countRow = frequency.map(attributes => Row(attributes._1, attributes._2))
        val countinvRow = frequency0.map(attributes => Row(attributes._1, attributes._2))

        val graphDF = spark.createDataFrame(graphRow, graphSchema)
        val countDF = spark.createDataFrame(countRow, countSchema)
        val countIDF = spark.createDataFrame(countinvRow, countinvSchema)

        graphDF.createOrReplaceTempView("Graph")
        countDF.createOrReplaceTempView("countDF")
        countIDF.createOrReplaceTempView("countIDF")

        graphDF.persist()
        countDF.persist()
        countIDF.persist()


        val resultDF = spark.sql(
            "SELECT g1.src, g1.dst, g2.dst, g3.dst, c1.cnt, c2.cnt From Graph g1, Graph g2, Graph g3, " +
                "countDF c1, countDF c2, countDF c3, countIDF c4 " +
                "where g1.dst = g2.src and g2.dst = g3.src and c1.src = g1.src and c2.src = g3.dst and c3.src = g2.src " +
                s"and c4.dst = g3.dst and c1.cnt < c2.cnt and c3.cnt < c4.cnt")

        val ts1 = System.currentTimeMillis()
        resultDF.count()
        val ts2 = System.currentTimeMillis()
        logger.info("Query3-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}