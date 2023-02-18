package sqlplus.example

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query2SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query2SparkSQL")
        val sc = new SparkContext(conf)

        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        val lines = sc.textFile(s"${args.head}/graph.dat")
        val graph = lines.map(line => {
            val temp = line.split(",")
            (temp(0).toInt, temp(1).toInt)
        })
        graph.cache()

        val graphSchemaString = "src dst"
        val graphFields = graphSchemaString.split(" ")
            .map(fieldName => StructField(fieldName, IntegerType, nullable = false))
        val graphSchema = StructType(graphFields)

        val graphRow = graph.map(attributes => Row(attributes._1, attributes._2))

        val graphDF = spark.createDataFrame(graphRow, graphSchema)

        graphDF.createOrReplaceTempView("Graph")

        graphDF.persist()

        val resultDF = spark.sql(
            "SELECT * " +
                "From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7 " +
                "where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst " +
                "and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst " +
                s"and g1.dst = g7.src and g4.src = g7.dst and g1.src+g2.src+g3.src < g4.src+g5.src+g6.src")

        val ts1 = System.currentTimeMillis()
        val resultCnt = resultDF.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("Query2-SparkSQL cnt: " + resultCnt)
        LOGGER.info("Query2-SparkSQL time: " + (ts2 - ts1) / 1000f)
        spark.close()
    }
}