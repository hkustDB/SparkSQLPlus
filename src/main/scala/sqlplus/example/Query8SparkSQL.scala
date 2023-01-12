package sqlplus.example

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query8SparkSQL {
	val logger = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query8SparkSQL")
		val sc = new SparkContext(conf)

		val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

		// Modify to the correct input file path
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
			"SELECT g1.src, g1.dst, g2.dst, g3.dst, g4.dst " +
				"From Graph g1, Graph g2, Graph g3, Graph g4 " +
				"where g1.dst = g2.src and g2.dst = g3.src and g3.dst = g4.src " +
				"and g2.src < g2.dst and g3.src < g3.dst")

		val ts1 = System.currentTimeMillis()
		resultDF.count()
		val ts2 = System.currentTimeMillis()
		logger.info("Query8-SparkSQL time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
