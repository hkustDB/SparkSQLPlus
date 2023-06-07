package sqlplus.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object Query8SparkSQL {
	val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName("Query8SparkSQL")
		val spark = SparkSession.builder.config(conf).getOrCreate()

		val schema0 = "src INTEGER, dst INTEGER"
		val df0 = spark.read.format("csv")
			.option("delimiter", ",")
			.option("quote", "")
			.option("header", "false")
			.schema(schema0)
			.load(s"${args.head}/graph.dat").persist()
		df0.count()
		df0.createOrReplaceTempView("Graph")

		val result = spark.sql(
			"SELECT g1.src AS src, g1.dst AS via1, g2.dst AS via2, g3.dst AS via3, g4.dst AS dst " +
				"FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4 " +
				"WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src " +
				"AND g2.src < g2.dst AND g3.src < g3.dst"
		)

		val ts1 = System.currentTimeMillis()
		val cnt = result.count()
		val ts2 = System.currentTimeMillis()
		LOGGER.info("Query8-SparkSQL cnt: " + cnt)
		LOGGER.info("Query8-SparkSQL time: " + (ts2 - ts1) / 1000f)

		spark.close()
	}
}
