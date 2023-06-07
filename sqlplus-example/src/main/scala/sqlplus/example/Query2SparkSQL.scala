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
            "SELECT * " +
                "FROM Graph AS g1, Graph AS g2, Graph AS g3, " +
                "Graph AS g4, Graph AS g5, Graph AS g6, Graph AS g7 " +
                "WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g1.src " +
                "AND g4.dst = g5.src AND g5.dst = g6.src AND g6.dst = g4.src " +
                "AND g1.dst = g7.src AND g7.dst = g4.src " +
                "AND g1.src + g2.src + g3.src < g4.src + g5.src + g6.src"
        )

        val ts1 = System.currentTimeMillis()
        val cnt = result.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("Query2-SparkSQL cnt: " + cnt)
        LOGGER.info("Query2-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}