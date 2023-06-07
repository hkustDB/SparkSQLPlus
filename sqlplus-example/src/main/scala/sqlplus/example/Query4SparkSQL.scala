package sqlplus.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object Query4SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query4SparkSQL")
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
            "SELECT DISTINCT g3.src AS src, g3.dst AS dst " +
                "FROM Graph AS g1, Graph AS g2, Graph AS g3, " +
                "(SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1, " +
                "(SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2 " +
                "WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src " +
                "AND c1.cnt < c2.cnt"
        )

        val ts1 = System.currentTimeMillis()
        val cnt = result.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("Query4-SparkSQL cnt: " + cnt)
        LOGGER.info("Query4-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}