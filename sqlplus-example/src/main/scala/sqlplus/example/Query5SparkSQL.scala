package sqlplus.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object Query5SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query5SparkSQL")
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
            "SELECT DISTINCT g2.src, g2.dst " +
                "FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4, Graph AS g5, " +
                "(SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1, " +
                "(SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2, " +
                "(SELECT dst, COUNT(*) AS cnt FROM Graph GROUP BY dst) AS c3, " +
                "(SELECT dst, COUNT(*) AS cnt FROM Graph GROUP BY dst) AS c4 " +
                "WHERE g1.dst = g2.src AND g2.dst = g3.src AND g1.src = c1.src " +
                "AND g3.dst = c2.src AND c1.cnt < c2.cnt " +
                "AND g4.dst = g2.src AND g2.dst = g5.src AND g4.src = c3.dst " +
                "AND g5.dst = c4.dst AND c3.cnt < c4.cnt"
        )

        val ts1 = System.currentTimeMillis()
        val cnt = result.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("Query5-SparkSQL cnt: " + cnt)
        LOGGER.info("Query5-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}