package sqlplus.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object Query9SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query9SparkSQL")
        val spark = SparkSession.builder.config(conf).getOrCreate()

        val schema0 = "T_ID BIGINT, T_DTS TIMESTAMP, T_TT_ID STRING, T_S_SYMB STRING, T_CA_ID BIGINT, T_TRADE_PRICE DOUBLE"
        val df0 = spark.read.format("csv")
            .option("delimiter", ",")
            .option("quote", "")
            .option("header", "false")
            .schema(schema0)
            .load(s"${args.head}/trade.dat").persist()
        df0.count()
        df0.createOrReplaceTempView("Trade")

        val result = spark.sql(
            "SELECT t1.T_ID, t1.T_DTS, t1.T_TT_ID, t1.T_TRADE_PRICE, " +
                "t2.T_ID, t2.T_DTS, t2.T_TT_ID, t2.T_TRADE_PRICE, " +
                "t1.T_S_SYMB, t1.T_CA_ID " +
                "FROM Trade t1, Trade t2 " +
                "WHERE t1.T_TT_ID LIKE '%B%' AND t2.T_TT_ID LIKE '%S%' " +
                "AND t1.T_CA_ID = t2.T_CA_ID AND t1.T_S_SYMB = t2.T_S_SYMB " +
                "AND t1.T_DTS <= t2.T_DTS AND t1.T_DTS + interval '90' day >= t2.T_DTS " +
                "AND t1.T_TRADE_PRICE * 1.2 < t2.T_TRADE_PRICE"
        )

        val ts1 = System.currentTimeMillis()
        val cnt = result.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("Query9-SparkSQL cnt: " + cnt)
        LOGGER.info("Query9-SparkSQL time: " + (ts2 - ts1) / 1000f)

        spark.close()
    }
}