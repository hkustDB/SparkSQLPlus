package sqlplus.example

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Query9SparkSQL {
    val LOGGER = LoggerFactory.getLogger("SparkSQLPlusExperiment")

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Query9SparkSQL")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        val schema = "T_ID BIGINT, T_DTS TIMESTAMP, T_TT_ID String, T_S_SYMB String, T_CA_ID BIGINT, T_TRADE_PRICE DOUBLE"
        val df = spark.read.format("csv")
            .option("delimiter", "|").option("quote", "")
            .option("header", "false")
            .schema(schema)
            .load(s"${args.head}/trade.dat")
        df.persist()
        df.count()
        df.createOrReplaceTempView("Trade")

        val resultDF = spark.sql(
            "SELECT T1.T_ID, T1.T_DTS, T1.T_TT_ID, T1.T_TRADE_PRICE, T2.T_ID, T2.T_DTS, T2.T_TT_ID, T2.T_TRADE_PRICE, \n" +
                "T1.T_S_SYMB, T1.T_CA_ID \n" +
                "FROM Trade T1, Trade T2 \n" +
                "WHERE T1.T_TT_ID LIKE '%B%' AND T2.T_TT_ID LIKE '%S%' \n" +
                "and T1.T_CA_ID = T2.T_CA_ID and T1.T_S_SYMB = T2.T_S_SYMB and T1.T_DTS <= T2.T_DTS \n" +
                "and T1.T_DTS + interval '90' day >= T2.T_DTS and T1.T_TRADE_PRICE * 1.2 < T2.T_TRADE_PRICE")

        val ts1 = System.currentTimeMillis()
        val resultCnt = resultDF.count()
        val ts2 = System.currentTimeMillis()
        LOGGER.info("Query9-SparkSQL cnt: " + resultCnt)
        LOGGER.info("Query9-SparkSQL time: " + (ts2 - ts1) / 1000f)
        spark.close()
    }
}