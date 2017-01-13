package com.apress.phoenix.chapter8

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
  * spark job using dataframe
  */
object PhoenixSparkDfApp {

  def main(args: Array[String]) {
    import org.apache.phoenix.spark._
    import org.apache.spark.SparkContext

    val zkQuorum = "localhost:2181"
    val master = "local[*]"
    val sparkConf = new SparkConf()
    sparkConf.setAppName("phoenix-spark-save")
        .setMaster(s"${master}")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // read from orders phoenix table
    val df = sqlContext.phoenixTableAsDataFrame("ORDERS",
        Seq.apply("ORDER_ID", "CUST_ID", "QUANTITY"),
        zkUrl = Some.apply(s"${zkQuorum}") )

    val result = df.rdd.map(row => (row.getLong(1), row.getLong(2))).reduceByKey(_ + _)
    // save to customer_stats phoenix table.
    result.saveToPhoenix("CUSTOMER_STATS",
        Seq("CUST_ID", "QUANTITY"),
        zkUrl = Some.apply(s"${zkQuorum}"))
  }
}
