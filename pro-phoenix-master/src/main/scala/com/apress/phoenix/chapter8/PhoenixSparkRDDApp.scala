package com.apress.phoenix.chapter8

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * spark job
  */
object PhoenixSparkRDDApp {

  def main(args: Array[String])  {

    import org.apache.phoenix.spark._
    import org.apache.spark.SparkContext
    val zkQuorum = "localhost:2181"
    val master = "local[*]"
    val sparkConf = new SparkConf ()
    sparkConf.setAppName ("phoenix-spark-save")
    .setMaster (s"${master}")

    val sc = new SparkContext (sparkConf)
    // read from orders phoenix table
    val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD ("ORDERS",
    Seq.apply ("ORDER_ID", "CUST_ID", "AMOUNT"),
    zkUrl = Some.apply (s"${zkQuorum}") )

    val result = rdd.map(row => (row("CUST_ID").asInstanceOf[Long],
            (row("AMOUNT").asInstanceOf[java.math.BigDecimal]).doubleValue()))
      .reduceByKey(_ + _)

    // save to customer_stats phoenix table.
    result.saveToPhoenix( "CUSTOMER_STATS", Seq("CUST_ID","AMOUNT"),
          zkUrl = Some.apply(s"${zkQuorum}"))
  }
}
