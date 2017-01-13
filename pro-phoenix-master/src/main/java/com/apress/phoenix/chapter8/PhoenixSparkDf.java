package com.apress.phoenix.chapter8;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 *
 */
public class PhoenixSparkDf implements Serializable {

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark-phoenix-df");
        sparkConf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        DataFrame df = sqlContext.read()
                .format("org.apache.phoenix.spark")
                .option("table", "ORDERS")
                .option("zkUrl", "localhost:2181")
                .load();
        df.count();

    }
}
