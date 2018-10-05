package com.ma.analytics;

import org.apache.spark.sql.SparkSession;

public class MyJavaApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().
          master("local[2]").appName("java-code-challenge").getOrCreate();

        /// Insert your code here
        spark.stop();
    }
}
