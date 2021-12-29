package com.mahfooz.spark.rdd.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class SparkRddBroadcast {
    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddBroadcast")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            Broadcast<String> broadcastVar = javaSparkContext.broadcast("Hello Spark");

            System.out.println(broadcastVar.getValue());

            broadcastVar.unpersist();

            broadcastVar.destroy(true);
        }

    }
}
