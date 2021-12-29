package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkRddMapToPair {
    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "SparkRddMapToPair App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            // Input RDD
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);
            intRDD.foreach(record -> System.out.println(record));

            // Filter RDD
            JavaPairRDD<String, Integer> mapToPairRDD = intRDD.mapToPair(
                    i -> (i % 2 == 0) ? new Tuple2<String, Integer>("even", i) : new Tuple2<String, Integer>("odd", i));

            mapToPairRDD.foreach(record -> System.out.println(record));
        }
    }
}