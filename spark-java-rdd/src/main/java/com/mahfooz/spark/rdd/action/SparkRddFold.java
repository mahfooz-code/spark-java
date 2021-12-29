package com.mahfooz.spark.rdd.action;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddFold {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddTop")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 4, 3));
            Integer foldInt = intRDD.fold(0, (a, b) -> a + b);
            System.out.println("The sum of all the elements of RDD using fold is " + foldInt);
        }
    }
}
