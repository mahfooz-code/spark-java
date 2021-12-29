package com.mahfooz.spark.rdd.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddTakeSample {
    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddTakeSample")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            // takeSample()
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 4, 3));
            List<Integer> takeSamepTrueSeededList = intRDD.takeSample(true, 4, 9);
            for (Integer intVal : takeSamepTrueSeededList) {
                System.out.println("The take sample vals with seed are : " + intVal);
            }
        }
    }

}
