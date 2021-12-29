package com.mahfooz.spark.rdd.action;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddTop {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddTop")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            // top()
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 4, 3));
            List<Integer> topTwo = intRDD.top(2);
            for (Integer intVal : topTwo) {
                System.out.println("The top two values of the RDD are : " + intVal);
            }

            // top()
            intRDD = javaSparkContext.parallelize(Arrays.asList(1, 4, 3));
            topTwo = intRDD.top(2, Comparator.reverseOrder());
            for (Integer intVal : topTwo) {
                System.out.println("The top two values of the RDD are : " + intVal);
            }
        }

    }
}
