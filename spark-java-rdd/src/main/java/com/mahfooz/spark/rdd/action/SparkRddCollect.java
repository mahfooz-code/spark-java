/*

Collect retrieves the data from different partitions of RDD into an array at the driver program.

*/
package com.mahfooz.spark.rdd.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddCollect {

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCheck")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));

            // Collect
            List<Integer> collectedList = intRDD.collect();
            for (Integer elements : collectedList) {
                System.out.println("The collected elements are ::" + elements);
            }
        }
    }
}
