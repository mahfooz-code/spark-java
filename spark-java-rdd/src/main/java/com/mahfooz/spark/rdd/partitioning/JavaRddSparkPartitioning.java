package com.mahfooz.spark.rdd.partitioning;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaRddSparkPartitioning {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("JavaRddSparkPartitioning")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 8);
            System.out.println("Before repartition:" + intRDD.getNumPartitions());

            // Repartitioning
            JavaRDD<Integer> textFileRepartitioned = intRDD.repartition(4);
            System.out.println("After repartition:" + textFileRepartitioned.getNumPartitions());

        }
    }
}
