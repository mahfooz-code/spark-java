package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddCartesian {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCartesian")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaRDD<String> rddStrings = javaSparkContext.parallelize(Arrays.asList("A", "B", "C"));
            JavaRDD<Integer> rddIntegers = javaSparkContext.parallelize(Arrays.asList(1, 4, 5));
            rddStrings.cartesian(rddIntegers).foreach(record -> System.out.println(record));
        }
    }
}
