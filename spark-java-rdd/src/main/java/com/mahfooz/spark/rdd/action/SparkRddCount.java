package com.mahfooz.spark.rdd.action;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddCount {

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCheck")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(new Integer[] { 1, 2, 4, 5, 6 }));

            // count()
            long countVal = intRDD.count();
            System.out.println("The number of elements in the the RDD are :" + countVal);
        }

    }

}
