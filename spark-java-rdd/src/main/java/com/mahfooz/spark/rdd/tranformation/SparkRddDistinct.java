package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddDistinct {

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddDistinct")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Integer> rddDuplicateElements = javaSparkContext
                    .parallelize(Arrays.asList(1, 1, 2, 4, 5, 6, 8, 8, 9, 10, 11, 11));
            JavaRDD<Integer> rddUniqueElements = rddDuplicateElements.distinct();
            rddUniqueElements.foreach(record -> System.out.println(record));
        }
    }

}
