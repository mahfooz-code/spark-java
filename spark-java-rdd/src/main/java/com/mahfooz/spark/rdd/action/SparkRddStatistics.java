package com.mahfooz.spark.rdd.action;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.sql.SparkSession;

public class SparkRddStatistics {

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddStatistics")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            // countApprox(long timeout)
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 4, 3, 5, 7));
            PartialResult<BoundedDouble> countAprx = intRDD.countApprox(2);

            System.out.println("Confidence::" + countAprx.getFinalValue().confidence());
            System.out.println("high::" + countAprx.getFinalValue().high());
            System.out.println("Low::" + countAprx.getFinalValue().low());
            System.out.println("Mean::" + countAprx.getFinalValue().mean());
            System.out.println("Final::" + countAprx.getFinalValue().toString());
        }
    }

}
