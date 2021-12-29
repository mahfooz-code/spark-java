package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkRddFlatMapValues {

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCartesian")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaPairRDD<String, String> monExpRDD = javaSparkContext
                    .parallelizePairs(Arrays.asList(new Tuple2<String, String>("Jan", "50,100,214,10"),
                            new Tuple2<String, String>("Feb", "60,314,223,77")));

            JavaPairRDD<String, Integer> monExpflattened = monExpRDD.flatMapValues(v -> Arrays.asList(v.split(","))
                    .stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList()));

            System.out.println(monExpflattened.collect());
        }
    }

}
