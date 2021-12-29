package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkRddFlatMapToPair {
    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "SparkRddFlatMapToPair App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            List<String> stringList = Arrays.asList("Hello World", "This is spark", "This is java");

            // Input RDD
            JavaRDD<String> stringRDD = javaSparkContext.parallelize(stringList, 2);
            stringRDD.foreach(record -> System.out.println(record));

            // FlatMap RDD
            JavaPairRDD<String, Integer> flatMapToPairRDD = stringRDD.flatMapToPair(s -> Arrays.asList(s.split(" "))
                    .stream().map(token -> new Tuple2<String, Integer>(token, token.length()))
                    .collect(Collectors.toList()).iterator());

            flatMapToPairRDD.foreach(record -> System.out.println(record));
        }
    }
}
