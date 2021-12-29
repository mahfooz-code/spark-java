package com.mahfooz.spark.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkRddJava {
    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "SparkRddJava App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

            JavaRDD<String> data = javaSparkContext.parallelize(Arrays.asList("where there is a will there is a way"));

            JavaPairRDD<String, Integer> flattenPairs = data
                    .flatMapToPair(text -> Arrays.asList(text.split(" ")).stream()
                            .map(record -> new Tuple2<String, Integer>(record, 1)).iterator());

            flattenPairs.foreach(record -> System.out.println(record));

            JavaPairRDD<String, Integer> wordCountRDD = flattenPairs
                    .reduceByKey((x, y) -> x + y);

            wordCountRDD.foreach(record -> System.out.println(record));
        }
    }
}
