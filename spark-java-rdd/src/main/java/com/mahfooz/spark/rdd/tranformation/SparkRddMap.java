package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRddMap {
    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "SparkRddMap App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            // Input RDD
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);
            intRDD.foreach(record -> System.out.println(record));

            // Mapped RDD
            JavaRDD<Integer> mappedRDD = intRDD.map(x -> x + 1);
            mappedRDD.foreach(record -> System.out.println(record));
        }
    }
}
