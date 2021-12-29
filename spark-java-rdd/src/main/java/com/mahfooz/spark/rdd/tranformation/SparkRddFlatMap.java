package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRddFlatMap {
    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "SparkRddFlatMap App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            // Input RDD
            JavaRDD<String> stringRDD = javaSparkContext.parallelize(Arrays.asList("Hello Spark", "Hello Java"), 3);
            stringRDD.foreach(record -> System.out.println(record));

            // Filter RDD
            JavaRDD<String> flatMappedRDD = stringRDD.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
            flatMappedRDD.foreach(record -> System.out.println(record));
        }
    }
}
