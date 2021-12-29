package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRddUnion {

    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "SparkRddUnion App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

            // Creating RDD1
            JavaRDD<Integer> intRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));

            // Creating RDD2
            JavaRDD<Integer> intRDD2 = javaSparkContext.parallelize(Arrays.asList(4, 3, 2, 1, 2, 3));

            JavaRDD<Integer> intRDD = intRDD1.union(intRDD2);
            intRDD.foreach(record -> System.out.println(record));

        }
    }

}
