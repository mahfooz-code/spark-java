/*

With a filter transformation, the function is executed on all the elements of the source RDD and only those elements, for which the function returns true, are selected to form the target RDD.

*/
package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRddFilter {
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

            // Filter RDD
            JavaRDD<Integer> filteredRDD = intRDD.filter(x -> (x % 2 == 0));
            filteredRDD.foreach(record -> System.out.println(record));
        }
    }
}
