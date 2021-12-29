package com.mahfooz.spark.rdd.partitioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddMapPartitions {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("JavaRddCustomerPartitioner")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            intRDD.mapPartitions(iterator -> {
                List<Integer> intList = new ArrayList<>();
                while (iterator.hasNext()) {
                    intList.add(iterator.next() + 1);
                }
                return intList.iterator();
            });

            System.out.println();
        }

    }
}
