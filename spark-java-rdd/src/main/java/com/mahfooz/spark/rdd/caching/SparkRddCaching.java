package com.mahfooz.spark.rdd.caching;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class SparkRddCaching {
    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCaching")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            // cache() and persist()
            JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2,
                    3, 4, 5), 3).cache();

            JavaRDD<Integer> evenRDD = rdd.filter(record -> (record % 2) == 0);

            evenRDD.persist(StorageLevel.MEMORY_AND_DISK());
            evenRDD.foreach(record -> System.out.println("The value of RDD are :" + record));

            // unpersisting the RDD
            evenRDD.unpersist();
            rdd.unpersist();
        }
    }
}
