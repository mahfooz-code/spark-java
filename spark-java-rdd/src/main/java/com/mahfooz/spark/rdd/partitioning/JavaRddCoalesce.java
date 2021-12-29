package com.mahfooz.spark.rdd.partitioning;

import java.util.Arrays;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class JavaRddCoalesce {

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("JavaRddCoalesce")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaPairRDD<Integer, String> unPartitionedRDD = javaSparkContext.parallelizePairs(
                    Arrays.asList(new Tuple2<Integer, String>(8, "h"), new Tuple2<Integer, String>(5, "e"),
                            new Tuple2<Integer, String>(4, "d"), new Tuple2<Integer, String>(2, "a"),
                            new Tuple2<Integer, String>(7, "g"), new Tuple2<Integer, String>(6, "f"),
                            new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(3, "c")));

            JavaPairRDD<Integer, String> partitionedRDD = unPartitionedRDD
                    .repartitionAndSortWithinPartitions(new HashPartitioner(3));

            JavaPairRDD<Integer, String> coaleaseRDD = partitionedRDD.coalesce(2);

            System.out.println(coaleaseRDD.collect());
        }
    }

}
