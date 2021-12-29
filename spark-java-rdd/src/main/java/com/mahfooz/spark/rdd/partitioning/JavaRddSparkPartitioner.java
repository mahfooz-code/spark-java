/*

{hashCode of the key}%{number of partitions}

*/
package com.mahfooz.spark.rdd.partitioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.HashPartitioner;

import scala.Tuple2;

public class JavaRddSparkPartitioner {

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCheck")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaPairRDD<Integer, String> pairRdd = javaSparkContext.parallelizePairs(
                    Arrays.asList(new Tuple2<Integer, String>(1, "A"), new Tuple2<Integer, String>(2, "B"),
                            new Tuple2<Integer, String>(3, "C"), new Tuple2<Integer, String>(4, "D"),
                            new Tuple2<Integer, String>(5, "E"), new Tuple2<Integer, String>(6, "F"),
                            new Tuple2<Integer, String>(7, "G"), new Tuple2<Integer, String>(8, "H")),
                    3);

            // Hash Partitioner
            JavaPairRDD<Integer, String> hashPartitioned = pairRdd.partitionBy(new HashPartitioner(2));
            System.out.println(hashPartitioned.getNumPartitions());

            JavaRDD<String> mapPartitionsWithIndex = hashPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
                List<String> list = new ArrayList<>();
                while (tupleIterator.hasNext()) {
                    list.add("Partition number:" + index + ",key:" + tupleIterator.next()._1());
                }
                return list.iterator();
            }, true);
            System.out.println(mapPartitionsWithIndex.collect());

            // Range Partitioner

        }

    }

}
