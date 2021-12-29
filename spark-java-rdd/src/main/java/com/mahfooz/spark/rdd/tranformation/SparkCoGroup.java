/*

This operation also groups two PairRDD. 
Consider, we have two PairRDD of <X,Y> and <X,Z> types .
When CoGroup transformation is executed on these RDDs, it will return an RDD of <X,(Iterable<Y>,Iterable<Z>)> type.
This operation is also called groupwith.
*/
package com.mahfooz.spark.rdd.tranformation;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkCoGroup {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkCoGroup")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaPairRDD<String, String> pairRDD1 = javaSparkContext.parallelizePairs(
                    Arrays.asList(new Tuple2<String, String>("B", "A"), new Tuple2<String, String>("B", "D"),
                            new Tuple2<String, String>("A", "E"), new Tuple2<String, String>("A", "B")));

            JavaPairRDD<String, Integer> pairRDD2 = javaSparkContext.parallelizePairs(
                    Arrays.asList(new Tuple2<String, Integer>("B", 2), new Tuple2<String, Integer>("B", 5),
                            new Tuple2<String, Integer>("A", 7), new Tuple2<String, Integer>("A", 8)));

            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupedRDD = pairRDD1.cogroup(pairRDD2);
            cogroupedRDD.foreach(record -> System.out.println(record));

            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> groupWithRDD = pairRDD1
                    .groupWith(pairRDD2);
            groupWithRDD.foreach(record -> System.out.println(record));

        }
    }
}
