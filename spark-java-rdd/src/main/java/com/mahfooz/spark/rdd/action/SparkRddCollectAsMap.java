/*

The collectAsMap action is called on PairRDD to collect the dataset in key/value format at the driver program.
Since all the partition data of RDD is dumped at the driver all the effort should be made to minimize the size.

*/
package com.mahfooz.spark.rdd.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkRddCollectAsMap {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCollectAsMap")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            // collectAsMap
            List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
            list.add(new Tuple2<String, Integer>("a", 1));
            list.add(new Tuple2<String, Integer>("b", 2));
            list.add(new Tuple2<String, Integer>("c", 3));
            list.add(new Tuple2<String, Integer>("a", 4));

            JavaPairRDD<String, Integer> pairRDD = javaSparkContext.parallelizePairs(list);
            Map<String, Integer> collectMap = pairRDD.collectAsMap();
            for (Entry<String, Integer> entrySet : collectMap.entrySet()) {
                System.out
                        .println("The key of Map is : " + entrySet.getKey() + " and the value is : "
                                + entrySet.getValue());
            }

        }
    }
}
