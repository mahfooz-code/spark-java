package com.mahfooz.spark.rdd.tranformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkRddMapValues {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddMapValues")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            JavaPairRDD<String, Integer> pairRDD = intRDD.mapPartitionsToPair(t -> {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                while (t.hasNext()) {
                    int element = t.next();
                    list.add(element % 2 == 0 ? new Tuple2<String, Integer>("even", element)
                            : new Tuple2<String, Integer>("odd", element));
                }
                return list.iterator();
            });

            pairRDD.mapValues(v1 -> v1 * 3).collect().forEach(record -> System.out.print(record));
        }
    }
}