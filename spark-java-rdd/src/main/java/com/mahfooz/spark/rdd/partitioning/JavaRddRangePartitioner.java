/*

Range Partitioner partitions the keys based on a range.
All keys falling in the same range will land in the same partition.
It is recommended that the data type follows the natural ordering, for example, integers, long, and so on.
org.apache.spark.RangePartitioner provides a parameterized constructor which accepts five parameters as follows:

RangePartitioner (
    int partitions, 
    RDD <? extends scala.Product2< K , V >> rdd, 
    boolean ascending, 
    scala.math.Ordering< K > evidence$1, 
    scala.reflect.ClassTag< K > evidence$2
)

where:

Parameter 1: Number of partitions
Parameter 2: RDD to be partitioned
Parameter 3: Order of the range ascending/descending
Parameter 4: Ordering method for the key
Parameter 5: Class tag for the key

*/
package com.mahfooz.spark.rdd.partitioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.RangePartitioner;

import scala.Tuple2;

public class JavaRddRangePartitioner {

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

            RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);

            // Range Partitioner
            RangePartitioner<Integer, String> rangePartitioner = new RangePartitioner(4,
                    rdd,
                    true, scala.math.Ordering.Int$.MODULE$,
                    scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
            JavaPairRDD<Integer, String> rangePartitioned = pairRdd.partitionBy(rangePartitioner);

            JavaRDD<String> mapPartitionsWithIndex = rangePartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
                List<String> list = new ArrayList<>();
                while (tupleIterator.hasNext()) {
                    list.add("Partition number:" + index + ",key:" + tupleIterator.next()._1());
                }
                return list.iterator();
            }, true);

            System.out.println(mapPartitionsWithIndex.collect());
        }

    }

}
