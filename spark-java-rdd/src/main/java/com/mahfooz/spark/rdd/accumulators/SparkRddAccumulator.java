package com.mahfooz.spark.rdd.accumulators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

public class SparkRddAccumulator {

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddAccumulator")
                .master("local[*]")
                .getOrCreate();) {

            LongAccumulator longAccumulator = spark.sparkContext().longAccumulator("ExceptionCounter");

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaRDD<String> textFile = javaSparkContext
                    .textFile("C:/Users/malam/development/data/spark/log/logFileWithException.log", 5);

            textFile.foreach(line -> {
                if (line.contains("Exception")) {
                    longAccumulator.add(1);
                    System.out.println("The intermediate value in loop " + longAccumulator.value());
                }
            });
            System.out.println("The final value of Accumulator : " + longAccumulator.value());

            // Collection accumulator
            CollectionAccumulator<Long> collectionAccumulator = spark.sparkContext().collectionAccumulator();
            textFile.foreach(line -> {
                if (line.contains("Exception")) {
                    collectionAccumulator.add(1L);
                    System.out.println("The intermediate value in loop " + collectionAccumulator.value());
                }
            });
            System.out.println("The final value of Accumulator : " + collectionAccumulator.value());
        }
    }

}
