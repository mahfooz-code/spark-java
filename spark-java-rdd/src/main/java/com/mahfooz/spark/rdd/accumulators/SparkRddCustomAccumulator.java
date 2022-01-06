package com.mahfooz.spark.rdd.accumulators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddCustomAccumulator {
    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCustomAccumulator")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            ListAccumulator listAccumulator = new ListAccumulator();
            spark.sparkContext().register(listAccumulator, "ListAccumulator");

            JavaRDD<String> textFile = javaSparkContext
                    .textFile("C:/Users/malam/development/data/spark/log/logFileWithException.log", 5);

            textFile.foreach(line -> {
                if (line.contains("Exception")) {
                    listAccumulator.add("1");
                    System.out.println("The intermediate value in loop " + listAccumulator.value());
                }
            });

            System.out.println("The final value of Accumulator : " + listAccumulator.value());
        }
    }
}
