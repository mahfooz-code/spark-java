package com.mahfooz.spark.rdd.datasource.localfile;

import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkRddLocalFile {
    public static void main(String[] args) {
        String localFileSystem = "C:/Users/malam/development/data/spark/us-states.csv";
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCheck")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            JavaRDD<String> localFile = javaSparkContext.textFile(localFileSystem);

            JavaPairRDD<String, Integer> wordCountLocalFile = localFile
                    .flatMap(x -> Arrays.asList(x.split(",")[1]).iterator())
                    .mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
                    .reduceByKey((x, y) -> x + y);

            wordCountLocalFile.collect().forEach(record -> System.out.println(record));
        }
    }
}
