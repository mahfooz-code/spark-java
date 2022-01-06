package com.mahfooz.spark.streaming.source.socket;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class SocketSourceStream {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SocketSourceStream")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        //This lines DataFrame represents an unbounded table containing the streaming text data.
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "mtmdevhdoped01")
                .option("port", 9991)
                .load();

        lines.printSchema();

        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        words.printSchema();

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("checkpointLocation","file:/C:/Users/malam/development/data/checkpoint/spark-streaming-ds")
                .start();

        query.awaitTermination();

    }
}
