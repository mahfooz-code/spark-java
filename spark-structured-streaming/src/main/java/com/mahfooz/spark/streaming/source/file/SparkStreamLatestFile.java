package com.mahfooz.spark.streaming.source.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkStreamLatestFile {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkStreamLatestFile")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        //This lines DataFrame represents an unbounded table containing the streaming text data.
        Dataset<Row> lines = spark
                .readStream()
                .format("text")
                .option("latestFirst", true)
                .option("path","")
                .option("fileNameOnly","")
                .load();

        lines.printSchema();

        // Split the lines into words
        Dataset<Long> words = lines
                .select("value")
                .as(Encoders.LONG());

        words.printSchema();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = words.writeStream()
                .format("console")
                .start();

        query.awaitTermination();

    }
}
