package com.mahfooz.spark.streaming.source.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkTriggerFileTextStream {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkTriggerFileJsonStream")
                .getOrCreate();


        Dataset<Row> df = spark.readStream()
                .format("text")
                .option("maxFilesPerTrigger", 1)
                .load("C:/Users/malam/development/data/spark/text");

        df.printSchema();

        df.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();

    }
}
