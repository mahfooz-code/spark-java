/*

 */
package com.mahfooz.spark.streaming.source.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkFileNamePatternStream {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkFileNamePatternStream")
                .getOrCreate();


        Dataset<Row> df = spark.readStream()
                .format("text")
                .option("maxFilesPerTrigger", 1)
                .option("fileNameOnly","true")
                .option("path","dataset*.txt")
                .load("C:/Users/malam/development/data/spark/filepattern");

        df.printSchema();

        df.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();

    }
}
