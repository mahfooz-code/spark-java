package com.mahfooz.spark.streaming.source.file.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class SparkCsvSourceFileStream {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkCsvSourceFileStream")
                .getOrCreate();

        // Read all the csv files written atomically in a directory
        StructType userSchema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        // generate streaming DataFrames that are untyped, meaning that the schema of the DataFrame is not checked at
        // compile time, only checked at runtime when the query is submitted.
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .option("header",true)
                .schema(userSchema)      // Specify schema of the csv files
                .csv("C:/Users/malam/development/data/spark/csv_source");
        // Equivalent to format("csv").load("/path/to/directory")

        // Start running the query that prints the running counts to the console
        StreamingQuery query = csvDF.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
