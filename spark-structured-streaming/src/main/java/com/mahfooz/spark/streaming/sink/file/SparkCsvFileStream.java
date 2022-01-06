package com.mahfooz.spark.streaming.sink.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class SparkCsvFileStream {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkCsvFileStream")
                .getOrCreate();

        // Read all the csv files written atomically in a directory
        StructType userSchema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .option("header",true)
                .schema(userSchema)      // Specify schema of the csv files
                .csv("C:/Users/malam/development/data/spark/csv_source");    // Equivalent to format("csv").load("/path/to/directory")

        csvDF.writeStream()
                .format("csv")
                .outputMode("append")
                .option("header",true)
                .option("checkpointLocation","C:/Users/malam/development/data/checkpoint/spark-csv-streaming/csv")
                .start("C:/Users/malam/development/data/spark/csv_source_output")             // Start the computation
                .awaitTermination();
    }
}
