package com.mahfooz.spark.streaming.source.file.partition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.input_file_name;

public class SparkFileSourcePartitionStream {

    private static final String dataDir="C:/Users/malam/development/data/spark/file/partition";
    private static final String checkpointDir="C:/Users/malam/development/data/checkpoint/file/partition";

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkFileSourcePartitionStream")
                .getOrCreate();

        StructType schema=new StructType()
                .add("name", "string")
                .add("age","integer")
                .add("year", "integer");

        Dataset<Row> inputData=spark.readStream()
                .option("header",true)
                .schema(schema)
                .csv(dataDir)
                .withColumn("filename",input_file_name());

        inputData.writeStream()
                .option("checkpointLocation", checkpointDir)
                .format("console")
                .option("truncate","false")
                .option("numRows",10)
                .start()
                .awaitTermination();

    }
}
