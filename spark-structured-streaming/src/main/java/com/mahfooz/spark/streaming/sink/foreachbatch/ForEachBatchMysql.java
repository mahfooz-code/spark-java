package com.mahfooz.spark.streaming.sink.foreachbatch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;


public class ForEachBatchMysql {

    private static final String dataDir = "C:/Users/malam/development/data/spark/mysql";
    private static final String jdbcFile = "C:/Users/malam/jdbc/mysql.properties";

    public static void main(String[] args) throws StreamingQueryException, IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Mysql")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType()
                .add("device", "string")
                .add("action", "string")
                .add("time", "timestamp");

        Dataset<Row> inputDF = spark.readStream()
                .schema(schema)
                .json(dataDir);

        Dataset<Row> countsDF = inputDF.groupBy(col("action"),
                        window(col("time"), "1 hour"))
                .count();

        countsDF.printSchema();

        //Writing to jdbc sink
        Properties properties = new Properties();
        properties.load(new FileInputStream(jdbcFile));

        countsDF.writeStream()
                .foreachBatch((dataset, batchId) -> {
                    dataset.select(col("action"),
                                    col("window.start"),
                                    col("window.end")
                                    , col("count"))
                            .write()
                            .format("jdbc")
                            .option("driver", properties.getProperty("driver"))
                            .option("url", properties.getProperty("jdbcUrl"))
                            .option("user", properties.getProperty("user"))
                            .option("password", properties.getProperty("password"))
                            .option("dbtable", "events")
                            .save();
                })
                .start()
                .awaitTermination();
    }
}
