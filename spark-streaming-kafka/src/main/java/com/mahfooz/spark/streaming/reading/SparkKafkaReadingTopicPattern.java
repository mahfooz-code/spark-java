package com.mahfooz.spark.streaming.reading;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkKafkaReadingTopicPattern {

    private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kakfa/kafka-pattern.properties";

    public static void main(String[] args) {
        try {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("SparkKafkaReadingTopicPattern")
                    .master("local[*]")
                    .getOrCreate();

            Properties properties = new Properties();
            properties.load(new FileInputStream(KAFKA_CONFIG_FILE));

            // Subscribe to a pattern
            Dataset<Row> df = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
                    .option("subscribePattern", properties.getProperty("subscribePattern.topic"))
                    .load();
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

            df.writeStream()
                    .format("console")
                    .option("truncate", "false")
                    .start()
                    .awaitTermination();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}