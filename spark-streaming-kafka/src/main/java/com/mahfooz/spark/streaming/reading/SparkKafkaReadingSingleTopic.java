package com.mahfooz.spark.streaming.reading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SparkKafkaReadingSingleTopic {

    private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kafka/kafka-single.properties";

    public static void main(String[] args) {
        try {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("SparkJavaSql")
                    .master("local[*]")
                    .getOrCreate();

            Properties properties = new Properties();
            properties.load(new FileInputStream(KAFKA_CONFIG_FILE));

            // Subscribe to 1 topic
            Dataset<Row> df = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
                    .option("subscribe", properties.getProperty("subscribe.topic"))
                    .load();

            df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)");

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
