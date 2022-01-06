package com.mahfooz.spark.streaming.reading.offset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SparkKafkaReadingEarliestLatest {
    private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kakfa/kafka-pattern.properties";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkJavaSql")
                .master("local[*]")
                .getOrCreate();) {

            Properties properties = new Properties();
            properties.load(new FileInputStream(KAFKA_CONFIG_FILE));
            // Subscribe to a pattern, at the earliest and latest offsets
            Dataset<Row> df = spark
                    .read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                    .option("subscribePattern", "topic.*")
                    .option("startingOffsets", "earliest")
                    .option("endingOffsets", "latest")
                    .load();
            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
