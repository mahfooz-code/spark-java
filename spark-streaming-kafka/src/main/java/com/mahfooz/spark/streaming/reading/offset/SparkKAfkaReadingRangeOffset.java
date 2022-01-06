package com.mahfooz.spark.streaming.reading.offset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SparkKAfkaReadingRangeOffset {

    private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kakfa/kafka-pattern.properties";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkJavaSql")
                .master("local[*]")
                .getOrCreate();) {

            Properties properties = new Properties();
            properties.load(new FileInputStream(KAFKA_CONFIG_FILE));

            // Subscribe to multiple topics, specifying explicit Kafka offsets
            Dataset<Row> df = spark
                    .read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                    .option("subscribe", "topic1,topic2")
                    .option("startingOffsets", "{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
                    .option("endingOffsets", "{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")
                    .load();

            df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
