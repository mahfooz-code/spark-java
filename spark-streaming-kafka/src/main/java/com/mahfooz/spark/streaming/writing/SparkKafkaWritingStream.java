/*

kafka-topics --create --partitions 1 --replication-factor 1 --topic spark-kafka-topic --bootstrap-server 10.133.43.96:9092

 */
package com.mahfooz.spark.streaming.writing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SparkKafkaWritingStream {

    private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kafka/kafka-single.properties";

    public static void main(String[] args) {
        try {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("SparkKafkaWritingStream")
                    .master("local[*]")
                    .getOrCreate();

            Dataset<Row> df = spark.readStream()
                    .format("rate")
                    .option("rowsPerSecond", 1)
                    .load();

            df.printSchema();

            Properties properties = new Properties();
            properties.load(new FileInputStream(KAFKA_CONFIG_FILE));

            df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)")
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers",properties.getProperty("kafka.bootstrap.servers"))
                    .option("topic", properties.getProperty("subscribe.topic"))
                    .option("checkpointLocation","file:/C:/Users/malam/development/data/spark-kafka-single")
                    .start().awaitTermination();

        } catch (StreamingQueryException | FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
