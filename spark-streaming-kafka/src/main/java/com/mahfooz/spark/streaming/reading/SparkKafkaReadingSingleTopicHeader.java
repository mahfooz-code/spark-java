package com.mahfooz.spark.streaming.reading;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkKafkaReadingSingleTopicHeader {

  private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kakfa/kafka-single.properties";

  public static void main(String[] args) {
    try (SparkSession spark = SparkSession
        .builder()
        .appName("SparkJavaSql")
        .master("local[*]")
        .getOrCreate();) {

      Properties properties = new Properties();
      properties.load(new FileInputStream(KAFKA_CONFIG_FILE));

      // Subscribe to 1 topic
      Dataset<Row> df = spark
          .readStream()
          .format("kafka")
          .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
          .option("subscribe", properties.getProperty("subscribe.topic"))
          .option("includeHeaders", "true")
          .load();

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers");

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
