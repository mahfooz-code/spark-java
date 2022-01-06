package com.mahfooz.spark.streaming.reading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class SparkKafkaReadingMutipleTopic {

  private static final String KAFKA_CONFIG_FILE = "C:/Users/malam/kakfa/kafka-multiple.properties";

  public static void main(String[] args) {

    try (SparkSession spark = SparkSession
        .builder()
        .appName("SparkJavaSql")
        .master("local[*]")
        .getOrCreate();) {

      Properties properties = new Properties();
      properties.load(new FileInputStream(KAFKA_CONFIG_FILE));

      // Subscribe to multiple topic
      Dataset<Row> df = spark
          .readStream()
          .format("kafka")
          .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
          .option("subscribe", properties.getProperty("subscribe.topic"))
          .load();

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
