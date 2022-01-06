package com.mahfooz.spark.df.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataframeGroupBy {

    private static final String jsonFile="C:/Users/malam/development/data/spark/json/people.json";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataframeDsl")
                .master("local[*]")
                .getOrCreate();) {

            Dataset<Row> df = spark.read().json(jsonFile);
            // Count people by age
            df.groupBy("age").count().show();
        }
    }
}
