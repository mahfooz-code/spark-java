package com.mahfooz.spark.df.dsl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkDataframeDsl {

    private static final String jsonFile="C:/Users/malam/development/data/spark/json/people.json";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataframeDsl")
                .master("local[*]")
                .getOrCreate();) {

            Dataset<Row> df = spark.read().json(jsonFile);

            // Displays the content of the DataFrame to stdout
            df.show();

            // Print the schema in a tree format
            df.printSchema();

            // Select only the "name" column
            df.select("name").show();

            // Select everybody, but increment the age by 1
            df.select(col("name"), col("age").plus(1)).show();
        }
    }
}
