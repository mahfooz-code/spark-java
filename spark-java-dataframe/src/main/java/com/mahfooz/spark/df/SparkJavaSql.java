package com.mahfooz.spark.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJavaSql {

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkJavaSql")
                .master("local[*]")
                .getOrCreate();) {
            Dataset<Row> csv = spark.read().format("csv").option("header", "true")
                    .load("C:/Users/malam/development/data/spark/csv/emp.csv");
            csv.show();
        }
    }
}
