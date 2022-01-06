package com.mahfooz.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJavaSql {

    private static String csvFile="C:/Users/malam/development/data/spark/csv/emp.csv";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkJavaSql")
                .master("local[*]")
                .getOrCreate();) {

            Dataset<Row> csv = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .load(csvFile);

            // Register the DataFrame as a SQL temporary view
            csv.createOrReplaceTempView("emp");
            Dataset<Row> sqlDF = spark.sql("SELECT * FROM emp");
            sqlDF.show();
        }
    }
}
