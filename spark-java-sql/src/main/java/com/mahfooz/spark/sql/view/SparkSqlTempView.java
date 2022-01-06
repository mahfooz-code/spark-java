/*

Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.

 */
package com.mahfooz.spark.sql.view;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlTempView {

    private static String csvFile="C:/Users/malam/development/data/spark/csv/emp.csv";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkJavaSql")
                .master("local[*]")
                .getOrCreate();) {

            Dataset<Row> df = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .load(csvFile);

            // Register the DataFrame as a SQL temporary view
            df.createOrReplaceTempView("emp");

            Dataset<Row> sqlDF = spark.sql("SELECT * FROM emp");
            sqlDF.show();
        }
    }
}
