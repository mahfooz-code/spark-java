/*

If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application
terminates, you can create a global temporary view.

Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer
it, e.g. SELECT * FROM global_temp.view.

 */
package com.mahfooz.spark.sql.view;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkGlobalTempView {

    private static String csvFile="C:/Users/malam/development/data/spark/csv/emp.csv";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkGlobalTempView")
                .master("local[*]")
                .getOrCreate();) {

            Dataset<Row> df = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .load(csvFile);

            // Register the DataFrame as a global temporary view
            df.createGlobalTempView("emp");

            // Global temporary view is tied to a system preserved database `global_temp`
            spark.sql("SELECT * FROM global_temp.emp").show();

            // Global temporary view is cross-session
            spark.newSession().sql("SELECT * FROM global_temp.emp").show();

        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
