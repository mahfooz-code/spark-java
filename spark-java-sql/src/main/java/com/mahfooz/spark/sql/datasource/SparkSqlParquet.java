/*

spark.sql.sources.default

 */
package com.mahfooz.spark.sql.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlParquet {

    private static final String parquetFile="C:/Users/malam/development/data/spark/parquet/world_temprature_by_date";
    private  static final String outputParquetFile="C:/Users/malam/development/data/spark/parquet/world_temprature";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkGlobalTempView")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","C:/Users/malam/development/data/spark/spark-warehouse")
                .getOrCreate();) {
            Dataset<Row> usersDF = spark.read().load(parquetFile);

            usersDF.printSchema();
            usersDF.createOrReplaceTempView("world_temperature");

            spark.sql("select * from world_temperature")
                    .write()
                    .partitionBy("year")
                    .save(outputParquetFile);
        }
    }
}
