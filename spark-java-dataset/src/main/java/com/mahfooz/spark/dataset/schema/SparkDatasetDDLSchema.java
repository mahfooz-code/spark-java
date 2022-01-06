package com.mahfooz.spark.dataset.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetDDLSchema {

    private static final String jsonFile="C:/Users/malam/development/data/spark/json/blogs.json";

    public static void main(String[] args) {
        try(SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkDatasetDDLSchema")
                .getOrCreate();) {

            String schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,`Published` STRING, " +
                    "`Hits` INT, `Campaigns` ARRAY<STRING>";

            Dataset<Row> blogsDF = spark.read().schema(schema).json(jsonFile);
            blogsDF.show(false);
            // Print the schema
            blogsDF.printSchema();
            System.out.println(blogsDF.schema());
        }
    }
}
