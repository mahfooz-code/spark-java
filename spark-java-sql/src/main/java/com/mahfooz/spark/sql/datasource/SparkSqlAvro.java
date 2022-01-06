/*

The spark-avro module is external and not included in spark-submit or spark-shell by default.
Since Spark 2.4 release, Spark SQL provides built-in support for reading and writing Apache Avro data.

As with any Spark applications, spark-submit is used to launch your application.
spark-avro_2.12 and its dependencies can be directly added to spark-submit using --packages.

    spark-submit --packages org.apache.spark:spark-avro_2.12:3.2.0 ...

Since spark-avro module is external, there is no .avro API in DataFrameReader or DataFrameWriter.
To load/save data in Avro format, you need to specify the data source option format as avro(or org.apache.spark.sql.avro).

 */
package com.mahfooz.spark.sql.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlAvro {

    private static final String avroFile = "C:/Users/malam/development/data/spark/avro/users.avro";
    private static final String outputAvroFile = "C:/Users/malam/development/data/spark/avro/namesAndFavColors.avro";

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkGlobalTempView")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "C:/Users/malam/development/data/spark/spark-warehouse")
                .getOrCreate();) {


            Dataset<Row> usersDF = spark.read()
                    .format("avro")
                    .load(avroFile);

            usersDF.select("name", "favorite_color")
                    .write()
                    .format("avro")
                    .save(outputAvroFile);
        }
    }
}


