/*

At the core of the Dataset API is a new concept called an encoder, which is responsible for converting between 
JVM objects and tabular representation.

The tabular representation is stored using Spark’s internal Tungsten binary format, allowing for operations on 
serialized data and improved memory utilization.  

*/
package com.mahfooz.spark.dataset.encoder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkDatasetStringEncoder {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("")
                .getOrCreate();

        Dataset<String> lines = spark.read()
                .text("/wikipedia").as(Encoders.STRING());

        lines
                .flatMap((String record) -> Arrays.asList(record.split(" ")).stream().iterator(),
                        Encoders.STRING())
                .filter((String record) -> record.isEmpty());

    }
}
