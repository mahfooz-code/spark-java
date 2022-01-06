package com.mahfooz.spark.dataset.encoder;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkDatasetPrimitiveEncoder {

    public static void main(String[] args) {

        try(SparkSession spark = SparkSession
                .builder()
                .appName("SparkDatasetPrimitiveEncoder")
                .master("local[*]")
                .getOrCreate();) {

            // Encoders for most common types are provided in class Encoders
            Encoder<Long> longEncoder = Encoders.LONG();
            Dataset<Long> primitiveDS = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
            Dataset<Long> transformedDS = primitiveDS.map(
                    (MapFunction<Long, Long>) value -> value + 1L,
                    longEncoder);
            transformedDS.show(); // Returns [2, 3, 4]
        }
    }
}
