/*

1) Importing it as import org.apache.spark.sql.avro.package$;
2) using package$.MODULE$.from_avro(...) should work

 */
package com.mahfooz.spark.sql.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.avro.package$;
import static org.apache.spark.sql.functions.col;

public class SparkSqlAvroSchema {

    private static final String avroSchema="./examples/src/main/resources/user.avsc";

    public static void main(String[] args) throws IOException {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkGlobalTempView")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "C:/Users/malam/development/data/spark/spark-warehouse")
                .getOrCreate();) {
            // `from_avro` requires Avro schema in JSON string format.
            String jsonFormatSchema = new String(Files.readAllBytes(Paths.get(avroSchema)));

            Dataset<Row> df = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                    .option("subscribe", "topic1")
                    .load();

            // 1. Decode the Avro data into a struct;
            // 2. Filter by column `favorite_color`;
            // 3. Encode the column `name` in Avro format.
            Dataset<Row> output = df
                    .select(package$.MODULE$.from_avro(col("value"), jsonFormatSchema).as("user"))
                    .where("user.favorite_color == \"red\"")
                    .select(package$.MODULE$.to_avro(col("user.name")).as("value"));

            StreamingQuery query = output
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                    .option("topic", "topic2")
                    .start();
        }
    }
}
