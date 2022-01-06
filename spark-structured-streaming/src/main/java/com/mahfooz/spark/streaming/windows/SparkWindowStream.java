package com.mahfooz.spark.streaming.windows;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class SparkWindowStream {

    private static final String dataDir="C:/Users/malam/development/data/spark/windows";

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark=SparkSession
                .builder()
                .appName("SparkWindowStream")
                .master("local[*]")
                .getOrCreate();

        //schema { timestamp: Timestamp, word: String }
        StructType schema=new StructType()
                .add("timestamp", "timestamp",false)
                .add("word","string",false);

        Dataset<Row> words = spark.readStream()
                .schema(schema)
                .json(dataDir);

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words.groupBy(
                functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
                words.col("word")
        ).count();

        windowedCounts.writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }
}
