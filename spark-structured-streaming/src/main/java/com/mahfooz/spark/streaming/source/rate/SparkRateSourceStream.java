/*

rowsPerSecond (e.g. 100, default: 1):
    How many rows should be generated per second.

rampUpTime (e.g. 5s, default: 0s): How long to ramp up before the generating speed becomes rowsPerSecond.
    Using finer granularities than seconds will be truncated to integer seconds.

numPartitions (e.g. 10, default: Spark's default parallelism):
    The partition number for the generated rows.

The source will try its best to reach rowsPerSecond, but the query may be resource constrained, and numPartitions can be
tweaked to help reach the desired speed.

 */
package com.mahfooz.spark.streaming.source.rate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkRateSourceStream {
    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkRateSourceStream")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        //This lines DataFrame represents an unbounded table containing the streaming text data.
        Dataset<Row> lines = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
                .option("numPartitions",5)
                .option("rampUpTime",5)
                .load();

        lines.printSchema();

        // Split the lines into words
        Dataset<Long> words = lines
                .select("value")
                .as(Encoders.LONG());

        words.printSchema();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = words.writeStream()
                .format("console")
                .start();

        query.awaitTermination();

    }
}
