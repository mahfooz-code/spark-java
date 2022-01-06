/*

maxFileAge: Maximum age of a file that can be found in this directory, before it is ignored.

 */
package com.mahfooz.spark.streaming.source.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkMaxFileAgeSourceStream {

    private static final String dataDir="C:/Users/malam/development/data/spark/file/maxage";
    private static final String checkpointDir="C:/Users/malam/development/data/checkpoint/file/text/maxage";

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkMaxFileAgeSourceStream")
                .getOrCreate();

        Dataset<Row> inputData=spark.readStream()
                .option("maxFileAge", "5s")
                .textFile(dataDir)
                .select("value");


        inputData.writeStream()
                .option("checkpointLocation", checkpointDir)
                .format("console")
                .start()
                .awaitTermination();
    }
}
