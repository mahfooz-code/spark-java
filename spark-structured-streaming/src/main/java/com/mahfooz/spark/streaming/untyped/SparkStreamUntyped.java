/*

 */
package com.mahfooz.spark.streaming.untyped;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkStreamUntyped {

    private static final String dataDir="C:/Users/malam/development/data/spark/untyped";

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession=SparkSession
                .builder()
                .appName("spark")
                .master("local[*]")
                .getOrCreate();

        // streaming DataFrame with IOT device data with schema
        // { device: string, type: string, signal: double, time: DateType }
        StructType schema=new StructType()
                .add("device","string")
                .add("type","string")
                .add("signal","double")
                .add(DataTypes.createStructField("time", DataTypes.DateType, true));

        Dataset<Row> df = sparkSession.readStream()
                .schema(schema)
                .json(dataDir);

        // Select the devices which have signal more than 10
        df.select("device").where("signal > 10"); // using untyped APIs

        // Running count of the number of updates for each device type
        df.groupBy("type").count(); // using untyped API

        df.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();
    }
}
