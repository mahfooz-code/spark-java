package com.mahfooz.spark.streaming.typed;

import com.mahfooz.spark.streaming.domain.DeviceData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkTypedDataStreaming {

    private static final String dataDir="C:/Users/malam/development/data/spark/typed";
    private static final String checkpointDir="C:/Users/malam/development/data/checkpoint/typed";

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

        // streaming Dataset with IOT device data
        Dataset<DeviceData> ds = df.as(ExpressionEncoder.javaBean(DeviceData.class));
        ds.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();
    }
}
