/*
When JavaBean classes cannot be defined ahead of time (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users), a Dataset<Row> can be created programmatically with three steps.

    Create an RDD of Rows from the original RDD.
    Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.

 */
package com.mahfooz.spark.df.schema;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkDataframeDynamicSchema {

    private static String textFile="C:/Users/malam/development/data/spark/text/people.txt";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataframeDynamicSchema")
                .master("local[*]")
                .getOrCreate();) {

            // Create an RDD
            JavaRDD<String> peopleRDD = spark.sparkContext()
                    .textFile(textFile, 1)
                    .toJavaRDD();

            // The schema is encoded in a string
            String schemaString = "name age";

            // Generate the schema based on the string of schema
            List<StructField> fields = new ArrayList<>();
            for (String fieldName : schemaString.split(" ")) {
                StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            // Convert records of the RDD (people) to Rows
            JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            });

            // Apply the schema to the RDD
            Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

            peopleDataFrame.show();
        }
    }
}
