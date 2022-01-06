package com.mahfooz.spark.dataset.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkDatasetSchema {

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDatasetSchema")
                .master("local[*]")
                .getOrCreate();) {

            // Create a RDD
            JavaRDD<String> deptRDD = spark.sparkContext()
                    .textFile("C:/Users/malam/development/data/sparkcsv/dept.txt", 1)
                    .toJavaRDD();

            // Convert the RDD to RDD<Rows>
            JavaRDD<Row> deptRows = deptRDD.filter(str -> !str.contains("deptno")).map(
                    record -> {
                        String[] cols = record.split(",");
                        return RowFactory.create(cols[0].trim(), cols[1].trim(), cols[2].trim());
                    });

            // Create schema
            String[] schemaArr = deptRDD.first().split(",");
            List<StructField> structFieldList = new ArrayList<>();
            for (String fieldName : schemaArr) {
                StructField structField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                structFieldList.add(structField);
            }
            StructType schema = DataTypes.createStructType(structFieldList);

            Dataset<Row> deptDf = spark.createDataFrame(deptRows, schema);
            deptDf.printSchema();
            deptDf.show();
        }
    }
}
