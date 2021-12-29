/*

export AWS_ACCESS_KEY_ID=accessKeyIDValue
export AWS_SECRET_ACCESS_KEY=securityAccesKeyValue 

*/
package com.mahfooz.spark.rdd.datasource.s3;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddS3 {
    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddCheck")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            // Using S3N
            javaSparkContext.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "accessKeyIDValue");
            javaSparkContext.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "securityAccesKeyValue");

            // Using S3A
        }
    }
}
