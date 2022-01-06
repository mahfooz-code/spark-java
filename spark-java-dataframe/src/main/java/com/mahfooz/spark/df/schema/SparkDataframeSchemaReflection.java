/*
When JavaBean classes cannot be defined ahead of time (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users), a Dataset<Row> can be created programmatically with three steps.

    Create an RDD of Rows from the original RDD.
    Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.

 */
package com.mahfooz.spark.df.schema;

import com.mahfooz.spark.df.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataframeSchemaReflection {

    private static String textFile="C:/Users/malam/development/data/spark/text/people.txt";

    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataframeSchemaReflection")
                .master("local[*]")
                .getOrCreate();) {

            // Create an RDD
            JavaRDD<Person> peopleRDD = spark.sparkContext()
                    .textFile(textFile, 1)
                    .toJavaRDD().map(line -> {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    });

            // Apply the schema to the RDD
            Dataset<Row> peopleDataFrame = spark.createDataFrame(peopleRDD, Person.class);

            peopleDataFrame.show();
        }
    }
}
