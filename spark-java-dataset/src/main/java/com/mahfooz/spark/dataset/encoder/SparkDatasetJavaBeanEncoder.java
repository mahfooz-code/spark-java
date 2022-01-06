package com.mahfooz.spark.dataset.encoder;

import com.mahfooz.spark.dataset.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class SparkDatasetJavaBeanEncoder {

    public static void main(String[] args) {
        try(SparkSession spark = SparkSession
                .builder()
                .appName("SparkDatasetPrimitiveEncoder")
                .master("local[*]")
                .getOrCreate();) {
            // Create an instance of a Bean class
            Person person = new Person();
            person.setName("Andy");
            person.setAge(32);

            // Encoders are created for Java beans
            Encoder<Person> personEncoder = Encoders.bean(Person.class);
            Dataset<Person> javaBeanDS = spark.createDataset(
                    Collections.singletonList(person),
                    personEncoder
            );
            javaBeanDS.show();
        }
    }
}
