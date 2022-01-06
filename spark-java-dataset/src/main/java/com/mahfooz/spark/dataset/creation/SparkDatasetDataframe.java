package com.mahfooz.spark.dataset.creation;

import com.mahfooz.spark.dataset.model.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetDataframe {

    private static final String jsonFile = "C:/Users/malam/development/data/spark/json/people.json";

    public static void main(String[] args) {
        try(SparkSession spark = SparkSession
                .builder()
                .appName("SparkDatasetPrimitiveEncoder")
                .master("local[*]")
                .getOrCreate();) {

            //encoder
            Encoder<Person> personEncoder = Encoders.bean(Person.class);

            Dataset<Person> peopleDS = spark.read().json(jsonFile).as(personEncoder);
            peopleDS.show();
        }
    }
}
