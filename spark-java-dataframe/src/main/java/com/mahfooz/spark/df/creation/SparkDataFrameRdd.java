package com.mahfooz.spark.df.creation;

import com.mahfooz.spark.df.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataFrameRdd {

    private static String textFile="C:/Users/malam/development/data/spark/text/people.txt";

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataframeDsl")
                .master("local[*]")
                .getOrCreate();) {

            // Create an RDD of Person objects from a text file
            JavaRDD<Person> peopleRDD = spark.read()
                    .textFile(textFile)
                    .javaRDD()
                    .map(line -> {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    });

            // Apply a schema to an RDD of JavaBeans to get a DataFrame
            Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
            peopleDF.show();
        }
    }
}
