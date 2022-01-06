package com.mahfooz.spark.dataset.creation;

import java.util.Arrays;

import com.mahfooz.spark.dataset.model.Employee;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class SparkDatasetRdd {

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataset")
                .master("local[*]")
                .getOrCreate()) {
            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Employee> empRDD = javaSparkContext
                    .parallelize(Arrays.asList(new Employee("Foo", 1), new Employee("Bar", 2)));

            Dataset<Employee> dsEmp = spark.createDataset(empRDD.rdd(), Encoders.bean(Employee.class));
            Dataset<Employee> filter = dsEmp.filter((FilterFunction<Employee>) emp -> emp.getId() > 1);
            filter.show();
        }
    }

}
