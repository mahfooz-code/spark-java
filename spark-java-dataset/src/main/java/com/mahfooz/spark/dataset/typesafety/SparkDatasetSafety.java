package com.mahfooz.spark.dataset.typesafety;

import java.util.Arrays;
import com.mahfooz.spark.dataset.model.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkDatasetSafety {
    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkDatasetSchema")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Employee> empRDD = javaSparkContext
                    .parallelize(Arrays.asList(new Employee("Foo", 1), new Employee("Bar", 2)));

            Dataset<Employee> dsEmp = spark.createDataset(empRDD.rdd(), Encoders.bean(Employee.class));

            // 1. Using strongly typed APIs
            dsEmp.filter((FilterFunction<Employee>) emp -> emp.getId() > 1).show();

            // 2. Using untyped APIs:
            // An untyped transformation provides SQL-like column specific names, and the
            // operation can be used to operate upon data.
            dsEmp.filter("id > 1").show();

            // 3. Using DSL.
            dsEmp.filter(col("id").gt(1)).show();
        }
    }
}
