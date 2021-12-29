package com.mahfooz.spark.rdd.partitioning;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class JavaRddMapPartitionsToPair {

    public static void main(String[] args) {
        String jdbcFile = "C:/Users/malam/jdbc/mysql.properties";
        try (SparkSession spark = SparkSession
                .builder()
                .appName("JavaRddMapPartitionsToPair")
                .master("local[*]")
                .getOrCreate();) {

            Properties properties = new Properties();
            properties.load(new FileInputStream(jdbcFile));

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            JavaPairRDD<String, Integer> pairRDD = intRDD.mapPartitionsToPair(iterator -> {
                Class.forName(properties.getProperty("driver"));
                Connection con = DriverManager.getConnection(properties.getProperty("jdbcUrl"),
                        properties.getProperty("user"),
                        properties.getProperty("password"));
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                List<Tuple2<String, Integer>> namesIndex = new ArrayList<>();
                while (iterator.hasNext()) {
                    int id = iterator.next();
                    ResultSet rs = stmt.executeQuery("select emp_name from emp where emp_id = " + id);
                    if (rs.first()) {
                        namesIndex.add(new Tuple2<>(rs.getString("emp_name"), id));
                    }
                }
                return namesIndex.iterator();
            });

            System.out.println(pairRDD.collect());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
