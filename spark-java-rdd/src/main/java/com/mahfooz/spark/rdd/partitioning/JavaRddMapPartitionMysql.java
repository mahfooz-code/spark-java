package com.mahfooz.spark.rdd.partitioning;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaRddMapPartitionMysql {
    public static void main(String[] args) {
        String jdbcFile = "C:/Users/malam/jdbc/mysql.properties";
        try (SparkSession spark = SparkSession
                .builder()
                .appName("JavaRddCustomerPartitioner")
                .master("local[*]")
                .getOrCreate();) {

            Properties properties = new Properties();
            properties.load(new FileInputStream(jdbcFile));

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            JavaRDD<String> jdbcRDD = intRDD.mapPartitions(iterator -> {
                Class.forName(properties.getProperty("driver"));
                List<String> names = new ArrayList<>();
                try (Connection con = DriverManager.getConnection(properties.getProperty("jdbcUrl"),
                        properties.getProperty("user"),
                        properties.getProperty("password"));) {
                    Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

                    while (iterator.hasNext()) {
                        ResultSet rs = stmt.executeQuery("select emp_name from emp where emp_id = " + iterator.next());
                        if (rs.first()) {
                            names.add(rs.getString("emp_name"));
                        }
                    }
                }
                return names.iterator();
            });

            System.out.println(jdbcRDD.collect());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
