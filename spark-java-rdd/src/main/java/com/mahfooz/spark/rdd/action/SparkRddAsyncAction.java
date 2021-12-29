package com.mahfooz.spark.rdd.action;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkRddAsyncAction {
    public static void main(String[] args) {

        try (SparkSession spark = SparkSession
                .builder()
                .appName("SparkRddStatistics")
                .master("local[*]")
                .getOrCreate();) {

            JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

            // countApprox(long timeout)
            JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 4, 3, 5, 7));

            JavaFutureAction<Long> intCount = intRDD.countAsync();
            System.out.println(" The async count for " + intCount);

            JavaFutureAction<List<Integer>> intCol = intRDD.collectAsync();
            for (Integer val : intCol.get()) {
                System.out.println("The collect val is " + val);
            }

            JavaFutureAction<List<Integer>> takeAsync = intRDD.takeAsync(3);
            for (Integer val : takeAsync.get()) {
                System.out.println(" The async value of take is :: " + val);
            }

            intRDD.foreachAsync(record -> System.out.println("The val is :" + record));

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
