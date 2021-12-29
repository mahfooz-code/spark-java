/*

HADOOP_HOME is needed.
System.setProperty("hadoop.home.dir", "PATH_OF_ HADOOP_HOME");

*/
package com.mahfooz.spark.conf;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class JavaSparkConfig {

    public static void main(String[] args) {
        // Construct a Spark conf
        SparkConf conf = new SparkConf();
        conf.set("spark.app.name", "AnotherFoobar App");
        conf.set("spark.master", "local[*]");
        conf.set("spark.ui.port", "37777");

        try (// InstantiateSparkContext with the Spark conf
                JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            JavaRDD<String> inputData = javaSparkContext
                    .textFile("C:/Users/malam/development/data/spark/site-traffic.txt");

            JavaPairRDD<String, Integer> flattenPairs = inputData
                    .flatMapToPair(text -> Arrays.asList(text.split(" ")).stream()
                            .map(record -> new Tuple2<String, Integer>(record, 1)).iterator());

            JavaPairRDD<String, Integer> wordCountRDD = flattenPairs
                    .reduceByKey((x, y) -> x + y);

            wordCountRDD.saveAsTextFile("C:/Users/malam/development/data/spark/output/site-traffic.txt");
        }
    }

}
