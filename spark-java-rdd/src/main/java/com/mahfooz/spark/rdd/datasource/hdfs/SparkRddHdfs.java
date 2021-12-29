/*

JavaRDD<String> hadoopFile= jsc.textFile("hdfs://"+"namenode-host:port/"+"absolute-file-path-on-hdfs"); 
JavaRDD<String> hadoopFile= jsc.textFile("hdfs://localhost:8020/user/spark/data.csv") 
JavaPairRDD<String, Integer> hadoopWordCount=hadoopFile.flatMap(x -> Arrays.asList(x.split(", ")).iterator()) 
               .mapToPair(x -> new Tuple2<String, Integer>((String) x, 1)) 
               .reduceByKey((x, y) -> x + y); 
               
*/
package com.mahfooz.spark.rdd.datasource.hdfs;

public class SparkRddHdfs {

}
