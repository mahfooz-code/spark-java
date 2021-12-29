cd $SPARK_HOME 
./bin/spark-shell \
    --master yarn \
    --deploy-mode client

cd $SPARK_HOME 
./bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    examples/jars/spark-examples_2.11-2.1.1.jar


#-num-executors: Spark on YARN cluster allows users to define the number of executors for the application.
#This option is not available yet in Spark standalone

#--queue: This option allows users to provide a queue name of YARN for submitting the application.
