cd $SPARK_HOME 
./bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    examples/jars/spark-examples_2.11-2.1.1.jar 


#Provide external jar files that the Spark application is referring.
./bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /path/of/external/lib \
    --deploy-mode cluster \
    --class com.Spark.ApplicationMain  \
    /path/to/application/jar 

#Driver memory
./bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 1G \
    --deploy-mode cluster \
    --class com.Spark.ApplicationMain  \
    /path/to/application/jar 

# Executor memory
./bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 1G \
    --deploy-mode cluster \
    --class com.Spark.ApplicationMain  \
    /path/to/application/jar 

#Specify the total number of cores needed for an application.
./bin/spark-submit \
    --master spark://spark-master:7077 \
    --total-executor-cores 10 \
    --deploy-mode cluster \
    --class com.Spark.ApplicationMain  \
    /path/to/application/jar 

# Restart driver if it dies
#When supervise configuration is provided, Spark master will supervise Spark Driver and if the Driver dies, 
#it restarts the driver.

./bin/spark-submit \
    --master spark://spark-master:7077 \
    --supervise \
    --deploy-mode cluster \
    --class com.Spark.ApplicationMain  \
    /path/to/application/jar 




