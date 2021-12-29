cd $SPARK_HOME
./bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --class org.apache.spark.examples.SparkPi \
    examples/jars/spark-examples_2.11-2.1.1.jar
