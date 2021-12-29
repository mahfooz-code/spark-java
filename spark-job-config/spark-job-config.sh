bin/spark-submit \
    --class com.foo.Bar \
    --master local[3] \
    --name "AnotherFoobar App" \
    --confspark.ui.port=37777\
    fooBar.jar


bin/spark-submit \
    --class com.foo.Bar \
    --properties-file job-config.conf \
fooBar.jar


