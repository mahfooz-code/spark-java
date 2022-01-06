/*
cleanSource:
option to clean up completed files after processing.
Available options are "archive", "delete", "off".
If the option is not provided, the default value is "off".

When "archive" is provided, additional option sourceArchiveDir must be provided as well.
Number of threads used in completed file cleaner can be configured with
    spark.sql.streaming.fileSource.cleaner.numThreads (default: 1).

 */
package com.mahfooz.spark.streaming.source.file;

public class SparkFileSourceStreamCleanup {
    public static void main(String[] args) {

    }
}
