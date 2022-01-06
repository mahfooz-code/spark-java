package com.mahfooz.spark.streaming.sink.foreach;

import org.apache.spark.sql.ForeachWriter;

public class CustomForeachWriter extends ForeachWriter<String> {

    @Override
    public boolean open(long partitionId, long version) {
        // Open connection
        return true;
    }

    @Override
    public void process(String record) {
        // Write string to connection
    }

    @Override
    public void close(Throwable errorOrNull) {
        // Close the connection
    }
}