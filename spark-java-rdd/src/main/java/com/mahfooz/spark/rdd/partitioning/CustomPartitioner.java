package com.mahfooz.spark.rdd.partitioning;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {

    final int maxPartitions = 2;

    @Override
    public int getPartition(Object key) {
        return (((String) key).length() % maxPartitions);
    }

    @Override
    public int numPartitions() {
        return maxPartitions;
    }
}