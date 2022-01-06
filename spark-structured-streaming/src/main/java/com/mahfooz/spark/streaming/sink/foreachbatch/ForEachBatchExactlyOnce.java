/*

By default, foreachBatch provides only at-least-once write guarantees.
However, you can use the batchId provided to the function as way to deduplicate the output and get an
exactly-once guarantee.
foreachBatch does not work with the continuous processing mode as it fundamentally relies on
the micro-batch execution of a streaming query.
If you write data in the continuous mode, use foreach instead.

 */
package com.mahfooz.spark.streaming.sink.foreachbatch;

public class ForEachBatchExactlyOnce {
}
