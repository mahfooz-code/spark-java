/*

The "Output" is defined as what gets written out to the external storage.
The output can be defined in a different mode:

Complete Mode

    The entire updated Result Table will be written to the external storage.
    It is up to the storage connector to decide how to handle writing of the entire table.

Append Mode
    Only the new rows appended in the Result Table since the last trigger will be written to the external storage.
    This is applicable only on the queries where existing rows in the Result Table are not expected to change.

Update Mode
    Only the rows that were updated in the Result Table since the last trigger will be written to the external storage
    Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since the
    last trigger.
    If the query doesn't contain aggregations, it will be equivalent to Append mode.



Queries with mapGroupsWithState
Queries with joins              Append
Other queries                   Append, Update

 */
package com.mahfooz.spark.streaming.mode;

public class SparkStreamingMode {
    public static void main(String[] args) {

    }
}
