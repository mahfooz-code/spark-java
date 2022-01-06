#   Output Modes
#   Append mode (default)

This is the default mode, where only the new rows added to the Result Table since the last trigger will be outputted to the sink.
This is supported for only those queries where rows added to the Result Table is never going to 
change. 
Hence, this mode guarantees that each row will be output only once (assuming fault-tolerant sink). 
For example, queries with only select, where, map, flatMap, filter, join, etc. will support 
Append mode.

1) map
2) flatmap
3) filter
4) join

#Complete mode

The whole Result Table will be outputted to the sink after every trigger.
This is supported for aggregation queries.

#Update mode 
Only the rows in the Result Table that were updated since the last trigger will be outputted 
to the sink.


#   Queries with aggregation

1) Aggregation on event-time with watermark
   
   1) Append
   2) Update
   3) Complete

    
    Append mode uses watermark to drop old aggregation state. 
    But the output of a windowed aggregation is delayed the late threshold specified in 
    withWatermark() as by the modes semantics, rows can be added to the Result Table only once 
    after they are finalized (i.e. after watermark is crossed).
    Update mode uses watermark to drop old aggregation state.

    Complete mode does not drop old aggregation state since by definition this mode preserves 
    all data in the Result Table.

   
2) Other aggregations
   
    
    Since no watermark is defined (only defined in other category), old aggregation state is not dropped.
    Append mode is not supported as aggregates can update thus violating the semantics of this mode.

   1) Complete
   2) Update


#   Queries with mapGroupsWithState

    Update
    Aggregations not allowed in a query with mapGroupsWithState.

#   Queries with flatMapGroupsWithState
    
1) Append operation mode	
   1) Append	
   2) Aggregations are allowed after flatMapGroupsWithState.
2) Update operation mode	
   1) Update	
   2) Aggregations not allowed in a query with flatMapGroupsWithState.

#   Queries with joins

1) Append	
2) Update and Complete mode not supported yet.

# Sink

# For Each Sink

Append, Update, Complete
Yes (at-least-once)


