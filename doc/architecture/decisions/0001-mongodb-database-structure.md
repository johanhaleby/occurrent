# 1. MongoDB Database Structure.

Date: 2020-07-12

## Status

Accepted

## Context

We need to record events in MongoDB in a specific structure/schema.

## Decision

The [CloudEvents](https://cloudevents.io/) are persisted like this in the "events collection" in the database (1):

```json
{
  "specversion": "1.0",
  "id": "86282094-5344-4309-932a-129a7774735e",
  "source": "http://name",
  "type": "org.occurrent.domain.NameDefined",
  "datacontenttype": "application/json",
  "dataschema" : "http://someschema.com/schema.json",
  "subject": "name1",
  "time": "2020-07-10T14:48:23.272Z",
  "data": {
    "timestamp": 1594392503272,
    "name": "name1"
  },
  "streamid" : "streamid"
}
```

Note that "streamid" is added as an extension by the MongoDB event stores in order to read all events for a particular stream.

If stream consistency is enabled then another collection, the "stream consistency" collection is also written to the database (2):

```json
{
  "_id" : "streamid",
  "version" : 1
}
```

When appending cloud events to the stream the consistency of the stream is maintained by comparing the version supplied by the user 
with the version present in (2). If they don't match then the cloud events are not written. Also if there are two threads writing to the same 
stream at once then one of them will run into an error which means it has to retry (optimistic locking). For this to work, transactions are required! 

Another previous approach was instead to store the events like this:

```json
{
  "_id": "streamid",
  "version" : 1,
  "events": [{
    "specversion": "1.0",
    "id": "86282094-5344-4309-932a-129a7774735e",
    "source": "http://name",
    "type": "org.occurrent.domain.NameDefined",
    "datacontenttype": "application/json",
    "subject": "name1",
    "time": "2020-07-10T14:48:23.272Z",
    "data": {
      "timestamp": 1594392503272,
      "name": "name1"
    }
  }]
}
``` 

I.e. the events were stored inside a single document. While there are several benefits of using this approach, such as:

1. No transactions required, just do;
    ```java
    eventCollection.updateOne(and(eq("_id", streamId), eq("version", expectedStreamVersion)),
                    combine(pushEach("events", serializedEvents), set("version", expectedStreamVersion + 1)),
                    new UpdateOptions().upsert(true));
    ``` 
1. Reads could be done in a streaming fashion even though the events were stored as a subarray using aggregations
1. Subscriptions could take a `List<CloudEvent>`, i.e. all events written in the same transaction to the event store. 
   When not using the approach subscriptions gets notified once for each event and the consumer needs to reassemble 
   the "transaction" somehow. This is a major drawback when not using this approach.  
 
There are however two major drawbacks that lead to not using this approach:

1. There's 16Mb document size limit in MongoDB so this approach wouldn't work for large streams
1. It's much hard to implement queries/filters for subscriptions. The aggregation support is 
   [limited](https://stackoverflow.com/questions/62846085/remove-element-from-subarray-using-an-aggregation-stage-applied-to-a-change-stre)
   when working with subscriptions preventing simple filters (it would have been much simpler if `unwind`
   was supported since then we could flatten out the `events` subarray before applying the queries, i.e. something like
   `(unwind("$events"), replaceRoot("$events"), match(filter.apply("type", item))`).
   Another problem with subscriptions is the format, when a document is _created_ the content is specified 
   in the `fullDocument` property but it's a different property when the document is updated. Thus a filter/query
   would not need to consider both these cases which is very difficult. With the new approach a query/filter is much
   easier since we only need to care about inserts.    

## Consequences

Unfortunately the implementation now must make use of transactions for consistency. If consistency is not required by the application
this can be turned-off by using `StreamConsistencyGuarantee.none()` and if so no transactions are required. However this mode will NOT store 
a stream version.  
