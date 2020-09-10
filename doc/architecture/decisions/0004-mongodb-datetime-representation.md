# 4. MongoDB datetime representation

Date: 2020-07-28

## Status

Accepted

## Context

Representing RFC 3339 in MongoDB when using the CloudEvent Jackson SDK _and_ allow for queries is problematic. 
This is because MongoDB internally represents a Date with only millisecond resolution (see [here](https://docs.mongodb.com/manual/reference/method/Date/#behavior)):

> Internally, Date objects are stored as a signed 64-bit integer representing the number of milliseconds since the Unix epoch (Jan 1, 1970).

Since the Java CloudEvent SDK uses `OffsetDateTime` we'll lose nanosecond precision and the timezone if converting the `OffsetDateTime` 
to `Date`. This is really bad since it goes against the "understandability" and "transparent" goals of Occurrent. I.e. if you create
a CloudEvent with a OffsetDateTime containing nanoseconds in timezone "Europe/Stockholm" you would expect to get the same value back on read 
which is not possible if we convert it to a date. 

## Decision

We've thus decided to just store the `ZoneDateTime` as an RFC 3339 string in MongoDB. This means that range queries on "time" will be 
horrendously slow and probably not work as expected (string comparision instead of date comparision). The `EventStoreQueries` API
currently supports sorting events by natural order ascending/descending which will be much faster (since it's using the timestamp of the 
generated mongodb object id).    

Note that sort options `TIME_ASC` and `TIME_DESC` are still retained in the API. The reason for this that we may allow customizations
to the serialization mechanism _if_ nanosecond resolution and is not required and timezone is always `UTC` in the future.  

## Alternatives

An alternative worth considering would be to add an additional field in the serialization process. For example retain the "time" as RFC 3339 string
but add an additional field in MongoDB that stores the `Date` so it can be used for fast "time" queries. 

I've decided not to do this though for the following reasons:

1. Code simplicity. We would have needed to handle "time" queries specially. For example if using "eq" we probably would like to compare "time" field 
   and if not using "eq" we want to compare if the `Date` field. Combinations such as "gte" becomes even more problematic.
1. For time queries to be fast an index would be needed. It would introduce additional complexity for users of the MongoDB EventStore that this index would need
   to be created for fast queries. Creating the index automatically would not be a good idea since it might not be required for every user.
1. User may never which to query for "time"! In these cases storing an extra field is simply unnecessary.  

For these reasons we've decided that it's better for the user to simply add a custom extension field him-/herself and create custom queries for this field.
The `EventStoreQueries` api event supports querying custom fields right now 
(though we could expand the API to allow custom `SortBy` fields instead of hardcoding "time" and "natural"). 

## Consequences

This decision is quite sad since it's still very common represent time in Java application as a `Date`. In these cases it would be perfectly 
fine to use the native `ISODate` in MongoDB. If converting a `Date` to a `OffsetDateTime` it would be possible to get the best of both worlds and just
store the `Date` in MongoDB and one of the nice benefits of using CloudEvents are lost. 