# 3. Add StreamId to CloudEvent when Writing to EventStore

Date: 2020-07-16

## Status

Accepted

## Context

Previously my thoughts regarding a "streamid" in the `CloudEvent` has been that since it's not visible to the user.
The "streamid" extension property was added on write and removed on read to avoid surprises to the user. 
However this leads to problems implementing snapshotting and sagas since then it's highly likely that we want to use the streamId 
in these cases (and the user probably wants to know the stream id in a saga).

## Decision

For this reason the implementors of the `WriteEventStream` api will _add_ a "streamid" to each `CloudEvent`.   

## Consequences

This leads to both negative and positive consequences.

Negative:
* Users has to either set the `streamid` extension property manually for each `CloudEvent` _or_ Occurrent will add it. 
  When Occurrent adds it automatically it may seem like "magic" from the part of the user.
* Depending on the `WriteEventStream` implementation we to transform `Stream<CloudEvent>` supplied by the user to `Stream<CloudEvent>` with a 
  `streamid` extension property. Could be a bit of an effort depending on the version of the `CloudEvent` (although we could limit the support for version 1 only).

Positive:
* Implementations of snapshotting and sagas and even custom subscriptions will be simpler.

### Alternatives

Alternatives would be to wrap the `CloudEvent` and add the `streamid`, and possibly other stuff, before writing to the database.
But there's something appealing about having plain `CloudEvent` representations in the DB and everywhere else. No need for additional envelopes etc.
So for now this will be the approach going forward.  

