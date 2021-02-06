# 6. MongoDB stream version

Date: 2020-08-15

## Status

Accepted

## Context

When doing a code review with [Per Ökvist](https://github.com/perokvist) he pointed out that the stream consistency 
was not implemented the way it normally seems to be in most event stores. Typically, each event would contain a version. 
While this is probably not crucial for persisting the event (and maintaining consistency) a unique version or sequence number per 
event can be really beneficial when doing projections. For example if you want to create an integration event when event X is 
received then you may way to read all events less than the version number of X. This also allows for different strategies 
when it comes to waiting for all projections to update on write before returning to a client (using a lock that waits until
all projections have reached a certain version). A stream/sequence version might also be required when implementing with 
certain event stores/databases.      

## Decision

In light of this I've re-written the consistency guarantee to use compare the WriteCondition to the latest "stream version"
for a particular stream. Only if that version fulfills the WriteCondition the new events are written. This greatly simplifies
both the implementation, user configuration (no need for "StreamConsistency") and understandability. 

## Consequences

This means that a "streamversion" extension must be written to each CloudEvent. However, this is probably an OK trade-off 
since you can make good use of this as a user. 