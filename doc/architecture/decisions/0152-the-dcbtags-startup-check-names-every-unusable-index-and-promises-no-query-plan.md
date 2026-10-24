# 152. The dcbTags startup check names every unusable index and promises no query plan

Date: 2026-10-24

## Status

Accepted. Follows up the startup check of [ADR 145](0145-the-standalone-dcbtags-index-is-for-match-all-dcb-queries.md)
in [#1236](https://github.com/johanhaleby/occurrent/issues/1236) and amends it on the four points under Decision.
Applies to `MongoEventStore`, `SpringMongoEventStore` and `ReactorMongoEventStore`.

## Context

ADR 145 and 0.34.0 added a startup warning for a MongoDB event store with `DCB` and without `STREAM`. It fires when the
collection holds a stream event with a numeric `position` and has no usable index on `dcbTags` alone. Reviewing it for
0.35.0 turned up three defects in the released check and one claim that is too strong.

- **The usable-index test is too narrow.** MongoDB returns a `partialFilterExpression` as it was given. The filter
  `{ $and: [ { dcbTags: { $exists: true } } ] }` holds the same documents as `{ dcbTags: { $exists: true } }`, and the
  released check called an index with it unusable.
- **The warning names one index.** A collection can hold several indexes on `dcbTags` alone, for example a partial
  one named `dcbTags_1` and a sparse one with another name. The released warning named the first, and its command
  fixed that one only. The fix could leave the collection with no usable index, and a `createIndex` for `dcbTags_1`
  fails with `IndexKeySpecsConflict` while another index still has that name.
- **The check reads in the wrong order.** The released stores called `listIndexes` first and sent the find for a
  positioned stream event only when the indexes had no usable one. A role that can read the collection but can't list
  its indexes got a failed check on every start, even on a collection that holds no stream event.
- **The warning says too much.** ADR 145 says that the match-all queries use the index, in the Decision table row
  for `dcbTags` and in its summary of the measurements. The warning said that a read, `count` or `exists` with
  `DcbCriteria.all()` and the append check of `DcbAppendCondition.wholeStoreLock()` read the DCB events without the
  stream events. That holds for some of them and not for the others.

### What I measured

I ran `explain("executionStats")` on MongoDB 8.0.34 on a collection with 50,000 stream events that have a `position`
and 1,000 DCB events with two tags each, in two layouts. In `interleaved` one DCB event follows every 50 stream events.
In `streamfirst` all the stream events come first. I compared a collection without an index on `dcbTags` alone with one
that has the sparse `dcbTags_1`. The filters have the shape the native store sends, with the same sorts and limits.

| Read | `interleaved`, with the index | `streamfirst`, with the index | Without the index |
|---|---|---|---|
| `count` | `dcbTags_1`, 2,000 keys, 1,000 documents | the same | `position_1`, 51,000 keys |
| `exists` and the `wholeStoreLock()` find, `limit(1)` | `dcbTags_1`, 1 key | the same | `position_1`, 51 keys, or 50,001 in `streamfirst` |
| `read(DcbCriteria.all())` forward | `dcbTags_1` plus an in-memory sort, 2,000 keys | the same | `position_1`, 51,000 keys |
| the same read backward | `dcbTags_1` plus an in-memory sort, 2,000 keys | `position_1`, 51,000 keys | `position_1`, 51,000 keys |
| the same read, limit 10, forward | `position_1`, 510 keys | `dcbTags_1` plus an in-memory sort, 2,000 keys | `position_1`, 510 keys, or 50,010 in `streamfirst` |
| the same read, limit 10, backward | `position_1`, 460 keys | `position_1`, 10 keys | the same |

So `count`, `exists` and the `wholeStoreLock()` find used the index in every run, and a read used it only some of the
time. In `streamfirst` the unbounded backward read walks all 51,000 keys of `position_1` with the index in place.
The planner's choice depends on the data, so the runs in ADR 145, on 200,000 stream events with one DCB event after every
1,000 of them, say little about other stores.

On 8.0.34, 7.0.45 and 4.2.8 the planner uses an index whose filter is an `$and` of one copy of the predicate, or
of two copies, exactly as it uses one with the plain filter. A nested `$and` inside the `$and` is used on 8.0.34 and
7.0.45 and ignored on 4.2.8.

## Decision

The check changes on these four points. Nothing else in ADR 145 changes.

1. **The usable-index test accepts the `$and` form.** An index on `dcbTags` alone is usable when it isn't hidden and
   is either sparse without a `partialFilterExpression`, or has the `partialFilterExpression`
   `{ dcbTags: { $exists: true } }`, or an `$and` of one or more copies of it. A nested `$and`, an `$and` with another
   condition and an empty `$and` stay unusable. The nested form stays out because 4.2.8 ignores it, so the check gives
   the same answer on every version I tested.
2. **The find runs first and `listIndexes` second.** The hinted and bounded find of ADR 145 runs first. The store lists
   the indexes only when that find returns a stream event, which is the condition ADR 145 already set for the warning.
   A role without `listIndexes` on a collection that holds no stream event never sends it, and the store logs nothing
   for that collection. The store constructors still need `listCollections` on the database, as before.
3. **The warning says which reads the index helps and promises no plan.** `count`, `exists` and the append check of
   `wholeStoreLock()` read only DCB events with the index, and a read with `DcbCriteria.all()` reads only DCB events
   when MongoDB uses the index. The text says it may not for a backward read or a read with a limit, which can still
   walk `position_1`. The two places in ADR 145 that say the match-all queries use the index describe what the
   planner did in its runs, not something the stores guarantee.
4. **The warning names every unusable index, and its commands leave exactly one usable index.** When the collection
   has indexes on `dcbTags` alone and none is usable, the warning names each one with its reasons. If one of them
   holds only DCB events and is hidden, the commands unhide that one with `collMod` and drop the others. Otherwise
   they drop every one of them in the order listed and then create the sparse `dcbTags_1`. The text says that
   dropping removes an index, so the operator checks first that nothing else uses it.

Nothing persisted changes. The stores create the same indexes with the same options as before and drop none, and no
event or document is written differently. The change is the text of one log line, the order of two reads at startup, and which index options
the check accepts.

## Consequences

An operator who built the `$and` form on purpose no longer gets a warning for it. An operator who sees the new warning
runs the commands in the order it prints and ends with one index on `dcbTags` alone. A store started with a role that
lacks `listIndexes` stays quiet on a collection without stream events and logs the check failure on one with them.

The warning no longer tells an operator that creating the index fixes every match-all read. The reads it can't fix,
the backward and limited ones in a store with a long stream history, stay slow, and the warning says so.

## Alternatives considered

**A partial index on `dcbTags` and `position`.** It doesn't serve the backward and limited reads that still walk
`position_1`. `dcbTags` leads the key, so every read sorted by `position` still needs the in-memory sort or
`position_1`. On the same data with a partial compound index added, the reads in the table chose the same indexes
as before, and an `$exists: true` filter on the partial index doesn't change that.

**A partial index on `position` alone.** With the `partialFilterExpression` `{ dcbTags: { $exists: true } }` it holds
only DCB events and costs one entry per DCB event, not one per tag. On the same data it served the forward and limited
reads and the `exists` find with 1,000 keys or fewer, in both layouts, and `count` read 1,000 keys without fetching a
document. The unbounded backward read in `streamfirst` still chose `position_1` with 51,000 keys, so it isn't a
complete fix. Whether the stores should create it is open in
[#1303](https://github.com/johanhaleby/occurrent/issues/1303). This ADR doesn't depend on the answer.

**Hint the standalone index on every match-all read.** The stores could add a hint for the standalone index to each of
them. A hint makes the read fail when the index is missing or hidden, and a store without the index is
exactly the one the warning is for, so it would fail where it now reads slowly. A hinted limited backward read in
`streamfirst` would also need the in-memory sort the forward limited read has there, 2,000 keys instead of 10. I
didn't measure hinted reads, so I left the reads alone.
