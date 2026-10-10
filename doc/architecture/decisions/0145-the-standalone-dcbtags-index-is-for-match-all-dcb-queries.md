# 145. The standalone dcbTags index is for match-all DCB queries

Date: 2026-10-10

## Status

Accepted. Resolves [#1233](https://github.com/johanhaleby/occurrent/issues/1233). Applies to `MongoEventStore`,
`SpringMongoEventStore` and `ReactorMongoEventStore`, and changes which stores get the `dcbTags` index from
[ADR 49](0049-dcb-reads-exclude-non-dcb-stream-events.md).

## Context

With the `DCB` capability enabled, the three MongoDB event stores create these indexes next to the `position` index,
all of them sparse. A sparse index only has entries for documents that have at least one of the index's fields.

| Index | Added in |
|---|---|
| `dcbTags` | [ADR 49](0049-dcb-reads-exclude-non-dcb-stream-events.md) |
| `(type, position)` | #303 |
| `(dcbTags, position)` | #303 |

ADR 49 made every DCB query include `dcbTags $exists: true`, so a DCB read never returns a stream event. For a read,
`count` or `exists` with `DcbCriteria.all()`, and for the append check of `DcbAppendCondition.wholeStoreLock()`, that
predicate and the position range are the whole filter. `wholeStoreLock(consistencyToken)` compares marker versions
instead and doesn't query the events. ADR 49 made the `dcbTags` index sparse for exactly that
predicate, back when it was the only DCB index.

#303 added the two compound indexes later, with its reasoning in the commit message and the code comments only. Since
`dcbTags` is the first field of `(dcbTags, position)`, the standalone index looked redundant, and the documentation
described it as the index for tag queries, which is the job `(dcbTags, position)` took over. ADR 49's warning against a
compound index is about a non-sparse `{position, dcbTags}`, so it didn't settle the question for the sparse
`(dcbTags, position)` either.

## Decision

The stores create the `dcbTags` index only when the store has both `STREAM` and `DCB`. They create `(type, position)`
and `(dcbTags, position)` whenever `DCB` is enabled, as before. Each index is for a different query.

| Index | Created when the store has | The query it's for |
|---|---|---|
| `dcbTags` | `STREAM` and `DCB` | A match-all query on a store where stream events also have a `position`. Only DCB events have a `dcbTags` field, so this index holds DCB events and nothing else, and the query reads only those. |
| `(type, position)` | `DCB` | A type-only criteria, read in position order. |
| `(dcbTags, position)` | `DCB` | A tag criteria, read in position order. |

`(dcbTags, position)` can't do the job of `dcbTags` for a match-all query. A stream event that has a `position` has
one of the two fields, so the sparse compound index has an entry for it, with a null `dcbTags`. MongoDB gives
`$exists: true` the bounds `[MinKey, MaxKey]` on `dcbTags` in both indexes. In the standalone index that range only
contains DCB events. In the compound index it also contains every positioned stream event, and MongoDB has to fetch
each of them to find out that it has no `dcbTags`.

On a DCB-only store whose collection holds nothing but DCB events, the `dcbTags` index holds the same events as the
`position` index and narrows nothing. Every DCB query I measured there examined the same keys and documents with and without `dcbTags`. The match-all
queries used `position`, and the tag and type queries used the compound indexes.
The index still has to be written on every append and kept on disk and in memory, so a DCB-only store doesn't get it.

### What I measured

I ran `explain("executionStats")` on MongoDB 8.0.29 and 4.2.8, the version ADR 49 tested on. The filters are what
the native store's own `toDcbBsonQuery` returns, sent with the same find, sort, limit and `countDocuments` calls the
stores make. To measure without the standalone index I hid it with `collMod` on 8.0. Hidden indexes need MongoDB 4.4, so on
4.2.8 I dropped it instead.

There were two stores, each written through `MongoEventStore`:

- A mixed store with `STREAM` and `DCB`: 200,000 stream events written with `write(..)`, with one DCB event appended
  after every 1,000 of them, so 200 DCB events in all. All 200,200 events have a `position`.
- A DCB-only store with 200,000 DCB events.

The numbers below were the same on both versions. Each cell is keys examined, documents examined, and the index the
planner picked.

Mixed store:

| Query | With `dcbTags` | Without `dcbTags` |
|---|---|---|
| `read(DcbCriteria.all())`, 200 returned | 400, 200, `dcbTags` plus an in-memory sort | 200,200, 200,200, `position` |
| `count(DcbCriteria.all())` and the native store's `exists(..)` | 400, 200, `dcbTags` | 200,200, 200,200, `position` |
| `find(..).limit(1)`, which `wholeStoreLock()` and the Spring and reactor `exists(..)` send | 1, 1, `dcbTags` | 1,001, 1,001, `position` |
| `read(DcbCriteria.all())` after a position 1,000 below the head, 1 returned | 400, 200, `dcbTags` plus an in-memory sort | 1,000, 1,000, `position` |
| `tags(course:7)`, 10 returned | 10, 10, `(dcbTags, position)` | the same |
| `types(CourseRenamed)`, 50 returned | 50, 50, `(type, position)` | the same |
| `read(DcbCriteria.all())` forced onto `(dcbTags, position)` with a hint | 200,400, 200,200, in-memory sort | the same |

DCB-only store:

| Query | With `dcbTags` | Without `dcbTags` |
|---|---|---|
| `read(DcbCriteria.all())`, 200,000 returned | 200,000, 200,000, `position` | the same |
| `count(DcbCriteria.all())` and the native store's `exists(..)` | 200,000, 200,000, `position` | the same |
| `find(..).limit(1)` | 1, 1, `position` | the same |
| `read(DcbCriteria.all())` after a position 1,000 below the head | 1,000, 1,000, `position` | the same |
| `tags(course:7)`, 1,000 returned | 1,000, 1,000, `(dcbTags, position)` | the same |
| `types(CourseRenamed)`, 200 returned | 200, 200, `(type, position)` | the same |

On the mixed store the planner picks the standalone index for every match-all query, and a read sorts the matching
DCB events in memory instead of walking the `position` index. On the DCB-only store the standalone index holds the
same documents as the `position` index, so it narrows nothing. There the planner picks `position` for every match-all
query, because it gives position order without a sort.

The 1,001 for `limit(1)` without the index is the number of stream events before the first DCB event in this data. I
didn't measure a store where DCB was enabled after years of stream events, but in that case every stream event comes
before the first DCB event, so without the `dcbTags` index the append check of `wholeStoreLock()` would read every
stream event that has a `position`.

### What the index costs on a DCB-only store

On the DCB-only store above, where every event has 2 tags, `collStats` on MongoDB 8.0 reported 7.9 MB for `dcbTags`
out of 53.6 MB for all of the collection's indexes, about 15%. That's more than the 7.5 MB of the `position` index.
The size grows with the number of events, so 10 million such events need about 400 MB for `dcbTags` alone, and
WiredTiger keeps it in the same cache as the indexes the queries use.

Each append of an event with 2 tags writes 9 index keys, and 2 of them go to `dcbTags`. I counted that from the index
list and didn't measure what it does to append latency.

## Consequences

The documentation's reason for the `dcbTags` index is wrong and is corrected to the match-all reason above. It also
says that only a store with both `STREAM` and `DCB` gets the index, and that a store needs it as soon as it has both.

The stores pick their indexes from the configured capabilities, not from what the collection holds, and Occurrent
never drops an index. Four cases follow from that.

- **A DCB-only store created on 0.30.0 to 0.33.x.** It already has `dcbTags` and keeps it. An operator can drop it by
  hand, and the next startup doesn't create it again. Section 27 of the
  [0.34.0 upgrade guide](../../migration/upgrading-to-0.34.0.md) says when that's safe.
- **A DCB-only store whose collection holds stream events that have a `position`.** That happens when a store that had
  `STREAM` is configured with `DCB` alone, or when an operator drops the index from such a store. A store that ever
  ran with both capabilities built `dcbTags` and keeps it. A collection that got its stream events from a `STREAM`
  store that never had `DCB` doesn't have it, and the DCB-only store doesn't create it. Without it, a match-all query
  and the append check of `wholeStoreLock()` read every stream event with a `position` in the range, 200,200
  documents instead of 200 in the mixed store above. The results are correct, only slower. The operator creates the
  index by hand in that case.
- **A DCB-only store that later enables `STREAM`.** Startup builds `dcbTags` over the whole collection, unless it's
  already there, and the store doesn't start until MongoDB has built it. On a large collection the operator builds it
  first, with the same key and options, as a rolling build the way step 1 of the
  [position backfill runbook](../../runbooks/position-backfill.md) builds `position`. The documentation and the
  upgrade guide both say so.
- **A `STREAM` store that enables `DCB`.** Startup builds all three DCB indexes, as before.

On the mixed store, a match-all read with the standalone index sorts every DCB event in the range in memory, even a
catch-up read near the head that returns one event. With 200 DCB events that's 400 keys against 1,000 for the
`position` index. I didn't measure where the planner switches to `position` as the number of DCB events grows. MongoDB
4.2 fails a sort that needs more than 32 MB, which I saw when I forced the 200,000 DCB-only events onto
`(dcbTags, position)` with a hint. The planner never chose a plan like that on its own in these runs.
