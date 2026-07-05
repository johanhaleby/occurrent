# 49. DCB reads exclude non-DCB stream events

Date: 2026-07-05

## Status

Accepted

## Context

[ADR 17](0017-introduce-dcb-as-shared-cloudevent-capability.md) and [ADR 18](0018-spring-mongo-event-store-capabilities.md)
state that on a store with both `STREAM` and `DCB` capabilities enabled, stream-written events do not participate in
DCB tag queries: DCB reads will not return them unless backfilled. That invariant was not actually enforced.
`DcbCriteria.all()` and type-only `DcbCriterion` queries (queries with no tag constraint) matched any event whose type
and tags happened to satisfy the criterion, regardless of whether the event was ever DCB-tagged. A stream event that
happened to carry no `dcbTags` still matched `DcbCriteria.all()`, because "match everything" is exactly what that
criterion asks for once the query layer stops distinguishing DCB events from stream events.

This was pre-existing in every store (in-memory, Mongo native, Spring Mongo blocking, Spring Mongo reactor). It surfaced
via a Copilot review comment on an unrelated PR (#276) and was filed as issue #279.

## Decision

**Correctness fix.** An unconditional guard, not expressible through `DcbCriteria`, was added at the query/read-building
layer of every store:

- **In-memory** (`InMemoryEventStore`): each of the three call sites that evaluate `DcbCloudEvents.matches(event, ...)`
  (the `read(DcbCriteria, DcbReadOptions)` filter, the `matchingDcbEvents` method backing `exists`/`count`, and the
  append-condition conflict check in `appendDcb`) now ANDs `DcbCloudEvents.isDcbEvent(event)` in front of the `matches`
  call.
- **Mongo stores** (native, Spring blocking, Spring reactor): the private method that builds the Mongo
  query/filter/criteria from a `DcbCriteria` (`toDcbBsonQuery` / `toDcbMongoQuery`) now ANDs an existence check on the
  `dcbTags` field (`DcbDocumentMapper.DCB_TAGS_INDEX_FIELD`) into both the `DcbCriteria.MatchAll` early return and the
  general (item-OR) case. `read`, `exists`, and `count` all call this same method in every store, as does the DCB
  append-condition conflict check, so this is a single-point fix per store.

`DcbCriteria`, `DcbCriterion`, and `DcbCloudEvents.matches()` are deliberately **unchanged**. `matches()` is a
documented pure predicate over type, tags, and excluded types, used and tested directly elsewhere (`DcbApiTest`), and
other callers already pair it manually with `isDcbEvent`. The "is this a DCB event" guard is not something a caller
can or should express through the criteria model; it is not user-specifiable, so it does not belong there. It belongs
exactly where the read/exists/count/conflict-check machinery turns a `DcbCriteria` into an actual query.

**Index fix.** The `dcbTags` index in all three Mongo stores is now created as **sparse**
(`new IndexOptions().sparse(true)`) instead of a plain index.

This was verified empirically with `explain("executionStats")` against a skewed dataset: 200,000 stream documents
without a `dcbTags` field and 200 DCB documents with it.

- A plain, non-sparse index on `dcbTags` gives **zero** selectivity benefit for the new `$exists: true` predicate. A
  non-sparse index also indexes the implicit "missing" value for every document that lacks the field, so it cannot
  resolve `$exists: true` into tight index bounds. The query planner falls back to the `position` index and examines
  199,999 of 200,000 documents, the same cost as the pre-fix query.
- A sparse index only contains entries for documents where `dcbTags` exists, so `$exists: true` becomes a full index
  scan of just the DCB documents: 200 of 200,000 documents examined, sub-millisecond.
- A non-sparse **compound** `{position, dcbTags}` index was tried as an alternative and rejected: it does not help
  either. Same 199,999-of-200,000 examined result, for the same reason (the compound index still carries an entry for
  every document, missing-value included).

Verified on both `mongo:4.2.8` (this repository's test version) and `mongo:8.0`, with identical query-planner
behavior on both.

## Consequences

`DcbCriteria.all()` and type-only criteria now only ever return DCB-written events, matching the invariant stated in
ADR 17 and ADR 18, on every store.

DCB has never shipped: there is no deployed environment with an existing `dcbTags` index, so there is nothing to
migrate and no backward-compatibility concern. The sparse option is simply the index's original shape, not a
migration. A future reader should not expect (or add) a startup guard, migration script, or upgrade path for this
index change; none is needed because there was never a plain index in production to replace.

A future contributor should not "simplify" the two-index situation into a single compound `{position, dcbTags}` index
without re-reading this ADR. That was tested and found to give no selectivity benefit over the current plain
`position` index; the sparse single-field `dcbTags` index is the one that is actually selective for the `$exists`
predicate.
