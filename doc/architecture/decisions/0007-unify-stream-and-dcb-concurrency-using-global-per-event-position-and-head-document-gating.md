# 7. Unify stream and DCB concurrency using global per event position and head document gating

Date: 2026-01-05

## Status

Accepted

## Context

Occurrent's MongoDB event store persists CloudEvents in a single collection. Historically, Occurrent used stream based semantics:

- Each event is associated with a `streamid`.
- A `streamversion` is stored on each event document.
- Conditional writes validate a stream version based `WriteCondition` by reading the latest stream version for the stream and then inserting new events.
- Concurrency safety is achieved via transactional writes and, critically, by uniqueness enforcement on stream version semantics (either explicitly through a unique index on `(streamId, streamVersion)` or implicitly through equivalent backstop mechanisms).

Dynamic Consistency Boundary (DCB) style preconditions require evaluation of whether any events matching a query have appeared after a monotonic sequence position. This requires a globally unique sequence position and a commit time enforcement mechanism. A naive approach of reading the latest matching event and then inserting events is not safe under concurrency because it allows write skew, where two concurrent writers observe the same pre state and both commit without conflicting.

Earlier considerations included implementing DCB by writing all DCB events to a single default streamId (for example `occurrent:dcb`) and relying on the existing streamVersion mechanism. This was rejected because it introduces unnecessary contention and serializes unrelated boundaries.

We also considered whether to remove streamVersion entirely and rely solely on global position and streamId plus position based reads. While possible, it would change semantics and requires explicit atomic gating for stream concurrency as well. Because DCB already requires an atomic gate, the system can be unified by adopting the same gating primitive for stream, DCB, and future custom approaches.

Occurrent already has a `count(streamId)` API. Therefore streamVersion does not need to be treated as an accurate stream size indicator and may contain gaps.

To preserve backward compatibility for consumers expecting a streamVersion, Occurrent may expose a streamVersion value even if the underlying storage uses a global position. This ADR decides to treat streamVersion as a compatibility field that may be derived from global position when reading.

CloudEvents constraints prevent representing tags as a list in CloudEvents context attributes. Therefore tags are persisted as indexed metadata in MongoDB, derived from domain events during CloudEvent creation or from CloudEvent extensions, and used for DCB query evaluation.

## Decision

1. Introduce a global per event sequence position `position` stored on each event document.
   - `position` is globally unique, monotonically increasing, and may contain gaps.
   - Allocate `position` in ranges per write batch using an atomic counter increment by `n` where `n` is the number of events to insert.
   - Assign each inserted event document a distinct `position`.

2. Unify conditional write enforcement for stream based writes, DCB writes, and custom approaches using a head document gating mechanism.
   - Maintain head documents keyed by a "consistency key" derived from the write condition.
   - A write condition resolves to one or more consistency keys.
   - Each consistency key maps to a head document containing at least `lastPosition`.

3. Maintain backward compatibility for streamVersion by allowing gaps and by deriving streamVersion from `position` during reads when needed.
   - StreamVersion is no longer treated as a contiguous per stream counter.
   - For compatibility, when returning events from a stream read, `streamversion` may be set equal to `position`.
   - Existing `count(streamId)` remains the supported mechanism for determining number of events in a stream.

4. Store and index tags in MongoDB as metadata suitable for DCB queries.
   - Tags are derived deterministically from domain events during CloudEvent creation using a user supplied function.
   - Tags are stored in MongoDB as a BSON array field and indexed using a multikey index.
   - If tags must travel with the CloudEvent, they may also be encoded as a single CloudEvents extension string, but MongoDB indexing uses the BSON array.

5. Support composable write conditions at the API level.
   - Introduce `StreamVersionCondition` and `DcbCondition` as variants of `WriteCondition`.
   - Support `AllOf` composition for cases where multiple preconditions must hold.
   - The datastore enforcement algorithm remains identical regardless of whether conditions represent stream based, DCB based, or custom keys.

## Consequences

Positive:
- A single concurrency mechanism enforces all conditional writes, eliminating special cases between stream and DCB.
- Correctness is improved by preventing write skew for DCB and any other query based preconditions.
- Global per event `position` enables DCB semantics and supports efficient ordered reads and resumption.
- Stream based reads remain supported and can order by `position`.
- StreamVersion remains available for backward compatibility without requiring contiguous numbering.
- Tag queries become efficient through MongoDB multikey indexes on the tag array.

Negative:
- Introduces head documents and conditional updates per write, increasing write amplification relative to a pure append model.
- Requires careful ordering of head updates to avoid deadlocks and a retry strategy for transient transaction failures.
- Adds additional indexes (`position`, tags) and corresponding storage overhead.
- StreamVersion no longer implies event count or contiguity, and callers must rely on `count(streamId)` for stream size.

## Algorithm

### Data model

Events collection documents:
- `ce` or equivalent fields representing the CloudEvent data as stored today.
- `position`: global sequence position, unique.
- `streamid`: the stream identifier.
- `streamversion`: compatibility field, may be set to `position` on read if desired.
- `tags`: array of strings, derived metadata used for DCB queries and indexing.
- other existing fields such as time, id, type, subject, etc.

Head documents (separate collection):
- `_id`: `"head:" + key`
- `lastPosition`: long

Counter document:
- `_id`: `"globalPosition"`
- `value`: long

Suggested indexes:
- unique `{ pos: 1 }`
- `{ streamId: 1, pos: 1 }` for ordered stream reads
- `{ tags: 1, pos: 1 }` or `{ tags: 1 }` plus `{ pos: 1 }` for DCB queries
- optional discriminator index if heads share the same collection

### Condition to consistency keys mapping

Define a function `keys(condition)` that returns a set of consistency keys:
- `StreamVersionCondition` uses one key, for example `"stream:" + streamId`.
  - This makes stream concurrency a special case of the same gating mechanism.
- `DcbCondition(query, afterPos)` maps to one key per tag in the query for common DCB usage.
  - For example `"tag:" + tagValue`.
  - If query includes type, include it in the key such as `"type:" + type + "|tag:" + tagValue"`.
- `AllOf` returns the union of keys of each subcondition.
- Custom approaches may define their own key mapping as long as they can be indexed and resolved deterministically for the write.

### Pseudo code

The write operation appends to a target streamId while enforcing arbitrary write conditions via head gating.

Inputs:
- `streamid`: target stream to which events are appended
- `condition`: composable `WriteCondition`
- `events`: list of CloudEvents to write

Auxiliary:
- `allocatePositions(n)` allocates a range `[start..end]` by atomically incrementing the counter by `n`.
- `afterPos(condition)` returns the maximum `after` constraint implied by the condition.
  - For DCB, this is `condition.afterPos`.
  - For stream based, this can be derived from the caller's provided token or treated as `null` if not used.
- `resolveKeys(streamId, condition)` returns sorted list of consistency keys.
- `updateHead(key, expectedAfter, newLastPos)` conditionally updates head doc.
- `insertEvents(docs)` inserts event documents.

Pseudo code:

```
write(streamId, condition, events):
  docs = materialize(events)
  if docs is empty:
    return success

  // allocate global positions once per batch
  (start, end) = allocatePositions(docs.size)
  for i in 0..docs.size-1:
    docs[i].pos = start + i
    docs[i].streamId = streamId
    // streamVersion is stored for compatibility but not treated as contiguous
    docs[i].streamVersion = docs[i].pos
    docs[i].tags = deriveTags(docs[i])  // derived and stored as array for indexing

  keys = resolveKeys(streamId, condition)  // deterministic order
  expectedAfter = extractAfter(condition)  // may be null

  beginTransaction()

    // head gating
    for key in keys:
      headId = "head:" + key

      // ensure head exists, and enforce precondition:
      // - if expectedAfter is null, treat as unconstrained and always advance head
      // - otherwise only advance head if head.lastPosition <= expectedAfter
      matched = updateOne(
        filter = { _id: headId, kind: "head", lastPosition: (expectedAfter == null ? any : { $lte: expectedAfter }) },
        update = { $set: { lastPosition: end }, $setOnInsert: { kind: "head" } },
        upsert = true
      )
      if matched == 0:
        abortTransaction()
        throw WriteConditionNotFulfilledException

    // persist events
    insertMany(docs)

  commitTransaction()

  return WriteResult(streamId, oldToken, newToken)
```

Notes:
- `oldToken` and `newToken` should be defined consistently for each API.
  - For stream operations, use the latest observed `position` as the token.
  - For DCB reads, use the global high water mark `position`.
- Deadlock avoidance:
  - Always update head docs in sorted key order.
- Retry strategy:
  - On transient transaction errors, retry the whole transaction.
  - Position allocation may occur outside the transaction, allowing gaps when retries occur.

### Read semantics

Stream read:
- Query by `streamid` ordered by `position` ascending.
- For compatibility, return `streamVersion = pos` if needed.

DCB read:
- Query by tags and or type combined with `position` ordering and optional `after` lower bound.
- Return a token based on the global high water mark `position` that can be used as `after` in subsequent DCB writes.

### Rationale for streamVersion behavior

- StreamVersion is retained for compatibility, but it is not treated as contiguous and may contain gaps.
- Accurate stream sizing is provided by the existing `count(streamId)` method.
- Using `position` as streamVersion aligns the concurrency token concept across stream, DCB, and custom approaches, and avoids needing a separate per stream counter if unification is desired.