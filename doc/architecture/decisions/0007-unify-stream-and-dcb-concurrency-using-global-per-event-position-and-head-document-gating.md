# 7. Unify stream and DCB concurrency using global per event position and consistency checkpoints

Date: 2026-01-05

## Status

Rejected (see [ADR 8](0008-stream-first-eventstore-api-with-dcb-via-applicationservice-using-global-position-and-consistency-checkpoints.md))

## Context

Occurrent's MongoDB event store persists CloudEvents in a single collection. Historically, Occurrent used stream based semantics:

- Each event is associated with a `streamid`.
- A `streamversion` is stored on each event document.
- Conditional writes validate a stream version based `WriteCondition` by reading the latest stream version for the stream and then inserting new events.
- Concurrency safety is achieved via transactional writes and, critically, by uniqueness enforcement on stream version semantics (either explicitly through a unique index on `(streamId, streamVersion)` or implicitly through equivalent backstop mechanisms).

Dynamic Consistency Boundary (DCB) style preconditions require evaluation of whether any events matching a query have appeared after a monotonic sequence position. This requires a globally unique sequence position and a commit time enforcement mechanism. A naive approach of reading the latest matching event and then inserting events is not safe under concurrency because it allows write skew, where two concurrent writers observe the same pre state and both commit without conflicting.

Earlier considerations included implementing DCB by writing all DCB events to a single default streamId (for example `occurrent:dcb`) and relying on the existing streamVersion mechanism. This was rejected because it introduces unnecessary contention and serializes unrelated boundaries.

We also considered whether to remove streamVersion entirely and rely solely on global position and streamId plus position based reads. While possible, it would change semantics and requires explicit atomic enforcement for stream concurrency as well. Because DCB already requires an atomic enforcement mechanism, the system can be unified by adopting the same primitive for stream, DCB, and future custom approaches.

Occurrent already has a `count(streamId)` API. Therefore streamVersion does not need to be treated as an accurate stream size indicator and may contain gaps.

To preserve backward compatibility for consumers expecting a streamVersion, Occurrent may expose a streamVersion value even if the underlying storage uses a global position. This ADR decides to treat streamVersion as a compatibility field that may be derived from global position when reading.

CloudEvents constraints prevent representing tags as a list in CloudEvents context attributes. Therefore tags are persisted as indexed metadata in MongoDB, derived from domain events during CloudEvent creation or from CloudEvent extensions, and used for DCB query evaluation.

Finally, DCB is treated as an internal datastore mechanism, and callers may choose different query keys (for example tags, subject, streamid, or custom keys) as long as these resolve deterministically to the same consistency keys used for conditional write enforcement.

## Decision

1. Introduce a global per event sequence position `position` stored on each event document.
   - `position` is globally unique, monotonically increasing, and may contain gaps.
   - Allocate `position` in ranges per write batch using an atomic counter increment by `n` where `n` is the number of events to insert.
   - Assign each inserted event document a distinct `position`.

2. Unify conditional write enforcement for stream based writes, DCB writes, and custom approaches using consistency checkpoints.
   - Maintain checkpoint documents keyed by a "consistency key" derived from the write condition.
   - A write condition resolves to one or more consistency keys.
   - Each consistency key maps to a checkpoint document containing at least `lastPosition`.
   - The same mechanism is used for streamid, tags, subject, and any other supported query key. Streamid is not a special case.

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

6. Introduce a constructor supplied, composable `ConsistencyKeyExtractor` used by the datastore to derive consistency keys from `WriteCondition`.
   - The extractor is used only for determining which checkpoint documents to update as part of conditional writes.
   - The extractor is not used for reading or querying events.
   - The extractor returns a sorted, de duplicated set of keys to ensure deterministic checkpoint update ordering and to avoid deadlocks.
   - The extractor itself should be composable by unioning keys from multiple extractors and or condition variants.

## Consequences

Positive:
- A single concurrency mechanism enforces all conditional writes, eliminating special cases between stream and DCB.
- Correctness is improved by preventing write skew for DCB and any other query based preconditions.
- Global per event `position` enables DCB semantics and supports efficient ordered reads and resumption.
- Stream based reads remain supported and can order by `position`.
- StreamVersion remains available for backward compatibility without requiring contiguous numbering.
- Tag queries become efficient through MongoDB multikey indexes on the tag array.
- Streamid is treated the same as any other query key at the datastore enforcement layer.

Negative:
- Introduces checkpoint documents and conditional updates per write, increasing write amplification relative to a pure append model.
- Requires careful ordering of checkpoint updates to avoid deadlocks and a retry strategy for transient transaction failures.
- Adds additional indexes (`position`, tags) and corresponding storage overhead.
- StreamVersion no longer implies event count or contiguity, and callers must rely on `count(streamId)` for stream size.
- Additional components are introduced (for example `ConsistencyKeyExtractor`) that must remain deterministic and stable to ensure correct enforcement.

## Algorithm

### Data model

Events collection documents:
- `ce` or equivalent fields representing the CloudEvent data as stored today.
- `position`: global sequence position, unique.
- `streamid`: the stream identifier.
- `streamversion`: compatibility field, may be set to `position` on read if desired.
- `tags`: array of strings, derived metadata used for DCB queries and indexing.
- other existing fields such as time, id, type, subject, etc.

Consistency checkpoint documents (separate collection):
- `_id`: `"checkpoint:" + key`
- `lastPosition`: long

Counter document:
- `_id`: `"globalPosition"`
- `value`: long

Suggested indexes:
- unique `{ position: 1 }`
- `{ streamid: 1, position: 1 }` for ordered stream reads
- `{ tags: 1, position: 1 }` or `{ tags: 1 }` plus `{ position: 1 }` for DCB queries
- optional indexes for additional supported query keys (for example `{ subject: 1, position: 1 }`)

### Condition to consistency keys mapping

Define a function `keys(condition)` that returns a set of consistency keys. The datastore uses a `ConsistencyKeyExtractor` for this purpose.

Examples:
- `StreamVersionCondition` uses one key, for example `"stream:" + streamid` where `streamid` is taken from the CloudEvent extension attribute.
- `DcbCondition(query, afterPos)` maps to one key per boundary identifier in the query for common DCB usage.
  - For tags, for example `"tag:" + tagValue`.
  - For subject, for example `"subject:" + subjectValue`.
  - If query includes type, include it in the key such as `"type:" + type + "|tag:" + tagValue"`.
- `AllOf` returns the union of keys of each subcondition.
- Custom approaches may define their own key mapping as long as they can be indexed and resolved deterministically for the write.

### Pseudo code

The write operation appends to a target streamid while enforcing arbitrary write conditions via consistency checkpoints.

Inputs:
- `streamid`: target stream to which events are appended
- `condition`: composable `WriteCondition`
- `events`: list of CloudEvents to write

Auxiliary:
- `allocatePositions(n)` allocates a range `[start..end]` by atomically incrementing the counter by `n`.
- `extractAfter(condition)` returns the maximum `after` constraint implied by the condition.
  - For DCB, this is `condition.afterPos`.
  - For stream based, this can be derived from the caller's provided token or treated as `null` if not used.
- `consistencyKeyExtractor.extractKeys(condition)` returns a sorted set of consistency keys.
- `updateCheckpoint(key, expectedAfter, newLastPosition)` conditionally updates a checkpoint document.
- `insertEvents(docs)` inserts event documents.

Pseudo code:

```
write(streamid, condition, events):
  docs = materialize(events)
  if docs is empty:
    return success

  // allocate global positions once per batch
  (start, end) = allocatePositions(docs.size)
  for i in 0..docs.size-1:
    docs[i].position = start + i
    docs[i].streamid = streamid
    // streamversion is stored for compatibility but not treated as contiguous
    docs[i].streamversion = docs[i].position
    docs[i].tags = deriveTags(docs[i])  // derived and stored as array for indexing

  keys = consistencyKeyExtractor.extractKeys(condition)  // deterministic order and de duplication
  expectedAfter = extractAfter(condition)  // may be null

  beginTransaction()

    // consistency checkpoint enforcement
    for key in keys:
      checkpointId = "checkpoint:" + key

      // ensure checkpoint exists, and enforce precondition:
      // - if expectedAfter is null, treat as unconstrained and always advance checkpoint
      // - otherwise only advance checkpoint if checkpoint.lastPosition <= expectedAfter
      matched = updateOne(
        filter = { _id: checkpointId, lastPosition: (expectedAfter == null ? any : { $lte: expectedAfter }) },
        update = { $set: { lastPosition: end } },
        upsert = true
      )
      if matched == 0:
        abortTransaction()
        throw WriteConditionNotFulfilledException

    // persist events
    insertMany(docs)

  commitTransaction()

  return WriteResult(streamid, oldToken, newToken)
```

Notes:
- `oldToken` and `newToken` should be defined consistently for each API.
  - For stream operations, use the latest observed `position` as the token.
  - For DCB reads, use the global high water mark `position`.
- Deadlock avoidance:
  - Always update checkpoint documents in sorted key order.
- Retry strategy:
  - On transient transaction errors, retry the whole transaction.
  - Position allocation may occur outside the transaction, allowing gaps when retries occur.

### Read semantics

Stream read:
- Query by `streamid` ordered by `position` ascending.
- For compatibility, return `streamversion = position` if needed.

DCB read:
- Query by tags and or type combined with `position` ordering and optional `after` lower bound.
- Return a token based on the global high water mark `position` that can be used as `after` in subsequent DCB writes.

### Rationale for streamVersion behavior

- StreamVersion is retained for compatibility, but it is not treated as contiguous and may contain gaps.
- Accurate stream sizing is provided by the existing `count(streamId)` method.
- Using `position` as streamVersion aligns the concurrency token concept across stream, DCB, and custom approaches, and avoids needing a separate per stream counter if unification is desired.

### Consistency keys for DCB

For DCB, a "consistency key" identifies the consistency boundary that must be protected by a consistency checkpoint. A `DcbCondition` resolves to one or more consistency keys, and the write must successfully advance the checkpoint for each key.

The most common approach is to derive one key per tag:

- Key format: `tag:<tagValue>`
- Checkpoint document id: `checkpoint:tag:<tagValue>`

Examples:
- Event tags: `["user:123", "course:432"]`
- Boundary per user: keys = `["tag:user:123"]`
- Boundary per user and course: keys = `["tag:user:123", "tag:course:432"]`

Tag order must not matter. Tags are treated as a set when deriving keys, and the resulting key set is always sorted and de duplicated to ensure deterministic checkpoint update ordering and to reduce deadlock risk. For example, `["user:123", "course:432"]` and `["course:432", "user:123"]` resolve to the same key set.

If additional precision is needed, keys may be namespaced by CloudEvent type:

- Key format: `type:<type>|tag:<tagValue>`

Alternative query primitives can be used as long as they map deterministically to keys:

- Subject based boundary: `subject:<subjectValue>`
- Any custom boundary must define a stable, canonical key format and ensure appropriate indexes exist for the corresponding query.

### allocatePositions implementation

```
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static java.util.Objects.requireNonNull;

public final class GlobalPositionAllocator {

    public record PositionRange(long start, long end) {
        public PositionRange {
            if (start > end) {
                throw new IllegalArgumentException("start must be <= end");
            }
        }
        public long size() {
            return end - start + 1;
        }
    }

    public static final class CounterDoc {
        @Id
        private String id;
        private long value;

        // Needed by Spring Data
        public CounterDoc() {
        }

        public CounterDoc(String id, long value) {
            this.id = id;
            this.value = value;
        }

        public String getId() {
            return id;
        }

        public long getValue() {
            return value;
        }
    }

    private final MongoTemplate mongoTemplate;
    private final String countersCollection;
    private final String counterId;

    public GlobalPositionAllocator(MongoTemplate mongoTemplate) {
        this(mongoTemplate, "occurrent_counters", "globalPosition");
    }

    public GlobalPositionAllocator(MongoTemplate mongoTemplate, String countersCollection, String counterId) {
        this.mongoTemplate = requireNonNull(mongoTemplate, "mongoTemplate");
        this.countersCollection = requireNonNull(countersCollection, "countersCollection");
        this.counterId = requireNonNull(counterId, "counterId");
    }

    /**
     * Allocates a contiguous range of global positions for a batch of size n.
     * Uses one atomic write: findAndModify($inc).
     *
     * Note: gaps are possible if you allocate outside a transaction and the later write aborts.
     */
    public PositionRange allocatePositions(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be > 0");
        }

        Query query = new Query(Criteria.where("_id").is(counterId));
        Update update = new Update().inc("value", (long) n);

        FindAndModifyOptions options = FindAndModifyOptions.options()
                .returnNew(true)
                .upsert(true);

        CounterDoc updated = mongoTemplate.findAndModify(
                query,
                update,
                options,
                CounterDoc.class,
                countersCollection
        );

        if (updated == null) {
            throw new IllegalStateException("findAndModify returned null unexpectedly");
        }

        long end = updated.getValue();
        long start = end - n + 1L;
        return new PositionRange(start, end);
    }
}
```

### updateCheckpoint implementation

```
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static java.util.Objects.requireNonNull;

public final class ConsistencyCheckpointStore {

    public static final String ID = "_id";
    public static final String LAST_POSITION = "lastPosition";

    private final MongoTemplate mongoTemplate;
    private final String checkpointsCollection;

    public ConsistencyCheckpointStore(MongoTemplate mongoTemplate, String checkpointsCollection) {
        this.mongoTemplate = requireNonNull(mongoTemplate, "mongoTemplate");
        this.checkpointsCollection = requireNonNull(checkpointsCollection, "checkpointsCollection");
    }

    /**
     * Advances checkpoint "checkpoint:{key}" to {@code newLastPosition} if allowed by {@code expectedAfter}.
     *
     * Semantics:
     * - If expectedAfter == null: always advance (or create) the checkpoint.
     * - If expectedAfter != null: advance only if checkpoint.lastPosition <= expectedAfter,
     *   treating a missing checkpoint as lastPosition == 0 (i.e. allowed).
     *
     * Returns true if the checkpoint was advanced or inserted, false if the precondition was not fulfilled.
     *
     * Notes:
     * - Call this inside the same Mongo transaction as your event inserts.
     * - Concurrent upserts for a never-before-seen key can throw DuplicateKeyException; retry the transaction.
     */
    public boolean updateCheckpoint(String key, Long expectedAfter, long newLastPosition) {
        requireNonNull(key, "key");
        if (newLastPosition <= 0) {
            throw new IllegalArgumentException("newLastPosition must be > 0");
        }

        String checkpointId = "checkpoint:" + key;

        Criteria idCriteria = Criteria.where(ID).is(checkpointId);

        Query query;
        if (expectedAfter == null) {
            // Unconstrained: always advance the checkpoint.
            query = new Query(idCriteria);
        } else {
            // Constrained: allow if lastPosition <= expectedAfter, or if the doc does not exist yet.
            Criteria allowedByAfter = new Criteria().orOperator(
                    Criteria.where(LAST_POSITION).lte(expectedAfter),
                    Criteria.where(LAST_POSITION).exists(false)
            );
            query = new Query(new Criteria().andOperator(idCriteria, allowedByAfter));
        }

        Update update = new Update()
                .set(LAST_POSITION, newLastPosition)
                // Optional metadata you might want:
                .setOnInsert("kind", "checkpoint")
                .setOnInsert("key", key);

        UpdateResult result = mongoTemplate.updateFirst(query, update, checkpointsCollection);

        // Success if we matched and updated an existing doc, or we inserted a new one via upsert.
        // With Spring Data, an upsert sets upsertedId; matchedCount stays 0 in that case.
        long matched = result.getMatchedCount();
        BsonValue upsertedId = result.getUpsertedId();

        return matched > 0 || upsertedId != null;
    }
}
```