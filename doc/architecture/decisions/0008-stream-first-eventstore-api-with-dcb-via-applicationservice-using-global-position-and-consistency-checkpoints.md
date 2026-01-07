# 8. Stream-first EventStore API with DCB via ApplicationService using global position and consistency checkpoints

Date: 2026-01-06

## Status

Proposal

## Context

Occurrent has historically treated streams as first-class:

- Writes target a `streamId`.
- Each persisted CloudEvent is associated with a `streamid` (CloudEvents extension).
- Each persisted CloudEvent has a `streamversion` (CloudEvents extension) used for stream-based conditional writes.
- Reads are typically performed via an `EventStoreQueries` API, but the underlying storage is a single MongoDB collection where stream semantics are logical and implemented via attributes and indexes.

Dynamic Consistency Boundary (DCB) style concurrency requires preventing write skew for invariants that span multiple boundaries (for example "student:123" and "course:432"). DCB expresses correctness as:

- A monotonic sequence position (token) that represents the commit order of events.
- A precondition of the form "no events matching query Q exist after position P".

A common pitfall is attempting to enforce DCB correctness using per-stream optimistic concurrency only. This fails when:
- events relevant to a single logical boundary can be written to different streams, or
- an invariant spans multiple boundaries (multiple tags, subject keys, or multiple streams).

If DCB writes are routed to a single stream (for example `occurrent:dcb`), contention becomes a bottleneck and defeats the purpose of dynamic boundaries.

Therefore, Occurrent needs:
1) A global position to support monotonic tokens across arbitrary queries and partitions.
2) An atomic enforcement mechanism to prevent write skew across arbitrary boundaries.
3) Stream-first semantics to preserve Occurrent's current API model and backward compatibility.

This ADR keeps streams as first-class for the EventStore API, but introduces DCB as an ApplicationService concern that selects an appropriate streamId automatically (partitioned) while still enforcing correctness via the same underlying concurrency mechanism.

This ADR also standardizes that `streamversion` is treated as a compatibility field and is set equal to `globalPosition` (or derived from it on read).

## Decision

### 1) Add a global position to every event

- Introduce `globalPosition` as a globally unique, monotonically increasing sequence number for every persisted event.
- Gaps are allowed (for example because position allocation can happen outside the write transaction and a later retry may abandon a range).

Compatibility:
- Persist or expose `streamversion = globalPosition`.
- Stream version is not treated as contiguous and may contain gaps.
- Stream sizing continues to be supported via `count(streamId)` or the query layer, not by assuming streamversion contiguity.

### 2) Unify all conditional writes using consistency checkpoints

All conditional writes, including classic stream-based and DCB-style, are enforced by the same primitive:

- A write condition resolves to one or more consistency keys.
- For each key, the datastore maintains a consistency checkpoint document with `lastPosition`.
- A write is allowed only if each checkpoint can be advanced from its current `lastPosition` to the end position of this write, subject to the write condition token (`afterPosition`).

Terminology:
- "Consistency key" is a stable identifier for a boundary (for example `stream:course:432`, `tag:user:123`).
- "Consistency checkpoint" is a document tracking the latest committed position for a key.

### 3) Keep streams first-class in the EventStore interface

The EventStore write API requires a `streamId`. This preserves Occurrent's stream-based model and keeps destination explicit.

DCB does not define a destination stream. In Occurrent, stream selection for DCB is handled in a DCB-specific ApplicationService.

### 4) Introduce a generic DCB ApplicationService API based on boundary tags

The DCB ApplicationService should feel similar to the stream-based ApplicationService, but with DCB boundaries as input.

Desired usage style:

- Stream-based:
  `applicationService.execute("streamId", events -> WordGuessingGame.guessWord(events, guess));`

- DCB-based:
  `dcbApplicationService.execute(Set.of("tag1", "tag2"), events -> WordGuessingGame.guessWord(events, guess));`

In the DCB case:
- The supplied tag set is the boundary used for the read query and the write precondition.
- The DCB ApplicationService computes `afterPosition` from the read result.
- The DCB ApplicationService assigns a destination `streamId` automatically using a StreamIdGenerator derived from the boundary tags (partitioned).
- The DCB ApplicationService writes using `EventStore.write(streamId, WriteCondition, events)`.

#### TagGenerator is required for writing tags to events

The boundary tag set used for the read does not necessarily equal the tag set that should be attached to each newly produced event. Some events may need:
- additional tags (for example `student:123`, `course:432`, and `enrollment:...`), or
- different tags depending on the event type.

Therefore, the DCB ApplicationService must not blindly copy the boundary tag set onto all outgoing events. It needs a TagGenerator to attach the correct tags per newly generated event.

TagGenerator naming:
- In the DCB ApplicationService, the user-facing concept is tags, so the component is named TagGenerator.

```java
import io.cloudevents.CloudEvent;

import java.util.Set;

public interface TagGenerator {
    /**
     * Generate the tags that should be persisted (indexed) for this particular event.
     * The returned set may be a superset of the boundary tags used for the DCB read.
     */
    Set<String> generateTags(CloudEvent event);
}
```

#### DCB ApplicationService interface (generic higher-order style)

```java
import io.cloudevents.CloudEvent;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface DcbApplicationService {

    /**
     * Execute a DCB operation.
     *
     * @param boundaryTags The DCB boundary tags used for read selection and write precondition.
     *                    Tag order must not matter.
     * @param decide      A pure function that decides which new events to emit based on the current history.
     * @return            The decided value (optional) or metadata depending on implementation needs.
     */
    <R> R execute(Set<String> boundaryTags, Function<List<CloudEvent>, Decision<R>> decide);

    record Decision<R>(R result, List<CloudEvent> newEvents) {}
}
```

Behavior:
- Read query is derived from `boundaryTags` (canonicalized, sorted, deduped).
- WriteCondition query is derived from the same `boundaryTags`.
- New events produced by the function are tagged using TagGenerator, not necessarily equal to `boundaryTags`.
- StreamIdGenerator is derived from the boundaryTags, not from the tags attached to events.

### 5) Define a query model usable for both reads and write conditions

A single `ConsistencyQuery` model is used:
- by the query API (read selection),
- by the write API (precondition expression),
- by the datastore consistency checkpoint implementation.

Note: `AnyOf` is useful for reads but is dangerous as a write precondition. For write conditions:
- either reject `AnyOf`, or
- interpret it conservatively by extracting the union of all referenced boundary keys.

## EventStore API

```java
import io.cloudevents.CloudEvent;

import java.util.List;
import java.util.Objects;

public interface EventStore {

    ReadResult read(ConsistencyQuery query);

    WriteResult write(String streamId, WriteCondition condition, List<CloudEvent> events);

    default WriteResult write(String streamId, WriteCondition condition, CloudEvent event) {
        Objects.requireNonNull(event, "event cannot be null");
        return write(streamId, condition, List.of(event));
    }

    long count(String streamId);

    // --- Value types ---

    sealed interface ConsistencyQuery permits ConsistencyQuery.Eq, ConsistencyQuery.Tag, ConsistencyQuery.Subject,
            ConsistencyQuery.AllOf, ConsistencyQuery.AnyOf {

        record Eq(String attribute, String value) implements ConsistencyQuery {
            public Eq {
                Objects.requireNonNull(attribute, "attribute");
                Objects.requireNonNull(value, "value");
            }
        }

        record Tag(String value) implements ConsistencyQuery {
            public Tag {
                Objects.requireNonNull(value, "value");
            }
        }

        record Subject(String value) implements ConsistencyQuery {
            public Subject {
                Objects.requireNonNull(value, "value");
            }
        }

        record AllOf(List<ConsistencyQuery> parts) implements ConsistencyQuery {
            public AllOf {
                Objects.requireNonNull(parts, "parts");
            }
        }

        record AnyOf(List<ConsistencyQuery> parts) implements ConsistencyQuery {
            public AnyOf {
                Objects.requireNonNull(parts, "parts");
            }
        }
    }

    /**
     * Semantics: all boundaries implied by query must not have advanced since afterPosition.
     */
    record WriteCondition(ConsistencyQuery query, long afterPosition) {
        public WriteCondition {
            Objects.requireNonNull(query, "query");
            if (afterPosition < 0) {
                throw new IllegalArgumentException("afterPosition must be >= 0");
            }
        }
    }

    record ReadResult(List<CloudEvent> events, long highWatermarkPosition) {
        public ReadResult {
            Objects.requireNonNull(events, "events");
            if (highWatermarkPosition < 0) {
                throw new IllegalArgumentException("highWatermarkPosition must be >= 0");
            }
        }
    }

    record WriteResult(String streamId, long oldToken, long newToken) {
        public WriteResult {
            Objects.requireNonNull(streamId, "streamId");
        }
    }
}
```

Notes:
- `ReadResult.highWatermarkPosition` is the max `globalPosition` observed in the returned events (or 0 if none).
- The write `oldToken` and `newToken` are positions (global) for consistency. For example:
  - oldToken: the observed token used to build the condition,
  - newToken: the last position written in this batch.

## Why globalPosition is still required even when streams exist

Per-stream `streamversion` (optimistic concurrency on a single stream) cannot enforce DCB-style invariants because DCB conflicts can span:
- multiple tags,
- multiple subjects,
- multiple streams,
- multiple partitions.

If DCB writes are routed to partition streams to avoid contention, two writes that should conflict may land in different streams. Per-stream `streamversion` cannot detect that conflict.

Example:

- Write W1 depends on tags `{ user:123, course:432 }`.
- Write W2 depends on tags `{ user:123, course:999 }`.
- If routing uses hash of the tag set, W1 and W2 may go to different streams.
- Per-stream `streamversion` cannot detect the shared boundary `user:123`.
- Without a global token and checkpointing keyed by `user:123`, write skew is possible.

A global monotonic position is required to:
- produce a comparable token across arbitrary queries and partitions,
- allow conditions of the form "no matching events after position P",
- unify the enforcement mechanism for stream-based, DCB-based, and custom boundary keys.

In this ADR, `streamversion` is set equal to `globalPosition` for compatibility, but correctness relies on:
- globalPosition allocation, and
- consistency checkpoint updates derived from WriteCondition boundaries.

## Consistency keys and checkpoint mapping

### Why a ConsistencyKeyExtractor is still required even though writes always target a streamId

The streamId is the write destination. It is not, by itself, the full set of boundaries that must be protected by a WriteCondition.

If the datastore only gated concurrency by the destination streamId, it would reduce all conditional writes to classic per-stream optimistic concurrency. That is insufficient for DCB and for any write condition that depends on boundaries other than the destination stream.

Concrete failure case when gating only by destination stream:

- The DCB ApplicationService routes writes to partition streams to avoid contention.
- Two writes share a boundary key but are routed to different streams.

Example:
- W1 boundary tags: `{ user:123, course:432 }` -> routed to `dcb:partition:7`
- W2 boundary tags: `{ user:123, course:999 }` -> routed to `dcb:partition:42`

If the datastore only gates on streamId:
- W1 and W2 touch different streams, so both writes can succeed concurrently.
- But they both depend on `user:123` and should conflict under the invariant being protected.
- This yields write skew and violates DCB semantics.

Therefore, correctness requires gating on the boundaries expressed in WriteCondition, not merely on the destination stream.

What ConsistencyKeyExtractor provides:
- a deterministic mapping from WriteCondition query to the set of consistency checkpoint keys that must be advanced atomically,
- stable names for checkpoint keys across different boundary types (stream, tag, subject, future custom predicates),
- deterministic ordering (sorted, deduped) to avoid deadlocks and ensure reproducibility.

Could this be avoided by routing all writes for a boundary into a single stream?
- Only if you force a single canonical routing key (for example always route by `user:123`), which:
  - reintroduces per-boundary hotspots,
  - does not generalize cleanly when invariants depend on multiple boundaries,
  - couples storage routing to invariants and makes cross-boundary writes harder to reason about.
- Occurrent explicitly avoids this by letting DCB routing be a distribution mechanism and letting checkpoints enforce correctness.

Net:
- streamId is required for destination,
- ConsistencyKeyExtractor is required to determine which checkpoint boundaries the write depends on,
- TagGenerator is a different concern (tagging the newly produced events), and cannot replace ConsistencyKeyExtractor.

### ConsistencyKeyExtractor

```java
import java.util.SortedSet;

public interface ConsistencyKeyExtractor {
    SortedSet<String> extractKeys(EventStore.ConsistencyQuery query);
}
```

Rules:
- Tags are treated as a set, tag order does not matter.
- Returned keys are sorted and de-duplicated to ensure deterministic checkpoint update ordering.

Example mappings:
- Stream boundary: `Eq("streamid", "course:432")` -> key `stream:course:432`
- Tag boundary: `Tag("user:123")` -> key `tag:user:123`
- Subject boundary: `Subject("user:123")` -> key `subject:user:123`
- AllOf -> union of keys from all parts

### Consistency checkpoints (MongoDB)

Checkpoint document:
- `_id`: `"checkpoint:" + key`
- `lastPosition`: long

Each write transaction advances all involved checkpoints if and only if the precondition token allows it.

## StreamIdGenerator for DCB ApplicationService (consistent hashing)

Goal:
- Avoid a single hot stream (like `occurrent:dcb`).
- Route DCB writes to partition streams deterministically.

Approach:
- Use the boundary tags (canonicalized, sorted, deduped).
- Hash to a 64-bit value.
- Apply Jump Consistent Hash to select bucket in `[0..numPartitions-1]`.

```java
import java.nio.charset.StandardCharsets;
import java.util.SortedSet;
import java.util.zip.CRC32;

public interface StreamIdGenerator {
    String generateStreamId(SortedSet<String> boundaryTags);
}

final class PartitionedDcbStreamIdGenerator implements StreamIdGenerator {

    private final int partitions;
    private final String prefix;

    PartitionedDcbStreamIdGenerator(int partitions) {
        this(partitions, "dcb:partition:");
    }

    PartitionedDcbStreamIdGenerator(int partitions, String prefix) {
        if (partitions <= 0) throw new IllegalArgumentException("partitions must be > 0");
        this.partitions = partitions;
        this.prefix = prefix;
    }

    @Override
    public String generateStreamId(SortedSet<String> boundaryTags) {
        if (boundaryTags.isEmpty()) {
            return prefix + "0";
        }
        String canonical = String.join("|", boundaryTags);

        long hash64 = hashToLong(canonical);
        int bucket = jumpConsistentHash(hash64, partitions);
        return prefix + bucket;
    }

    private static long hashToLong(String s) {
        CRC32 crc = new CRC32();
        crc.update(s.getBytes(StandardCharsets.UTF_8));
        long x = crc.getValue();
        return (x << 32) ^ (x * 0x9E3779B97F4A7C15L);
    }

    static int jumpConsistentHash(long key, int numBuckets) {
        long b = -1;
        long j = 0;
        while (j < numBuckets) {
            b = j;
            key = key * 2862933555777941757L + 1;
            j = (long) ((b + 1) * (1L << 31) / (double) ((key >>> 33) + 1));
        }
        return (int) b;
    }
}
```

Notes:
- Correctness does not depend on routing. Correctness depends on checkpointing on the underlying boundary keys.
- A good default is 64 partitions.

## Example usage

### A) DCB StudentJoinedCourse enrollment using boundary tags and TagGenerator

```java
import io.cloudevents.CloudEvent;

import java.util.List;
import java.util.Set;

final class EnrollmentDcbWorkflow {

    private final DcbApplicationService dcb;

    EnrollmentDcbWorkflow(DcbApplicationService dcb) {
        this.dcb = dcb;
    }

    public void enroll(String studentId, String courseId) {
        String userTag = "user:" + studentId;
        String courseTag = "course:" + courseId;

        dcb.execute(Set.of(userTag, courseTag), events -> {
            // 1) Validate invariants using current history (events)
            // 2) Decide which events to emit
            CloudEvent joined = CloudEvents.studentJoinedCourse(studentId, courseId);

            // Note: The TagGenerator (configured on the DCB AS) decides which tags to attach to `joined`.
            // It may attach userTag and courseTag and optionally additional tags.
            return new DcbApplicationService.Decision<>(null, List.of(joined));
        });
    }
}
```

### B) Classic stream-based write to a single stream

```java
import io.cloudevents.CloudEvent;

import java.util.List;

final class StreamApplicationService {

    private final EventStore eventStore;

    StreamApplicationService(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void addCourseEvent(String courseId, CloudEvent event) {
        String courseStream = "course:" + courseId;

        EventStore.ConsistencyQuery query = new EventStore.ConsistencyQuery.Eq("streamid", courseStream);
        EventStore.ReadResult read = eventStore.read(query);
        long after = read.highWatermarkPosition();

        EventStore.WriteCondition condition = new EventStore.WriteCondition(query, after);
        eventStore.write(courseStream, condition, List.of(event));
    }
}
```

### C) Cross-stream invariant without tags (read two streams, write to one)

```java
import io.cloudevents.CloudEvent;

import java.util.List;

final class CrossStreamEnrollmentService {

    private final EventStore eventStore;

    CrossStreamEnrollmentService(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void enroll(String studentId, String courseId) {
        String studentStream = "student:" + studentId;
        String courseStream = "course:" + courseId;

        EventStore.ConsistencyQuery readQuery = new EventStore.ConsistencyQuery.AnyOf(List.of(
                new EventStore.ConsistencyQuery.Eq("streamid", studentStream),
                new EventStore.ConsistencyQuery.Eq("streamid", courseStream)
        ));
        EventStore.ReadResult read = eventStore.read(readQuery);
        long after = read.highWatermarkPosition();

        CloudEvent joined = CloudEvents.studentJoinedCourse(studentId, courseId);

        EventStore.ConsistencyQuery writeQuery = new EventStore.ConsistencyQuery.AllOf(List.of(
                new EventStore.ConsistencyQuery.Eq("streamid", studentStream),
                new EventStore.ConsistencyQuery.Eq("streamid", courseStream)
        ));
        EventStore.WriteCondition condition = new EventStore.WriteCondition(writeQuery, after);

        eventStore.write(courseStream, condition, List.of(joined));
    }
}
```

## Consequences

Positive:
- Preserves Occurrent's stream-first write semantics and reduces backward incompatibility.
- Enables a generic DCB ApplicationService API that feels similar to the stream-based ApplicationService.
- Avoids a single global stream hotspot by routing DCB writes to partition streams.
- Unifies stream-based, DCB-based, and custom boundary enforcement via one checkpointing mechanism.
- Provides a single monotonic token (`globalPosition`) usable across all queries and partitions.
- Maintains compatibility by standardizing `streamversion = globalPosition`.

Negative:
- Requires a global position allocator and additional checkpoint writes per conditional write.
- Adds operational complexity and write amplification (checkpoint updates, additional indexes).
- Requires careful definition of which query forms are allowed for write conditions (AnyOf is unsafe unless conservative).
- Requires TagGenerator configuration for DCB to attach correct tags to newly produced events.
- Requires routing logic for DCB writes (default provided, but must be understood and possibly tuned).