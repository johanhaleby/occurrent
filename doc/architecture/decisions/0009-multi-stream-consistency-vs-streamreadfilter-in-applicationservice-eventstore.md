# 9. Multi-Stream Consistency vs StreamReadFilter in ApplicationService/EventStore

Date: 2026-01-16

## Status

Accepted

## Context

Occurrent's ApplicationService executes commands by reading an event stream, applying domain logic, and conditionally writing new events back to the same stream. This model is simple, well understood, and aligns with Occurrent's current stream-first semantics.

Two related directions were considered:

1. Multi-stream consistency
   - Read events from multiple streams and enforce atomic compare-and-set style preconditions across them.
   - Optionally write to multiple streams within one transaction to ensure the guarded streams advance.
   - This can address certain cross-aggregate invariants (for example student-course enrollment) without adopting full Dynamic Consistency Boundary semantics.

2. StreamReadFilter for read efficiency
   - Allow reading only a subset of events from a stream, based on event type, subject, or other CloudEvent attributes.
   - Similar filtering already exists for subscriptions, and many command handlers do not require the full event history.

Multi-stream consistency was deemed attractive for some cases but introduces significant conceptual and API complexity:

- It forces the library to answer hard questions about canonical stream ownership, write destinations, and whether writing to a third stream is safe when preconditions are based on other streams.
- Correctness requires that streams used as concurrency guards must be advanced in the same transaction, otherwise write skew remains possible.
- Avoiding duplicated events typically requires link or marker events, which is a modeling pattern the project does not want to mandate.
- The resulting API would be substantially more complex than the current ApplicationService abstraction and could lead to confusing or unsafe usage.
- Implementing it correctly in MongoDB requires additional transactional logic and careful semantics around conditional updates, retries, and partial failures.

In contrast, StreamReadFilter is a low-risk, high-value improvement:

- It improves performance and reduces memory and deserialization overhead for common command handlers.
- It keeps the write model unchanged and does not alter concurrency semantics.
- It aligns with existing filtering concepts already present in subscription APIs.
- It is backward compatible and can be introduced incrementally.

Occurrent already separates read concerns via EventStoreQueries. Therefore, StreamReadFilter should be introduced at the EventStore layer, and then surfaced through ApplicationService as a convenience.

## Decision

1. Introduce StreamReadFilter support in EventStore.
   - StreamReadFilter allows selecting a subset of events from a stream based on CloudEvent metadata such as:
     - event type (`type`)
     - subject (`subject`)
     - source (`source`)
     - possibly additional extension attributes
   - StreamReadFilter is a read optimization. It is not a correctness feature.

2. Surface StreamReadFilter in ApplicationService as an optional read parameter.
   - ApplicationService will provide an overload that accepts a StreamReadFilter and internally delegates to EventStore.
   - The default ApplicationService behavior remains unchanged and reads the full stream.

3. Keep write semantics unchanged.
   - ApplicationService continues to write only to the target stream passed to execute.
   - Existing stream version based conditional write behavior remains unchanged.

4. Defer multi-stream consistency.
   - Occurrent will not introduce multi-stream read and atomic multi-stream conditional write semantics at this time.
   - Note though that workarounds exists by wrapping multiple ApplicationService calls (or create a custom AS for the speciifc use-case) in a transaction when using a transactional EventStore.
   - Multi-stream consistency remains a potential future enhancement but requires further design work and justification.
   - The project explicitly avoids requiring link events or marker events as a consistency mechanism in the core abstractions.

## Consequences

Positive:
- Reduces IO and CPU overhead for command handlers that only require a subset of events.
- Improves latency for common operations like time passage, rescheduling, and maintenance commands.
- Keeps the existing ApplicationService mental model and write semantics unchanged.
- Backward compatible, incremental adoption by users.
- Clean separation of concerns:
  - StreamReadFilter lives in EventStore where read logic already belongs.
  - ApplicationService can offer StreamReadFilter as a convenience without expanding the write interface.
- Avoids introducing complex, potentially confusing multi-stream concurrency semantics into the core API.

Negative:
- StreamReadFilter can be misused. Filtering out events that affect invariants can lead to incorrect decisions.
- Users may need guidance and documentation on safe filtering patterns and command handler assumptions.
- Does not solve cross-stream invariants. Users must continue to model invariants within a single stream or handle cross-aggregate workflows outside the core ApplicationService abstraction.
- Multi-stream consistency remains a future possibility, but will require additional design work and strong justification before adoption.