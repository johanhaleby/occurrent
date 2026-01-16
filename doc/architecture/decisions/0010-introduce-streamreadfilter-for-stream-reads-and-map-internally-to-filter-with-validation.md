# 10. Introduce StreamReadFilter for stream reads and map internally to Filter with validation

Date: 2026-01-16

## Status

Accepted

## Context

Occurrent provides a general-purpose `org.occurrent.filter.Filter` DSL used for querying event stores and subscriptions. The Filter DSL is feature rich and already has established mapping logic to datastore queries, such as MongoDB query predicates.

Occurrent also uses stream-first semantics in the ApplicationService and in stream-based reads, where:
- `streamId` is supplied as a method argument and is therefore implicit for the read.
- `streamVersion` is a concurrency token and is not something callers should constrain during read selection in stream contexts.

But we want to:
- add stream read filtering in ApplicationService and the EventStore stream read path,
- avoid duplicating mapping logic and composition semantics,
- and still make illegal usage hard or impossible.

Reusing `Filter` directly in stream read APIs would allow callers to specify `streamId` and `streamVersion` conditions even though they are meaningless or unsafe in this context. This violates the goal of making illegal states unrepresentable and creates ambiguity for users.

At the same time, stream read filtering should support:
- CloudEvent core attribute predicates (type, subject, source, time, etc),
- CloudEvent extension predicates (custom extensions),
- data payload predicates.

A dedicated `StreamReadFilter` can provide the correct public surface for stream reads:
- exclude `all`, `streamId`, and `streamVersion` from the API,
- support `attribute`, `extension`, and `data`,
- support composition (`and`, `or`),
- and still reuse the existing `Filter` mapping logic by converting `StreamReadFilter` to `Filter` internally.

Because `attribute` and `extension` accept names, it is still possible to attempt filtering on forbidden names (`streamid`, `streamversion`). Therefore, validation must be performed during mapping in the EventStore implementation.

## Decision

1. Introduce a new public filter interface, `StreamReadFilter`, dedicated to stream reads.
   - It supports:
     - `attribute(name, condition)` for CloudEvent core attributes.
     - `extension(name, condition)` for CloudEvent extension attributes.
     - `data(path, condition)` for filtering inside the data payload.
     - composition via `and` and `or`.
   - It does not support:
     - `all` (match everything) as an explicit construct.
     - direct predicates for `streamId` or `streamVersion`.
   - Stream read APIs that want no filtering either:
     - use an overload without a filter, or
     - pass `null` or `Optional<StreamReadFilter>` depending on API style.

2. Keep `org.occurrent.filter.Filter` as the internal representation used by EventStore implementations.
   - EventStore stream read logic maps the provided StreamReadFilter into a Filter instance.
   - Existing datastore mapping and query composition logic remains unchanged.

3. Validate StreamReadFilter during mapping inside EventStore to prevent forbidden predicates.
   - Validation rules:
     - `attribute(OccurrentCloudEventExtension.STREAM_ID, ...)` is forbidden.
     - `attribute(OccurrentCloudEventExtension.STREAM_VERSION, ...)` is forbidden.
     - `extension(OccurrentCloudEventExtension.STREAM_ID, ...)` is forbidden.
     - `extension(OccurrentCloudEventExtension.STREAM_VERSION, ...)` is forbidden.
     - Any other attribute or extension name is allowed.
   - If a forbidden name is detected, EventStore throws an IllegalArgumentException with a clear message.

4. Implicitly apply the stream constraint in the EventStore stream read path.
   - The stream read method will always apply `Filter.streamId(streamId)` internally.
   - The effective filter becomes:
     - `Filter.streamId(streamId)` AND `mappedFilter(streamReadFilter)` (if provided)

5. Do not duplicate datastore mapping logic.
   - The mapping from StreamReadFilter to Filter is structural and preserves composition.
   - Only a thin adapter and validator is added.

## Consequences

Positive:
- Stream read filtering has a clear, stream-specific API surface that omits invalid operations by design.
- Users can still express useful predicates for:
  - core CloudEvent attributes,
  - custom extension attributes,
  - data payload fields.
- Existing Filter composition and datastore mapping logic is reused, minimizing implementation effort and risk.
- Validation is centralized in EventStore, ensuring consistent behavior across different call sites (ApplicationService, direct stream reads).
- The streamId constraint remains explicit in the method signature and implicit in query generation, reducing ambiguity.

Negative:
- Validation is still required because attribute and extension names are strings and forbidden predicates can be attempted indirectly.
- Introduces a second filter type, requiring an adapter layer and additional documentation.
- Users who already understand Filter may need to learn StreamReadFilter for stream reads, while Filter remains for general queries and subscriptions.
- If future needs require additional stream-specific constraints, StreamReadFilter may need to evolve or introduce further validation rules.