# 1. Introduce optional capability interface for filtered stream reads

Date: 2026-01-16

## Status

Accepted

## Context

Occurrent's `EventStore` is currently composed from smaller capability interfaces:

- `ReadEventStream`
- `ConditionallyWriteToEventStream`
- `UnconditionallyWriteToEventStream`
- `EventStreamExists`

Stream reads today are defined in `ReadEventStream` and are intentionally minimal:

- `read(streamId)`
- `read(streamId, skip, limit)`

We are introducing `StreamReadFilter`, a stream-scoped filter DSL that allows filtering by CloudEvent attributes, extensions, and data while explicitly excluding stream identity and concurrency fields (streamId and streamVersion). This feature is primarily meant to reduce read amplification for certain commands and use cases.

Not all underlying data stores are expected to support server-side filtering for stream reads. Examples include:
- event stores with limited query support for stream partitioned reads
- alternative backends where filtering requires expensive scans
- implementations that aim to remain minimal and portable

If we add `read(streamId, StreamReadFilter)` directly to `ReadEventStream`, then all `EventStore` implementations would be forced to implement it, even if only by reading everything and filtering client-side. This would:
- create performance traps
- blur the meaning of the API contract
- make it harder to port Occurrent to backends that cannot support this capability efficiently

At the same time, the `ApplicationService` currently depends on `EventStore`. Splitting the application service into multiple types, one requiring filtering and one not, would increase surface area and fragmentation, and complicate adoption.

We want:
- portability for event store implementations
- a single ApplicationService abstraction
- a clear and explicit failure mode if filtering is requested but not supported

## Decision

1. Keep `ReadEventStream` minimal and backend-portable.
   - Do not add `StreamReadFilter` methods to `ReadEventStream`.

2. Introduce a new optional capability interface for filtered stream reads.
   - Name: `ReadEventStreamWithFilter` (or equivalent).
   - Methods:
     - `EventStream<CloudEvent> read(String streamId, StreamReadFilter filter)`
     - `EventStream<CloudEvent> read(String streamId, StreamReadFilter filter, int skip, int limit)`

3. Preserve a single `ApplicationService` type.
   - The ApplicationService continues to depend on the baseline `EventStore` interface.
   - When a command execution requests a `StreamReadFilter`, the ApplicationService requires that the underlying event store implements `ReadEventStreamWithFilter`.
   - If the capability is not present, the ApplicationService fails fast with a clear exception indicating that filtered stream reads are not supported by the configured event store.

4. Make filter usage explicit at the ApplicationService call site.
   - Provide an overload or options object that includes an optional `StreamReadFilter`.
   - If no filter is provided, the ApplicationService only uses `ReadEventStream`.
   - If a filter is provided, the ApplicationService uses `ReadEventStreamWithFilter`.

## Consequences

Positive:
- Event store implementations remain portable. Backends that cannot support stream read filtering are not forced to implement it.
- Avoids performance traps where an implementation silently does client-side filtering.
- A single ApplicationService abstraction is retained, avoiding API fragmentation and multiple application service variants.
- Filtered stream reads are an explicit opt-in capability with a clear failure mode when unsupported.
- Capability composition remains consistent with Occurrent's existing interface design approach.

Negative:
- Filtered stream reads require a runtime capability check.
- Users may encounter a runtime error if they request filtered reads on an event store that does not implement the capability.
- Documentation must clearly explain which features require `ReadEventStreamWithFilter` support.
- Implementations that want to filter must implement an additional interface and associated mapping and validation logic.