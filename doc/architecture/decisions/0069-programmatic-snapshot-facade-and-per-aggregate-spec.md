# 69. Programmatic snapshot API: global facade plus per-aggregate spec

Date: 2026-07-24

## Status

Accepted

Refines [ADR 61](0061-first-class-snapshot-support.md).

## Context

ADR 61 introduced first-class snapshot support with three programmatic entry points: two decider executors
(`SnapshotDeciderApplicationService` for streams, `SnapshotDcbDeciderApplicationService` for DCB) and a deciders-free
on-demand reader (`SnapshotViews`). While finishing the programmatic API before release, two shapes turned out to clash
with Occurrent's established "construct one application service, use it for every use case" model.

**The decider executors bound per-aggregate config to an instance that should be global.** The `SnapshotStore<S>` and
`SnapshotOptions<S,E>` (schema version plus write policy) are per-aggregate, but they were either bound in the executor
constructor or passed per `execute(...)` call. Both are wrong. Binding them in the constructor makes the executor
aggregate-specific, because `SnapshotPolicies.whenTerminal(decider)` captures one decider, so you could not build one
executor and reuse it. Passing them per call was worse: nothing stopped the same stream id from getting different
options for a `Deposit` than for a `Withdraw`, which is meaningless because a snapshot policy is a property of the
aggregate, not of the command. The per-call form existed only as unused Kotlin extension sugar, and the documented Java
examples did not compile.

**The deciders-free reader bound the store at construction too.** `SnapshotViews.create(eventStore, converter, store)`
made the facade `SnapshotViews<S,E>` per state type rather than one global reader over the event store.

This is all unreleased (under `### Changelog next version`), so it was free to reshape with no migration path.

An alternative that keeps a single executor was considered and rejected: make `SnapshotStore` global and type-agnostic
so it need not be per-aggregate. It cannot remove the per-aggregate state-type declaration, because scalar and enum
snapshot state is not self-describing and the Mongo store needs `Class<S>` to deserialize, so a global store only
relocates the type declaration while rewriting a core capability and the just-merged store, `SnapshotView`, and
`@Snapshot`. That is overengineering for no gain.

## Decision

**A global executor or facade holds the shared infrastructure, and a per-aggregate spec holds everything specific to one
aggregate.** This mirrors the `Decider` plus `DeciderApplicationService` and `DcbDecider` plus `DcbDeciderApplicationService`
split already in the codebase: a facade built once around the global write engine (or the event store plus converter),
and a small value passed per call that carries the per-feature detail.

The three executors keep their role but lose their per-aggregate constructor arguments:

- `SnapshotDeciderApplicationService<E>` and `SnapshotDcbDeciderApplicationService<E>` take only the underlying
  application service. They are generic in the event type `E` only. `execute(...)` gains a state type parameter `S` and
  takes the spec per call.
- `SnapshotViews<E>` is created with `create(eventStore, converter)`. `readState`/`refresh` gain the state type
  parameter and take the spec per call.

Three new per-aggregate specs, each created with a `from(...)` factory that mirrors `DcbDecider.from`:

```java
// stream decider path
var snapshots = new SnapshotDeciderApplicationService<>(applicationService);          // global, once
var ledger = SnapshotDecider.from(ledgerDecider, ledgerStore,
        SnapshotOptions.everyNEvents(1, 100).or(SnapshotPolicies.whenTerminal(ledgerDecider)));
WriteResult r = snapshots.execute(periodId, new Deposit(100), ledger);

// deciders-free read path
var views = SnapshotViews.create(eventStore, cloudEventConverter);                    // global, once
var accountSource = SnapshotViewSource.from(accountView, accountStore);               // per aggregate
AccountState s = views.readState(accountId, accountSource);
views.refresh(accountId, accountSource);                                              // explicit maintenance write
```

- `SnapshotDecider<C,S,E>` bundles a `Decider` with its `SnapshotStore` and `SnapshotOptions`.
- `SnapshotDcbDecider<C,S,E>` bundles a `DcbDecider` with its `SnapshotStore`, `SnapshotOptions`, and the function that
  turns the resolved `DcbCriteria` into a snapshot key. `from(dcbDecider, store, options)` defaults the key function to
  `DcbSnapshotKeys::canonicalKey`; a four-argument `from` overload takes an explicit one. The key function moved off the
  executor constructor and onto the spec, since it is per-aggregate.
- `SnapshotViewSource<S,E>` bundles a `SnapshotView` with its `SnapshotStore`. It is named to stay distinct from
  `SnapshotView` (the fold) and `SnapshotViews` (the facade).

Reactor twins (`ReactiveSnapshotDecider`, `ReactiveSnapshotDcbDecider`, `ReactiveSnapshotViewSource`) hold a
`ReactiveSnapshotStore<S>` and pair with the reactive executors.

**The executor is kept, not folded into the spec.** It holds the global write engine or the event store plus converter,
which must not be copied into every per-aggregate spec, and it keeps the parallel with `Decider`/`DcbDecider` plus their
application services. A self-executing spec would drag the global infrastructure into a per-aggregate object and misuse
the `Decider` name for something that also writes.

**The spec deliberately bundles the `SnapshotStore` rather than threading it per call.** This means a spec holds an I/O
collaborator, so unlike `Decider`/`DcbDecider` it is not a pure value. That is an accepted trade. The whole point is to
define each aggregate's snapshotting once and then pass that single object, so threading the store per call would defeat
it and reintroduce the "different config per call for the same aggregate" footgun. The spec stays inert: the executor
performs all reads and writes, and the wrapped decider or view stays independently pure and testable.

**The factory idiom is intentionally not uniform.** `new` for the application services (matching
`DeciderApplicationService`/`DcbDeciderApplicationService`), `create(...)` for the `SnapshotViews` reader (matching
`ProjectionRunner`/`SagaRunner`), and `from(...)` for the specs (matching `DcbDecider`).

The unused per-command Kotlin `execute(..., store, options[, keyFunction])` extensions are removed. Kotlin builds the
facade and the specs directly, exactly as Java does, since all of them are plain Java types.

The invariants from ADR 61 are unchanged. In particular the decider executors still save the snapshot post-commit,
best-effort (a save failure is logged, never fails the committed command), while `SnapshotViews.refresh` is an explicit
maintenance write that propagates a store failure. The reshape only moved where `store`, `options`, and the key function
are read from, not what the executors do with them.

### Alternatives considered and rejected

- **Per-command `store`/`SnapshotOptions` on `execute`.** Lets snapshot policy vary per command for one aggregate, which
  is meaningless, and left the Java examples non-compiling.
- **Binding `store`/`options` in the executor constructor.** Makes the executor aggregate-specific through
  `whenTerminal(decider)`, so it cannot be the one reusable facade the rest of Occurrent uses.
- **A global, type-agnostic `SnapshotStore`.** Cannot remove the per-aggregate state-type declaration, because scalar
  and enum state is not self-describing and Mongo needs `Class<S>`. It only relocates the declaration while rewriting a
  core capability plus already-merged code.
- **Passing the store per call with otherwise-pure specs.** Defeats the goal of defining each aggregate's snapshotting
  once and passing one object, and reopens the per-call-config footgun. Bundling the store, and accepting the mild
  impurity, is the better trade.
- **A self-executing spec with no separate executor.** Drags the global write engine into a per-aggregate object and
  misuses the `Decider` name for a type that also does I/O.

## Consequences

- One `SnapshotDeciderApplicationService`, `SnapshotDcbDeciderApplicationService`, and `SnapshotViews` is constructed per
  application and reused for every aggregate, matching how `DeciderApplicationService` and friends are already used.
- The stream and deciders-free programmatic paths now read and feel the same: a global facade plus a `from(...)` spec.
- A spec is not a pure value, since it holds a `SnapshotStore`. This is a deliberate, documented trade for defining
  snapshotting once per aggregate. The wrapped decider or view remains pure.
- `@Snapshot` is unaffected. The declarative path uses `SnapshotView`, not these facades, and keeps its own store
  resolution. The split between the annotation and the programmatic facades is unchanged from ADR 61.
- No general `Aggregate` abstraction is introduced. These specs are the per-aggregate snapshot definition only; a broader
  aggregate concept is a separate, larger question.
- Being unreleased, this ships with no migration path and no deprecation. The removed Kotlin extensions were never
  released.
