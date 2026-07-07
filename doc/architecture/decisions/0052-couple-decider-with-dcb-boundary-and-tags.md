# 52. Couple a Decider with its DCB boundary and tagging into a DcbDecider

Date: 2026-07-07

## Status

Accepted

## Context

Executing a command through a `Decider` on the DCB application service forced the caller to hand over three things
that must agree with each other but were specified separately: the read criteria (`DcbCriteria`) built at the call
site, the `Decider` passed at the call site, and the write tags produced by a global `TagGenerator` wired once into
`GenericDcbApplicationService`. Every DCB use case ended up repeating the boundary id, once to build the criteria and
once inside the command itself, and the read criteria and the write tags could silently drift apart because they came
from unrelated sources. Nothing caught that drift at compile time.

The DCB `execute` already reuses the read criteria as the optimistic-lock append condition
(`DcbAppendCondition.failIfEventsMatch(query, consistencyToken)`). That is the enabling fact here. Binding the
criteria to the decider makes the read scope and the append condition identical by construction, and binding the tags
to the decider makes the read boundary and the write tags come from one place. This is what dcb.events means when it
says the query should be deferred from the decision model definition rather than written by hand.

## Decision

Introduce `DcbDecider<C, S, E>`, a Java record in `dsl/dcb-dsl/common` that wraps a `Decider`, a
`Function<C, DcbCriteria>` that derives the read boundary from the command (returning `null` when the decider does
not apply to a given command, used during composition), and a `TagGenerator<E>` for the events it emits. All three
pieces now live next to each other on one object instead of being assembled separately at each call site.

The DSL gains `execute(command, dcbDecider)`, in both the blocking and reactor variants. The decider's tags reach the
append through a new optional per-execute `TagGenerator` on `DcbExecuteOptions`, which overrides the global one when
present. The global `TagGenerator` on `GenericDcbApplicationService` becomes optional, so a decider-only application
no longer has to configure one.

`DcbDecider` composes the same way `Decider` does, through `adapt` and `compose`. The composed read boundary is
`DcbCriteria.anyOf` over the boundaries returned by the children that recognize the command, and the composed tags
are the set union of the children's tags. This mirrors how `Decider.compose` already dispatches: a command is offered
to every child decider, and adapted children ignore commands and events that are not their own, so the composed
decision depends only on the children that actually recognize it.

A single `execute(commands, dcbDecider)` over a batch of commands requires every command in the batch to resolve to
the same criteria, since the batch is appended atomically under one append condition.

## Consequences

The read boundary, the append condition, and the write tags now have one source of truth: the decider. This closes
the drift hazard described above, since there is no longer a second, independently specified criteria or tag source
for them to disagree with. A decider-only application no longer needs a global `TagGenerator`.

Deriving the criteria from the command requires the command to carry the boundary ids the criteria is built from,
which the existing examples already do, so this is not a new requirement in practice.

The write path keeps its existing safety net: `addTags` still fails loudly if it needs to tag events but no tagger
(per-execute or global) is available, so events are never silently appended untagged.

Tagging now has two complementary models rather than one. A decider-based flow takes its tags from the `DcbDecider`
(per-execute), and needs no global tagger. Decider-less DCB, meaning the raw `execute(criteria, function)` path or
annotation-driven `@DcbTag`, still relies on the optional global `TagGenerator`, either an explicit bean or the
`AnnotationTagGenerator` fallback, or on per-execute tags passed through `DcbExecuteOptions`. The precedence is
per-execute first, then global. The Spring starter reflects this: the `DcbApplicationService` is now auto-configured
even when no global `TagGenerator` bean exists, so a decider-only application needs none, while `@DcbTag` and
raw-execute users still get a global tagger when one is present. The cost is that a user who enables DCB, uses neither
deciders nor `@DcbTag`, and forgets to define a tagger now sees the failure at append time rather than at wiring time.

This builds on the existing `Decider` and its composition model, and on the DCB application service and tag model
established by earlier ADRs. The DSL's previous `execute(criteria, command, decider)` extensions are replaced by the
`DcbDecider` form. Since the DCB API is unreleased, no deprecated alias is kept. The low-level
`execute(criteria, function)` on the application service is unchanged, for callers that produce and tag their own
events without a decider.
