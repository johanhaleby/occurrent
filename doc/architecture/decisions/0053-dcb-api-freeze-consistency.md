# 53. DCB API freeze consistency pass

Date: 2026-07-07

## Status

Accepted

## Context

DCB is unreleased. Before the DCB API surface locks, PRs #293 and #294 swept the decider, DSL, and dcb-api packages for
internal inconsistencies that would otherwise ship as permanent API warts. None of these are new features. Each is a
narrow correction found by comparing the three compose combinators, the four execute variants, and the two retry
policies against each other and asking whether their behavior actually agreed.

Four gaps turned up. The three compose combinators, `TagGenerator.compose`, `Decider.compose`, and `DcbDecider.compose`,
allowed zero or one argument even though both are degenerate. The execute methods had no defined behavior for a
command that no decider recognizes at all, as opposed to a child decider inside a composition declining a command.
A whole-store optimistic lock had no dedicated spelling, so it was easy to confuse with a whole-store read and easy
to build by accident through composition. The blocking and reactor default retry policies had drifted to different
attempt counts.

## Decision

**Compose combinators require at least two arguments.** `TagGenerator.compose`, `Decider.compose`, and
`DcbDecider.compose` all now reject fewer than two elements with `IllegalArgumentException`, on both the varargs and
list overloads. A zero-argument compose built a decider that always returned no events, which is vacuously terminal
and never a real use case. A one-element compose is a pointless wrapper around its single argument. Requiring two
makes composing mean what it says, and it makes the three combinators consistent with each other instead of each
having its own tolerance for degenerate input.

**An unrecognized command is a thrown error, not a silent no-op.** `execute`, `executeAndReturnDecision`,
`executeAndReturnState`, and `executeAndReturnEvents` all throw `IllegalArgumentException` when a command resolves to
no DCB boundary at all, on both the blocking and reactor stacks. A command that no decider recognizes is a programming
error, most often a mis-wired command type, and a silent no-op would hide that mistake behind what looks like a
successful call. `executeAndReturnDecision` in particular has no meaningful decision to return when no decider
handled the command, so throwing is the only coherent option there and the other three variants now match it.

This is a different signal from a child decider's criteria function returning `null` during composition, established
in [ADR 52](0052-couple-decider-with-dcb-boundary-and-tags.md). That `null` means "this particular child does not
recognize the command" and is what composition uses to decide which children to skip. It is preserved unchanged and
still drives dispatch inside `compose`. The new behavior only fires when the command reaches the top of the stack
still unrecognized by every decider offered it, which composition's internal `null` signal already had no way to
express on its own.

**`DcbAppendCondition.wholeStoreLock()` is the preferred spelling for a whole-store optimistic lock.** A new factory
method, plus a token-based variant, names the whole-store append-lock case explicitly. Before this, the same effect
was reached by passing `DcbCriteria.all()` into `failIfEventsMatch`, which is the same syntax used to express a
whole-store read. Naming the lock separately from the read stops the two from being interchanged by accident.
`failIfEventsMatch(DcbCriteria.all())` keeps working. It is still a legitimate way to express a single-writer or
bootstrap guard, it is simply no longer the only way to say "lock the whole store," and the new name is preferred
where a whole-store lock is what is meant.

Separately, `DcbDecider.compose` builds its combined read boundary with `DcbCriteria.anyOf` over the children's
boundaries, and `anyOf` short-circuits to a whole-store match if any one child reads the whole store. Composing a
whole-store-reading decider with an otherwise narrowly scoped one therefore downgrades the scoped child's lock to a
whole-store lock for the whole composition. This is documented here rather than guarded against in code, because
whole-store composition can be a deliberate and legitimate choice, and a hard guard would block that legitimate case
along with the accidental one.

**Blocking and reactor retry policies now agree.** The default DCB retry policies on both stacks make five attempts
total, with the same exponential backoff and no jitter. Before this they were both exponential-backoff policies but
did not agree on attempt count, so the same optimistic-lock failure retried a different number of times depending on
which stack handled it.

## Consequences

The three compose combinators now share one guard convention, so a caller who has learned the rule for one of them
already knows it for the others. Callers that were composing zero or one decider, tagger, or DCB decider (found
nowhere in the codebase's own use cases) now get a constructor-time `IllegalArgumentException` instead of a
degenerate but silently accepted object.

Passing a fully unrecognized command to `execute` now fails loudly instead of doing nothing. Existing composed
deciders are unaffected, because their internal `null` handling is untouched. This changes behavior only for the case
that was previously an unannounced no-op, which is the case this decision targets.

`wholeStoreLock()` gives whole-store locking a name distinct from whole-store reading, closing the accidental-mixup
risk between the two. The `anyOf` short-circuit through composition remains a real way to end up with a whole-store
lock without asking for one by name, and this ADR treats that as an accepted, documented property of composition
rather than a defect, on the grounds that whole-store composition is sometimes exactly what is wanted.

Retry behavior is now identical across the blocking and reactor stacks, so a migration between them does not also
change how many times a transient optimistic-lock conflict gets retried.

Two pre-release caveats were considered and deliberately left as is rather than changed in this pass.
`DcbConsistencyToken.value()` stays a `long` rather than being widened or made opaque. `DcbExecuteOptions` widening of
`sideEffect` and `tagGenerator` still relies on independent unchecked casts. Both are recorded here as conscious,
revisitable decisions rather than oversights, since DCB is unreleased and either could still change before the API
locks for real.

This ADR does not introduce new capability. It records a consistency pass over the surface established by
[ADR 47](0047-dcb-criteria-tag-type-and-typed-class-construction.md),
[ADR 48](0048-annotation-driven-dcb-tag-generator.md), and ADR 52, carried out in PRs #293 and #294 while the DCB API
is still free to change without a deprecation cycle.
