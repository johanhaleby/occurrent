# 166. A NOW start asks to replay unless it starts at the present on every start

Date: 2026-11-05

## Status

Accepted. Amends decision 9 of [ADR 132](0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md),
which shipped in 0.34.0 and is therefore amended by reference rather than edited. Part of
[#1404](https://github.com/johanhaleby/occurrent/issues/1404).

## Context

Decision 9 of ADR 132 names a third composition that records with no automatic clear. It can replay and can report its
phase, but is wired so it is never asked to. It says `StartPosition.DEFAULT` with no global start position
set and `NOW` both put a catch-up-capable composition there, "since neither ever asks it to replay". Both starters'
`ProjectionAnnotationRegistrar` built a startup warning on that sentence, for a `recordAppliedAppends = true`
projection. The blocking one logged that recording never resets for every `startAt = NOW`, and for an unset
`startAt` on the MongoDB composition the starter configures. The reactive one logged it for `NOW` and for an unset
`startAt` on that composition.

The sentence is not true of `NOW` with the default `resumeBehavior`. On a restart such a subscription continues from
the checkpoint it stored, and when a catch-up stored that checkpoint, the catch-up replays from it. A projection whose
annotation went from `startAt = BEGINNING` to `startAt = NOW` between two deployments, after a catch-up that did not
finish, replays on the next boot. On the reactive stack an unset `startAt` starts the same way and replays too. The
registrar cannot know which kind of checkpoint a later boot finds, so it cannot promise that the projection never
replays.

On the blocking stack an unset `startAt` on a `@DcbSubscription`, a DCB `@Snapshot` or a DCB projection reaches the
catch-up layer as the model default, so it resumes from such a checkpoint and replays too. On a blocking stream or
agnostic subscription an unset `startAt` does not replay from such a checkpoint. It starts at the present instead, and
misses what was written while the application was down. That is a separate defect, older than 0.35.0, and this ADR
does not change it.

On the blocking stack, `NOW` with `resumeBehavior = SAME_AS_START_AT` keeps no position on Occurrent's durable and
competing consumer models. A restart with a catch-up checkpoint in storage gets only the event written after it
started. A pause and a resume replayed, because the catch-up layer read the checkpoint on the resume. On the reactive
stack a resume with a catch-up checkpoint in storage does not replay from it.

A `SubscriptionResumePoints` bean, new in 0.35.0, can replace the start an annotation built with a checkpoint, `NOW`
and `DEFAULT` alike.

## Decision

Decision 9's third case covers `NOW` only with `resumeBehavior = SAME_AS_START_AT`. `NOW` with the default
`resumeBehavior` is a start that can ask its composition to replay, and it records and clears the way any replaying
start does. On the reactive stack an unset `startAt` is such a start too, and it is no longer in the third case. On the
blocking stack it stays there for a stream or an agnostic projection, since that never replays. A DCB projection
with `startAt` unset is no longer in it.

With no `SubscriptionResumePoints` bean answering for it, `NOW` with `SAME_AS_START_AT` starts at the present on every
restart, and a resume doesn't replay from a catch-up checkpoint in storage. On the blocking stack it bypasses the
catch-up layer as well as the durable and competing consumer ones, so without such a bean no layer reads a position
for it.

Neither `NOW` nor `DEFAULT` belongs to the third case when the application declares a `SubscriptionResumePoints`
bean.

Both registrars' warnings follow this. A push projection with `catchup = NONE` still warns on both stacks, whatever
beans are declared, since the bean doesn't start it. For a projection that subscribes to the event store:

- The blocking one fires for `NOW` only with `SAME_AS_START_AT`, and for an unset `startAt` on a stream or an
  agnostic projection on the composition the starter configures. It doesn't fire while a `SubscriptionResumePoints`
  bean is declared.
- The reactive one fires for `NOW` only with `SAME_AS_START_AT`, and for a composition with no catch-up layer. With
  a `SubscriptionResumePoints` bean declared it fires only for a composition with no catch-up layer, which never
  replays whatever the start.

## Consequences

- A `NOW` projection with the default `resumeBehavior` no longer gets a warning that was false whenever a catch-up
  checkpoint was stored for it. When no such checkpoint is stored it does not replay either, and it now starts without
  the warning that would have been true. The registrar cannot tell the two apart at startup. The same holds for a
  reactive projection, and a blocking DCB projection, with `startAt` unset.
- `ComposedCatchupModel.defaultBypassesCatchup` still records that a first start with `startAt` unset doesn't replay,
  but the reactive warning no longer reads it.
- On the blocking stack the warning for `NOW` with `SAME_AS_START_AT` rests on Occurrent's own models opting out of
  keeping a position. On the reactive stack it rests on tests of the stream composition the starter configures. In both,
  an application-supplied model of another type that keeps a position is not seen by the registrar.
- An application that declares a `SubscriptionResumePoints` bean gets the warning only for a push projection with
  `catchup = NONE` and, on the reactive stack, for a composition with no catch-up layer. That holds even for a
  projection whose resume points never answer a checkpoint. The registrar does not ask the bean, since its answer can
  change from one start to the next.
- If the blocking defect with an unset `startAt` is fixed by resuming from the catch-up checkpoint, that start
  replays, and the blocking warning for it has to go the way the reactive one did.
