# 30. Keep MatchAll as a DCB append condition, with its whole-store-lock limit documented

Date: 2026-06-28

## Status

Accepted

## Context

ADR 0021 serializes DCB appends through per-attribute markers. An append increments a marker for each tag and each type of its events and of its condition's query, two appends that can match a common event share at least one marker and so conflict, and a read captures the sum of its query's marker versions as the consistency token, which the conditional append rechecks inside its transaction.

A `MatchAll` query (`DcbQuery.all()`) matches every event, and its only marker is `all`. So a `MatchAll` append condition conflicts with another `MatchAll` append (both touch `all`), but not with a tag-scoped or type-scoped append, which touches only its own tag and type markers and never `all`. A `MatchAll` condition is therefore not skew-safe against a concurrent scoped append: a scoped append can commit a matching event that the `MatchAll` condition never sees.

Making `MatchAll` fully skew-safe would require every append to also touch the `all` marker, which serializes every append in the store on one document. That is the global serialization ADR 0021 set out to avoid, so it is not an option. The open question is whether to keep `MatchAll` as an append condition at all.

## Decision

Keep it, and state the limit loudly rather than removing it.

A `MatchAll` append condition stays correct and useful wherever there is no concurrent scoped append:

- single-writer operations such as a migration, an admin fix-up, or a rebuild guard, where there is no concurrency to skew, and
- an empty-store or bootstrap guard, which is safe even against a concurrent second bootstrapper, because both carry a `MatchAll` condition and so conflict on the `all` marker.

The limit only bites when a `MatchAll` boundary is mixed with concurrent scoped appends on a multi-writer store. Rejecting `MatchAll` in an append condition would forbid the correct uses above to prevent that one misuse, and a correct-and-cheap whole-store lock is impossible as shown above. The proportionate choice is to keep the capability and name the unsafe pattern explicitly at the API: the Javadoc of `DcbQuery.all()` and `DcbAppendCondition.failIfEventsMatch`, and the user documentation.

## Consequences

- `MatchAll` append conditions remain available for single-writer and bootstrap use, where they are correct.
- A user who mixes a `MatchAll` boundary with concurrent scoped appends can still under-protect. This is now stated at the API, not only in this ADR and ADR 0021. It is not enforced in code.
- If the footgun is hit in practice, a later change can reject `MatchAll` in an append condition, making the unsafe pattern unrepresentable, at the cost of the single-writer and bootstrap uses.
