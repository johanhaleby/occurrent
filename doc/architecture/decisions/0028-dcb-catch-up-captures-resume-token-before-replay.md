# 28. DCB catch-up captures the live resume token before the bulk replay

Date: 2026-06-28

## Status

Accepted

## Context

ADR 0020 gave the DCB catch-up its shape: replay the matching history by `dcbposition`, then hand over to the live change stream, resuming it from a token captured after the replay so the token stays fresh across a long rebuild.

ADR 0021 then moved `dcbposition` assignment outside the append commit transaction. A consequence is that the store head can run ahead of committed data: a position below the head can be an in-flight hole whose append has reserved the position but not yet committed. The append's own write path is immune to this because it serializes on per-attribute markers rather than positions, but the catch-up replay still reads by position.

Those two combine into a delivery gap. Suppose a matching event holds a below-head position and is still in flight when the replay scans its window, then commits while the replay is still running. The replay does not see it (it was uncommitted when scanned). The reconciliation loop only scans forward from the head, so it never revisits a below-head position. And the live change stream resumes from a token captured after the replay, by which time the event has already committed, so the token sits past it. The event is delivered by none of the three paths and is silently lost. This breaks the at-least-once contract: a read model rebuilt during concurrent writes can be permanently missing an event.

Position-ordered reconciliation cannot close this on its own. A below-head hole is old by position, not new, so neither a forward scan nor a newest-N count scan finds it without re-reading the whole matched range, which is unbounded for a selective query over a large store. Only a commit-ordered observer can recover it, which is exactly what the change stream is.

## Decision

Capture the live resume token before the bulk replay instead of after.

With the token taken before the replay, an event that commits during the replay falls after the token, so the live change stream carries it on handover regardless of its position. The existing handover cache dedups the overlap between the replay and the live phase.

## Consequences

Positive:

- The at-least-once contract holds under concurrent writes during a rebuild. A below-head event that commits mid-replay is delivered by the live phase rather than dropped.
- The change is localized to where the token is captured. The replay, reconciliation, cache, and handover are otherwise unchanged.

Negative:

- The token can age out of the change stream history on a rebuild that runs longer than that history, because it is now captured earlier. When that happens the handover resume fails loudly rather than silently dropping events. Size the change stream history (the MongoDB oplog window) for very large rebuilds. A loud failure that a restart recovers from is strictly better than a silent loss that a rebuild does not.
- The live phase re-delivers events written during the replay, deduped by the cache. The duplicate volume is bounded by the writes during the replay, not by the store size, because the historical backlog committed before the token and so is not in the live stream. Delivery is at-least-once, so duplicates are within contract.

Considered and not taken: starting the live subscription before the replay and buffering its events until handover, so the cursor stays active and never ages out. That removes the oplog-window caveat entirely, but it is a large rewrite of the handover and its position-storage and lifecycle handling, for a rare edge. Capturing the token early restores correctness with a contained change, and the buffering design remains the upgrade path if the oplog-window caveat ever bites in practice.
