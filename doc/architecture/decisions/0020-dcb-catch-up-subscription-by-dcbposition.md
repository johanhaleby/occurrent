# 20. DCB catch-up subscription by dcbposition

Date: 2026-06-21

## Status

Accepted

## Context

`CatchupSubscriptionModel` replays historic events before handing over to a live, position-aware subscription. Its replay reads stream events from `EventStoreQueries` ordered by `(time, streamversion)`, and the live subscription resumes by a database change-stream token. The catch-up phase is gated on a time-based subscription position (ADR 0014).

DCB events are normal CloudEvents stored in the same collection (ADR 0017). They carry a server-assigned, monotonic `dcbposition` and `dcbtags` as CloudEvent extensions. The live change-stream subscription therefore already observes them. What a pure-DCB application cannot do today is rebuild a read model from DCB history: the `subscribeDcb` DSL is live only. It subscribes to all CloudEvents, post-filters by `DcbQuery`, and resumes by the change-stream token. There is no replay ordered by `dcbposition`, so a projection that starts from the beginning of the DCB sequence never sees the events written before it subscribed.

The clean catch-up key for DCB is `dcbposition`. It is monotonic with insertion and server-assigned, so ordering and reconciliation by it are immune to the clock skew that forced ADR 0014 to reconcile the stream delta by insertion order rather than by `time`. It is also immune to the `estimatedDocumentCount` undercount recorded as a negative in ADR 0014, because a position-ranged read never needs a collection count to decide how much to read.

## Decision

Extend `CatchupSubscriptionModel` with a DCB mode, additively. A new constructor takes a `DcbEventStore`, a `DcbQuery`, and the existing `CatchupSubscriptionModelConfig`. The stream constructors and every stream code path are unchanged and remain the default. DCB behavior is reachable only through the new constructor, and a given instance is in exactly one mode.

In DCB mode the catch-up phase replays by `dcbposition` instead of by time:

1. Page through the DCB sequence with position-windowed reads. Starting from the resume position (`0` for a beginning-of-sequence rebuild, or the stored `dcbposition`), read `dcbEventStore.read(query, DcbReadOptions.between(cursor, min(cursor + window, head)))`, deliver the matching events, advance the cursor, and refresh `head` from the returned `lastSequencePosition`. `lastSequencePosition` is the store head at read time, not the maximum matched position, so each read both returns a bounded slice and reports how far the sequence now extends. Memory stays bounded by the position window, which matters because `read` returns a materialized list.
2. Capture the live change-stream position after the bulk replay, exactly as in stream mode, so the resume token stays fresh on a long replay.
3. Reconcile events written during the replay by continuing to page until the head stops advancing. Because `dcbposition` is monotonic, this is a plain range read with no count and no time sort, so it has neither the clock-skew loss nor the count-to-read window that the stream delta has to defend against.
4. Hand over to the live change-stream subscription, deduplicating the seam through the same fixed-size cache the stream path uses.

The position model adds a `DcbSubscriptionPosition` that encodes the `dcbposition` as a self-describing string, so it round-trips through the existing `SubscriptionPositionStorage` and is distinguishable from both a time-based position and a change-stream token. During catch-up the model persists a `DcbSubscriptionPosition`. After handover the live phase persists the change-stream token, as in stream mode. On restart the stored position selects the path: a `DcbSubscriptionPosition` resumes DCB catch-up from `afterSequencePosition`, a change-stream token resumes the live subscription directly, and the existing time-based and default cases are untouched.

In DCB mode the model owns the `DcbQuery` end to end. The replay reads only matching events. On the live path the model post-filters the wrapped subscription's CloudEvents by `DcbCloudEvents.getPosition(event) > 0` and `DcbCloudEvents.matches(event, query)` before delivering, so the consumer sees only matching DCB events in both phases. The `DcbSubscriptions` DSL becomes a thin converter when it is backed by a catch-up-capable model.

The shared handover machinery (capture the live position, start the delegated subscription, deduplicate the seam) is reused by both modes rather than forked.

## Consequences

Positive:

- A pure-DCB application can rebuild a read model from history and resume by `dcbposition`. The Spring Boot starter can wire catch-up in DCB-only mode.
- The DCB delta is reconciled by a monotonic, server-assigned position, so it is immune to the two losses that constrain the stream delta: the clock-skew miss closed in ADR 0014, and the `estimatedDocumentCount` undercount left open as a negative there. This is the robust reconciliation ADR 0014 pointed at (the global position from ADR 0008), realized here as `dcbposition`.
- Position-windowed reads keep replay memory bounded even though `DcbEventStore.read` returns a materialized list, so a large rebuild does not load the whole matched set at once.
- One subscription model serves both stream and DCB, so the durable and competing-consumer wrappers and the position storage are shared.

Negative:

- A DCB-mode instance is bound to a single `DcbQuery` set at construction. Several read models over different queries need several instances. This matches the one-projection-one-query norm and keeps the resume semantics unambiguous, since the stored position belongs to one query.
- The model now has two replay sources, `EventStoreQueries` for stream and `DcbEventStore` for DCB, selected by constructor. The surface grows, but the stream path is untouched and an instance is only ever in one mode.
- The live phase still reads all CloudEvents and post-filters by query, because the change-stream subscription cannot filter by `dcbtags` selectively today. This is the same tradeoff `subscribeDcb` already carries.
- Resume by `dcbposition` assumes the store assigns it monotonically, which it does, with gaps allowed. Gaps are harmless because the range read returns whatever positions exist in the window.
