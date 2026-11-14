# 176. A plain DCB read through a mongos reads in a causally consistent session of its own

Date: 2026-11-14

## Status

Accepted. Resolves [#1479](https://github.com/johanhaleby/occurrent/issues/1479). Amends
[ADR 149](0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md) and
[ADR 175](0175-a-conditional-dcb-append-through-a-mongos-queries-the-events-in-a-snapshot-of-its-own.md) on what a
plain read through a mongos can miss.

## Context

A plain read is a DCB read without `withVersions()`. It reads the markers of its criteria, which become the consistency
token, then the position counter, then the events. Each of the three is its own operation, and through a mongos nothing
ties them to one point in time.

Take an event E that matches the criteria, whose append has committed on the shard that holds its markers and is still
in flight on the shard that holds the events. A plain read in between gets a token with E's markers and no E. A
conditional append with that token then sees no marker that moved, so the check of ADR 149 and ADR 175 passes it
although E matches. This holds for criteria with tags, and for `DcbCriteria.all()` when E was appended with an `all()`
condition, which increments the `all` marker.

A probe on MongoDB 8.0 with two shards held E's commit on the events' shard while E's marker was visible on the other
shard. In each of three runs:

- The plain read returned a token with E's marker at version 1 and no events.
- A causally consistent session that read E's marker and then the events waited more than 3 seconds while E was held,
  at read concern local and at majority, and returned E 0.19 to 0.35 seconds after the hold was released.
- The same events query with no session returned no events at once.
- With nothing held, the read in a causally consistent session took about 3 ms at read concern local and about 98 ms at
  majority.

In a causally consistent session, every read after the first sends an `afterClusterTime`, the cluster time the
session's previous operation saw, and a shard waits for a transaction still prepared below that time before it
answers. The probe's timings fit that, but it didn't check that E was in the prepared state while held.

ADR 175 considered reading the token and the events in one snapshot transaction, and left #1479 to be decided on its own.

## Decision

Through a mongos, a plain read that runs outside any session of the caller's reads the markers, the counter and the
events in one causally consistent `ClientSession` of the store's own, in that order, and closes the session afterwards.
The events read then sends an `afterClusterTime` at or above the markers read, so when the markers read saw a marker
E incremented, the events' shard waits for E's commit before it answers. The probe showed that at the primary at read
concern local and majority, and the tests show it at the primary with no read concern or read preference set.

What counts as outside a session of the caller's differs per store.

| Store | Reads in a session of its own when |
| --- | --- |
| `MongoEventStore` | No session is bound in `ClientSessionHolder`. `NativeMongoTransactionExecutor` binds one. With one bound, the read takes the markers and the position counter outside that session and only the events in it, as in 0.34.0. |
| `SpringMongoEventStore` | Transaction synchronization isn't active. With it active, the template reads in the session bound to it, or binds one, since the store sets session synchronization `ALWAYS` on the template. |
| `ReactorMongoEventStore` | No caller transaction is active and the reactive transaction synchronization is inactive. The session is closed when the read's `Mono` completes, fails or is cancelled. |

The Spring and reactor stores take the session from the template's database factory and read through a copy of the
template bound to it, with session synchronization `NEVER`, as ADR 175's snapshot transaction does.

In the reactor store, a template whose `withSession` returns something other than a `ReactiveMongoTemplate` gives the
store no copy it can set `NEVER` on. With synchronization inactive, nothing binds another session, so the read goes
through what `withSession` returned and stays in the causally consistent session. With synchronization active and no
session bound, which only the fallback of a read with `withVersions()` reaches, the read fails with an
`IllegalStateException`, since without `NEVER` the synchronization would bind another session to it.
`ReactiveMongoTemplate.withSession` in spring-data-mongodb 5.1.0 returns a `ReactiveMongoTemplate`, so only a subclass
that overrides it takes either path. A test with such a subclass covers each.

A read with `withVersions()` that falls back to a plain read takes the same path when it runs outside a session of the
caller's. That's a read from a store set up with `resumeFromMarkerVersions(false)`, a read of `DcbCriteria.all()`, and
a read whose snapshot transaction failed and that passed no saved versions. In the Spring and reactor stores, the
fallback that already read in a session of its own, so as not to bind a session to a Spring scope with no session bound
yet, now asks for a causally consistent session too. The MongoDB driver already made a session causally consistent when its options didn't
say, so that read doesn't change.

The rest stays as it was.

- In the Spring and reactor stores, a read in a caller's transaction or in a session already bound reads the markers,
  the counter and the events in that session. So does a read in a transaction `SpringTransactionExecutor` or
  `SpringReactiveTransactionExecutor` opened.
- In `MongoEventStore`, a read with a session bound in `ClientSessionHolder`, which `NativeMongoTransactionExecutor`
  binds, reads the markers and the counter outside that session and only the events in it.
- A read with `withVersions()` whose stream reports `markerVersions()` keeps its own snapshot transaction.
- On a replica set and on every other deployment, the read is the same as before.
- There's no new setting and no way to turn this off. Each read keeps the read concern the store used.
- `MongoEventStore` reads through the same collections as before, so it keeps their read preference. `withSession`
  doesn't copy the read preference set on a Spring template, so the Spring and reactor stores set it on the copy bound
  to the session. There are two exceptions in the reactor store. With synchronization active and no session bound,
  which only the fallback of a read with `withVersions()` reaches, asking the template for its read preference would
  bind a session, so that read runs at the database's read preference, as it did before this decision. A template
  whose `withSession` returns something other than a `ReactiveMongoTemplate` gives the store no copy to set it on, so
  the read runs at whatever read preference that object applies. The probe and the tests that hold E's commit read at
  the primary, so I don't know whether the events read waits for E when a read preference sends it to a secondary.
- The native catch-up read scopes already read the token, the head and the events in one causally consistent session,
  and don't change.

## Consequences

- Through a mongos, at the default read concern and read preference, when the markers read of a plain read outside a
  caller's session sees a marker E incremented, the read returns E. A test on two shards for each store holds E's
  commit on the events' shard, starts the read, releases the hold after up to 2 seconds and asserts that the read
  waited and returns E. It does so for criteria whose only tag marker is on the first shard, for `DcbCriteria.all()`
  after E was appended with an `all()` condition, and for criteria with a tag marker on each shard. All three returned no events before this change.
- A read concern or read preference set on the client, the database, the template or in a Spring store's query options
  isn't covered, and the read may still miss E there. The Spring stores apply their query options to the events query
  and not to the markers query, as 0.34.0 did, so a `Query` there can read the events at another read concern or read
  preference than the markers. That gap is older than this decision and has an issue of its own.
- The token isn't one point in time. The markers read is the session's first operation and sends no
  `afterClusterTime`, so a marker on the events' shard can be from before E. The test with a tag marker on each shard
  asserts that the read returns E with `course:c1` at version 1 and `student:s1` at version 0.
- A conditional append through a mongos with such a token is refused, although the read returned E.
  `DcbAppendConditionCheck` sees the marker from before E as moved and asks the events above it on that key, where it
  finds E. That's the torn token ADR 175 refuses. The test with a tag marker on each shard then appends with that
  token and asserts the refusal. With the check changed to pass whenever a marker moved, that test fails on its
  refusal assertion in all three stores.
- A plain read through a mongos outside a caller's session can now take as long as a commit in flight on the events'
  shard. In the probe it waited for as long as the commit was held. The store sets no bound on that wait, and I didn't
  check whether a client `timeoutMS` cuts it short. With nothing held, the probe's read in a causally
  consistent session took about 98 ms at read concern majority and about 3 ms at local. The probe didn't time the same
  read without a session, so I don't know how much of the 98 ms is new.
- Two holes remain, and this decision doesn't cover them. In the Spring and reactor stores, a read in a caller's
  transaction or in a session already bound, which includes the session the template binds while transaction
  synchronization is active, reads in that session as before. In `MongoEventStore`, a read with a session bound in
  `ClientSessionHolder` reads the markers and the counter outside that session and only the events in it, as 0.34.0
  did. No test checks whether either read can miss E, so I don't know whether it can.
- `withVersions()` isn't one of the holes. A stream that reports `markerVersions()` came from its snapshot
  transaction.
- ADR 175 lists a two-shard test of a token read while a matching event had committed on one shard and not the other.
  That test asserted the torn read as its setup, so it's now the test with a tag marker on each shard.

## Alternatives considered

**Read the token, the counter and the events in one snapshot transaction.** ADR 175 considered this. It would also make
the token one point in time. It costs a read-only transaction with a majority commit on every plain read through a
mongos, and the read would need an answer for a snapshot transaction that fails, as `withVersions()` has. No probe
measured it.

**Leave the read as it was.** A plain read through a mongos would keep returning a token with E's markers and no E,
and a conditional append with that token would keep passing although E matches. ADR 149 and ADR 175 recorded that as
open.
