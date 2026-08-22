# 136. A store reports damage it caused and never repairs it by itself

Date: 2026-08-22

## Status

Accepted. Number 0136 allocated at the rel34 plan gate for [#906](https://github.com/johanhaleby/occurrent/issues/906).
0134 is the maximum on `main` and 0135 is taken by the concurrent unit on PR 941.

## Context

Up to and including 0.33.0, `updateEvent` rebuilt the stored document through the stream-only
`OccurrentCloudEventMongoDocumentMapper.convertToDocument`, which routes `position` through the general CloudEvent
extension writer. That writer has overloads for `String`, `OffsetDateTime`, `Integer` and `Boolean`, and `position`
is a `Long`, so it fell through to the string one. The indexed `dcbTags` array is a store-only document field that
the stream mapper does not know about, so it was never written back at all. PR 901 fixed both, on all three MongoDB
stores. It does not touch an event that was already damaged.

A damaged event is invisible, and invisible in a wider way than it first looks. MongoDB brackets comparison
operators by type, so a string `position` matches neither bound of a numeric range. This was measured rather than
reasoned about. Against MongoDB 7, `{position: {$gt: 0, $lte: 100}}` excludes a string `position`, and so does
`{position: {$gt: 0}}` on its own. The common description of this, that BSON sort ordering places strings above
numbers, gives the right answer for the upper bound and the wrong one for the lower.

That exclusion reaches further than DCB reads:

- DCB reads, `exists` and `count`, which additionally require `dcbTags` to exist.
- The conflict query behind a conditional append. An append that should have been refused is accepted instead, which
  is a correctness failure rather than a missing read.
- Position-ordered stream reads, forwards and backwards. A store with stream position on is affected even if it
  never used DCB.
- Position-based catch-up, which reads through those same queries.

None of it reports anything. `PositionBackfill` does not help either, since it looks for events where `position` does
not exist and a damaged event has the field.

So there are two questions. Who repairs the damage, and how does anyone find out they have it.

## Decision

### 1. A store never repairs stored events by itself

The repair is an opt-in offline tool, `occurrent-eventstore-mongodb-update-event-repair`, alongside the existing
`position-backfill` module. A store writes nothing to fix history at startup, at read time, or anywhere else.

Automatic repair at startup was rejected on the risk it brings. It would change a user's stored events without being
asked, in a database this project does not run and cannot observe. Some of the damage cannot be repaired safely at
all, and a duplicate key rejection would then surface inside the startup path of a running service. Finding events
with a missing `dcbTags` array also needs a collection scan, so an automatic repair means scanning the whole
collection every time an application boots.

Read-time repair was rejected on mechanism rather than on cost. A damaged event is missing from exactly the queries
that would reach it, so a read never sees one. Repairing at read time would need its own scan, which is the startup
option again with more steps.

### 2. A store does detect the damage, and says so

On startup, a position-writing store looks for one event whose `position` is a string and logs a warning naming the
repair when it finds one. Detection is not repair. It writes nothing and it changes no read.

The cost of a manual tool is that nobody runs it unless they know they need to, and this is what pays that cost
down. The check is free on a healthy store: `{position: {$type: "string"}}` is an index range restricted to the
string type, and against MongoDB 7 with the same unique sparse `position` index the stores create, a collection with
no damage examined zero index keys and zero documents.

The `dcbTags` half stays out of startup. Looking for a document that holds the `dcbtags` extension without the
array derived from it cannot use an index, and measured against a healthy 20000 event collection it read all 20000
documents. That check belongs in the offline tool, which is allowed to take its time.

Together the two checks cover every damaged event. Before PR 901, whenever the updated event still had a
position, the write-back always turned it into a string, so anything that kept a position trips the new check.
Anything that lost its position entirely has no `position` field and trips the existing un-backfilled events check,
which already warns today, although it names the wrong remedy. The upgrade guide says so.

### 3. The warning cannot be escalated to a startup failure

`requireBackfilledPosition` exists for the un-backfilled case and there is deliberately no equivalent here.

Un-backfilled history is an ongoing condition. A store keeps writing new positioned events alongside old
un-positioned ones, and refusing to start protects an operator from running that way indefinitely. This damage is
different. It is finite and already done, no new event can acquire it now that PR 901 has shipped, and a one-off
repair ends it. Refusing to start would take an application down over history rather than protect anything still
being written.

### 4. This is not the un-backfilled position check wearing a different hat

The two conditions are mechanically similar and are kept apart on purpose. `PositionBackfillValidator` says a store
predates position. `UpdateEventRepairValidator` says a store's events were damaged by a defect this project shipped.
Different cause, different remedy, different thing for an operator to do next, so separate wording and separate
checks rather than a widened predicate on the existing one.

### 5. The repair restores what survived and reports the rest

The tool rebuilds `position` from the string still in the document and rebuilds `dcbTags` from the `dcbtags`
CloudEvent extension, which is a genuine string and so came through the coercion intact. It reuses the store's own
`PositionDocumentMapper` and `DcbCloudEvents.decodeTags`, so a repaired event is what a running store would write,
including an empty tag set rebuilding to an empty array rather than an array holding one empty string.

It is not a general recovery, and it does not present itself as one. Three kinds of damage survive it, reported per
event by `_id` rather than guessed at:

- A position that was dropped entirely. An update function returning an event built from scratch had no position
  extension, so nothing stored it. Assigning a fresh one in `_id` order would look plausible and be wrong, because a
  consumer holding a checkpoint from before the damage would then disagree with the store.
- A position another event already holds as a number, which the unique index refuses. Two events claim one position
  and nothing in either document says which is entitled to it.
- A `position` string that is not a number, which no known path produces and so points somewhere else.

One kind cannot even be seen. `preserveTags` did not exist before PR 901, so an update function that returned a
replacement event without the `dcbtags` extension left a document that no longer looks like a DCB event at all, and
nothing distinguishes it from an ordinary stream event. Where `dcbtags` was replaced rather than dropped, the repair
faithfully rebuilds the array from the wrong tags. The upgrade guide states both plainly.

### 6. When this check may be removed

This is the reason the decision is recorded rather than left in a module README.

The check exists for one closed window, meaning events written by 0.33.0 or earlier through `updateEvent`, on a store that
has not yet been repaired. No version from 0.34.0 onwards can create a damaged event. The check may be removed once
upgrades directly from 0.33.0 or earlier stop being supported, which is a deliberate decision about supported
upgrade paths and not something to infer from the check looking old. Until that decision is taken, a maintainer
finding this code should leave it alone. Removing it early costs nothing visible and silently takes away the only
signal an affected store ever gets.

## Consequences

Three MongoDB stores gain one indexed lookup at startup, on the path that already runs the un-backfilled events
check. On a healthy store it reads nothing.

An affected operator learns about the damage from a log line and runs a tool. Nobody's stored events change without
them asking, which is the property worth having, because the damage is inert and a wrong repair is not.

A store that was damaged and then repaired keeps warning until the repair actually runs, since the check reads the
data rather than a marker. A partly finished repair narrows the warning's cause but does not silence it, which is
the honest behaviour.

An operator whose damage falls in the unrecoverable cases gets a list of event ids and no automatic answer. That is
worse than a fix and better than a store that quietly invented positions.

## Rejected alternatives

**Widen `PositionBackfill` to also repair this.** Its predicate, its runbook and its upgrade sequence are about
turning position on for a store that predates it. The two jobs share a collection and nothing else, and merging them
would mean one tool whose README has to explain two unrelated reasons to run it.

**Fail startup by default when damage is found.** It makes an upgrade to 0.34.0 unbootable for exactly the people
already harmed by the defect, and it protects nothing, since the damage cannot grow.

**Repair with an aggregation pipeline using `$toLong` and `$split`.** It would run server-side in one command, but it
reimplements the tag encoding in a second place, has to special-case the empty tag set to avoid producing an array
holding one empty string, and ties the tool to a server version. Reading each document and writing it back through
the store's own mappers keeps one definition of what a stored event looks like.
