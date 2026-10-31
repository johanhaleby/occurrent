# 149. The MongoDB DCB append check compares each marker, and then the events above it

Date: 2026-10-30

## Status

Accepted. Decides [#1231](https://github.com/johanhaleby/occurrent/issues/1231) and
[#1300](https://github.com/johanhaleby/occurrent/issues/1300). Amends
[ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) on how the append check compares a token and on pruning
markers, [ADR 88](0088-what-a-dcb-append-condition-guarantees.md) on what the MongoDB stores answer for an excluded
type, [ADR 147](0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md) on what the token holds, and
[ADR 31](0031-capture-dcb-consistency-token-in-a-single-read.md) on what one read of the markers gives.
Builds on the marker versions of [ADR 148](0148-dcb-snapshots-resume-from-marker-versions-not-positions.md).

The second part of [#1229](https://github.com/johanhaleby/occurrent/issues/1229), splitting each type marker into
several documents, is decided in [ADR 162](0162-a-dcb-type-marker-is-split-into-stripes.md). The check below works
on whatever marker keys a criteria has, so it holds for those documents too.

## Context

A conditional DCB append on the three MongoDB stores compares one number. The read sums the versions of the marker
documents of its criteria into the token, and the append sums them again inside its transaction and refuses when the
two sums differ (`MongoEventStore.java:708`, `SpringMongoEventStore.java:829`, `ReactorMongoEventStore.java:584`). That
sum has two problems.

**It moves for appends that don't match the criteria** (#1231). After ADR 147, the markers of an item with tags are
its `tag:` markers, and every append increments a marker for every tag of its events. So the check refuses in three
cases where no matching event was committed after the read:

- **Another type with the same tag.** The criteria is `type("TicketSold").tags(concert:k1)`, and a `ConcertRenamed`
  tagged `concert:k1` is committed after the read.
- **Only some of the item's tags.** The criteria is `type("StudentEnrolledInCourse").tags(course:c1, student:s1)`,
  and an enrolment of `s2` in `c1` is committed after the read.
- **An excluded type with the same tag.** The criteria excludes `CourseRenamed`, and a `CourseRenamed` tagged
  `course:c1` is committed after the read.

The in-memory store doesn't refuse in any of them, because it compares the events committed after the token with the
criteria. The issue's own idea, an exact query for matching events above the read position, isn't sound on MongoDB. A
store reserves positions before the append transaction starts (`MongoEventStore.java:642`), so an event can commit
below a position a reader has already seen. ADR 148 keeps that, and reserving inside the transaction would make every
append in the store conflict with every other one (ADR 148, design 3).

**It doesn't notice that a marker was pruned** (#1300). ADR 21 lets an operator delete marker documents during
quiescence and calls that harmless, and so do the comments above `incrementConflictMarkers` in the native and Spring
stores. A token can be held much longer than a quiet moment, for example by a command waiting in a queue. Say a command reads
`tags(a)` while `tag:a` is at version 1. The marker is deleted, another command appends an event tagged `a`, and the
upsert creates `tag:a` again at version 1. The first command then appends with its token, and the sums are equal. A
prune can also lower one marker while another goes up, so two different states of the markers can give the same sum.

Since ADR 148, a 0.35 store keeps three things that change this. Every marker document has an `epoch`, which is new
each time the marker is created. Every marker has a counter `s` of the increments that wrote marker versions on their
events, so `version - s` (called u below) counts the increments of a writer that wrote none. And every event a 0.35
writer appends has an `mv` array, with the key, the epoch and the version its transaction gave each of its markers.
ADR 148 also showed that, per key, marker versions follow commit order. Two transactions can't both increment one
marker, because MongoDB aborts one of them with a write conflict, and the one that retries reads the other's commit.

## Decision

### The token holds what the read saw of each marker

`DcbConsistencyToken` gets a third field. It holds, for each marker key of the read's criteria, the marker's epoch,
its version and its u, as the read saw them. A marker the read didn't find is recorded with no epoch, version 0 and u 0.

- `DcbConsistencyToken.of(DcbCriteria, long, Map<String, DcbConsistencyToken.MarkerObservation>)` is added, with
  `record MarkerObservation(@Nullable String epoch, long version, long unstamped)`.
- `markers()` returns the map, which is empty for a token made with `of(DcbCriteria, long)`.
- Two tokens are equal when their criteria are the same, their versions are equal and their maps are equal.

Like `version()`, the map is for store implementations. ADR 147 made the token a final class so that a field could
be added this way. A `DcbEventStore` of your own can keep using `of(DcbCriteria, long)`, and a MongoDB store refuses a
token made that way, as the next section says. The MongoDB stores keep the sum in `version()`.
`MarkerObservation` replaces the stores' internal `ObservedMarker`. It can't be `DcbMarkerVersions.MarkerVersion`,
which requires an epoch and a version of at least 1, because a token also has to hold a marker that was missing or
had no epoch.

Every MongoDB read already loads the marker documents to sum them (`MongoEventStore.java:764-782`), and the marker
documents already hold the epoch and `s`. So the map costs no extra read. Each store fills it on every read path,
the one that asks for versions, the one that doesn't and the catch-up read scope.

### The check compares each marker

Inside the append transaction, the store reads the condition's markers as it does today and compares each one with
the token's map, never the sums. A token whose map doesn't hold exactly the condition's marker keys wasn't read
by this store, and the append throws `IllegalArgumentException`. That includes the empty map of a token made with
`of(DcbCriteria, long)`.

1. When a marker the token saw has another epoch, or is gone, or has a lower version, the check refuses. That's a
   prune or a recreated marker, and it's #1300's fix. A marker the read didn't find isn't one the token saw, so that
   marker existing now is no reason to refuse.
2. When every marker has the token's version, the check passes, as it does today.
3. Otherwise some markers moved. On anything but a replica set the check refuses here, for the reason in the section
   on sharded clusters below. On a replica set it skips an item that can't have a matching event committed after the
   read.
   That's an item with tags that has a marker that didn't move, because such an event increments every tag marker of
   the item (ADR 147, I2). It's also an item without tags none of whose markers moved, because such an event increments
   one of them.
4. For every other item the check needs the query in the next section. When it can't run that query, it refuses.

Steps 1 to 3 need nothing but the markers, so they also work for a marker a 0.34 writer incremented. Step 3 removes
the second case of #1231 whenever the other append lacks one of the item's tags. It also keeps an untouched item out of the query, such as `type(X)` in `anyOf(type(X), tags(a, b))` when only
`a` moved.

### The check queries the events above the versions the read saw

For each item step 3 didn't skip, the store queries, inside the same transaction, for an event that matches the item
and has an `mv` element with a selection key of the item, the epoch the token saw and a version above it:

- For an item with tags, the selection key is the one a read after marker versions uses (ADR 148), the item's first
  tag key in canonical order. Every matching event has every tag of the item, so one key finds all of them.
- For an item without tags, every type key whose marker moved is a selection key, one branch each. A marker that
  didn't move holds no such event, and one that doesn't exist has no epoch to select on.

The query uses the `{mv.k, mv.e, mv.v}` index ADR 148 created, runs on the transaction's session, takes the first
match and doesn't apply the Spring stores' `queryOptions`. A match refuses the append, and no match passes it.

On a replica set, the store runs the query only when both of these hold, and refuses otherwise:

- The token saw the marker with an epoch, or didn't see it at all. A marker that existed without an epoch at the read
  was last written by 0.34, and its events may have no `mv`.
- The marker's u is the token's u. When u moved, a writer that writes no `mv` incremented the marker since the read,
  and its event can't be found by `mv`.

The query doesn't depend on `resumeFromMarkerVersions` (ADR 148). That setting is for deployments that can't run the
read-only transaction a snapshot read needs, and the check runs inside the append's own transaction. Every append
writes `mv`, `s` and the epoch whatever the setting says.

A key the token didn't see gets version 0 and the marker's current epoch. The marker didn't exist at the read, so
that epoch was created after it, and every `mv` element with that epoch is newer than the read.

`DcbCriteria.all()` has no selection key, so a `wholeStoreLock` condition keeps steps 1 and 2 and refuses whenever
the `all` marker moved.

### On a sharded cluster the check refuses when a marker moved

Two things stop working through a mongos. A transaction at read concern local or majority opens a snapshot on each shard
when it first touches it, and MongoDB doesn't promise that these come from the same point in time, so the query could
read the events from before the markers the check just read. And a plain read isn't one snapshot either, so a token can
hold one marker from after a matching event E and another from before it, while the read misses E. Step 3 would pass
that token, because the marker from after E didn't move, and the sum check would refuse it. A probe on two shards with
`_dcb_checkpoints` range-sharded on `_id` showed this, with a fail point holding E's commit on one shard. Natural timing
didn't reproduce it in 1600 iterations.

So on anything but a replica set, steps 1 and 2 still apply, and any marker that moved refuses the append. That refuses
everything the sum check refused. It doesn't refuse everything 0.34 refused, because a token no longer holds the type
markers of an item with tags (ADR 147). An event of type T with another tag moved a marker of a 0.34 read of
`type(T).tags(a)`, and it moves no marker of this one.

The store asks the server once, when the store is created, with a `hello` command. A server too old to know `hello`,
such as MongoDB 4.2.8, answers with error code 59, and the store then uses `isMaster`. All three stores apply the same
test to the answer. The store counts as running on a replica set only when the answer has a `setName` and its `msg`
isn't `isdbgrid`. A mongos, a load balancer in front of one and a standalone server don't count. Any other failure of
the command makes creating the store fail, as the store's own initialization already does when it can't reach the
server. A wrong "no" costs a refusal the sum check would also make, and a wrong "yes" would let a matching append
through.

This doesn't make a plain read through a mongos safe. With the markers on one shard and the events on another, a token
can hold E's markers while the read misses E, and then no marker moves and the append passes. The sum check has the same
hole, so it isn't new. A read with `withVersions()` whose stream reports versions ran in a snapshot transaction, and in the
same probe it blocked during the window and the append was refused. A stream that reports none may have come from a
plain read, and then has the hole. In that probe, holding E's commit for 9 seconds made the read give up on its snapshot
after 5 seconds, report no versions and let the append pass. ADR 148 has the other reasons a read reports none.

### Why a matching event can't get through

Take a matching event E committed after the read loaded the markers, which it does before it loads the events
(ADR 31). Per key, E's append gave every marker of E a version above the one the token holds. There are three cases:

- E's append committed before the append transaction's snapshot. If its markers kept their epoch and the u counters
  didn't move, E has an `mv` element for the selection key with the token's epoch and a version above the token's, so
  the query finds it. If an epoch or u changed, steps 1 and 4 refuse.
- E's append commits after that snapshot. It increments every tag marker of an item it matches, or a type marker of
  an item without tags that the item's markers include. The conditional append increments the same markers from its criteria, so
  MongoDB lets only one of them commit. A conditional append that loses retries its transaction and reads E's commit.
- E was deleted before the check. The query doesn't find it and the append passes, the way the in-memory store passes
  it. The check may still refuse, for example through step 1 after the marker was pruned. Refusing for an event that
  no longer exists is allowed and isn't required.

E's position never enters the argument, so it holds while positions are reserved outside the transaction, a lost reservation
included. The fifth assumption below covers an E the token already holds.

The first two cases need the transaction to read the markers and the events from one snapshot. On a replica set,
MongoDB's documentation says only that a transaction at read concern local "might observe" a snapshot taken when it
opened. A probe on a single-node replica set at the D2 branch saw one snapshot across both collections. That's the
assumption, and on a sharded cluster the store doesn't make it.

The markers of a plain read aren't one snapshot on a replica set either, since a find can yield between documents. A
probe that forced a yield got a token with one marker from before E and one from after it, and step 3 can pass that token
where the sum check refused it. That's sound when the read's events hold E, because the caller then decides with E in
hand. A read of the whole boundary from the primary at read concern local holds E, because it takes the position and the
events after the token. The assumptions below say when a read doesn't.

The argument also assumes the following. First, nothing replaces the document of an event a 0.35 node wrote, which ADR
148 also needs. The sum check doesn't read events, so this is new for an ordinary conditional append, also with
`resumeFromMarkerVersions(false)`. A 0.34 `updateEvent` during a rolling upgrade can remove an event's `mv`, and the
query then passes a token that the sum check refused. Second, a marker version a token holds isn't rolled back and then
given to another append under the same epoch. With no tear the sum check has the same hole, and a torn token can pass
here where the sum refused it. A plain read takes the token at the collection's default read concern, so a failover that
rolls back an increment written at w:1 can break that. So can a token read inside a transaction after that transaction's
own append, when the transaction then rolls back. Third, a read's events aren't behind its markers, by member or by read
concern. The Spring and reactor stores apply `queryOptions` to the events read and not to the marker read, so
`withReadPreference(secondary())` breaks this, and so does read concern majority with the secondaries stalled. A native
store built on a collection with its own read preference does the same, and it reads a token outside a caller's session
and the events inside it. In a probe on the Spring store, a torn marker read on the primary and a secondary that hadn't
applied E passed a token the sum check refused. With no tear these reads keep the sum equal, so the sum check has the
same hole, and the tear is a rarer path beside it. Fourth, the read returned the whole boundary. A read with `limit`,
`skip` or a position bound may not return E, and in a probe a torn token from a read with `limit`, `skip` or
`upToPosition` passed where the sum check refused it. With no tear the same read passes on the sum too, because the
token doesn't depend on the window, as `DcbReadOptions` says. Fifth, no position reservation was lost. A store reserves
an append's positions with an update of the counter. The native store runs the update outside any transaction, also when
the append joins a caller's transaction. The Spring and reactor stores do when they run the transaction themselves.
Outside a transaction the update waits for write concern majority. It takes the write concern of the database the store
uses, which falls back to the client's, and sets `w` to majority on it, so a w:1 on either of those or as the server's
default doesn't apply to this update, and a `wtimeout` on them does. A write concern set on a Spring template doesn't
reach it, since the update goes to the collection. Below majority, a failover can lose that update while the append
commits E above the counter. Every read stops at the counter, a read with `withVersions()` too, so a read can hold E's
markers and miss
E, and a read after that read's versions doesn't return E. A full read, and a read after versions saved before E, misses
E until the counter reaches it. An append that reserves E's position and gets past its condition fails with
`DuplicateCloudEventException`, whatever its boundary. A conditional append on the token that reserves another position
can pass. In a probe it passed when a reservation below E's was lost too, and on the native store when an unrelated
append took E's position first. With no tear the sum is equal, so the sum check passes the same append, unless a later
event moved a marker of the criteria. The sum refuses then, and this check passes when that event doesn't match. On the
Spring and reactor stores, the update in a caller's transaction joins it and commits with the event, so a failover can't
lose it alone. An operation inside a transaction can't set a write concern of its own, so this path stays as it was. A
stepdown on both stores, with that transaction at majority and at w:1, kept E and the counter together,
or lost both when a w:1 commit was rolled back. The exception is a store whose own transaction manager uses the same
database factory as the caller's and whose transaction is set to `REQUIRES_NEW` or `NOT_SUPPORTED`. The update joins the
caller's transaction and E commits apart from it, so in a probe on both stores a caller's rollback left E above the
counter with no failover. An append in a caller's transaction that reserves E's position after another append lost its
reservation rolls its own back with the duplicate, so in a probe that set the counter by hand such appends kept failing
at E's position until an append in the store's own transaction moved the counter. The probes of the loss itself ran
before the update waited for majority, and used a stepdown, and a raw `$inc` standing in for the reservation below E's.
All three stores lost the update at w:1 on three members, and the native store did at w:2 on five members and on a
primary, a secondary and an arbiter with no write concern set. The Spring store did on three data-bearing members with
no write concern on the client and w:1 as the server's default. At w:majority, and on three data-bearing members with no
write concern set and the server's implicit default, all three stores' reads held E. A test on each store now steps the
primary down with an append's update not yet replicated and a client at w:1, and a read finds E. That test fails without
the wait. No failover probe ran through a mongos.

On a replica set of a primary, a secondary and an arbiter with the secondary down, a majority can't acknowledge the
update, where appends used to complete in 6 to 18 ms. In a probe on all three stores, an append with no timeout was
still waiting after 25 seconds and completed when the secondary came back. `wtimeoutMS=2000` and `timeoutMS=2000` on the
connection string failed it after 2 seconds, and `socketTimeoutMS=2000` after 4. On the Spring and reactor stores a
write concern with a `wtimeout` on the database factory failed it after 2 seconds too, and one on a template didn't, for
the reason above. An append that fails this way writes no event, and the positions it reserved can stay unused.

### Pruning markers

ADR 21's pruning rule changes. A marker may be deleted only in a transaction that also finds no event with its tag
or type. For a type, that covers every stripe of ADR 162. An append that increments the marker at the same time
writes the same document, so one of the two transactions aborts. Deleting a marker while one of its events exists
isn't supported. ADR 21 and the two store comments that call pruning during quiescence harmless change to this rule.

Two conditions come with it. On a sharded cluster the transaction runs at read concern snapshot. At local or majority
it can read an empty event shard, and then delete the marker on another shard at a later point in time, so an append
that commits between the two conflicts with nothing. And no 0.34 node may append while markers are deleted. A 0.34 node
recreates a marker with no epoch, starting again at version 1. A token that saw the old marker with no epoch can't tell
the two apart once the new one is at the token's version.

With that rule, a marker the token didn't see and that is still missing at the check has no event committed after the
read that still exists. Without it, an operator could delete the marker of an event that still exists, and a token taken
before that event's append would pass. So #1300 is closed for every marker the token saw, and for a marker the token
didn't see it's closed as long as the rule is followed. With one tag the sum check passes that case too. With several
tags the sum check can refuse it, when another marker of the criteria moved, and this check can pass it. Occurrent
doesn't prune markers itself and doesn't ship a tool that does.

## Alternatives considered

**An exact query above the read position** (the issue's idea). It misses an event that reserved its position before
the read and committed after it. That's the defect ADR 21 replaced positions with markers for.

**Reserve positions inside the append transaction**, so the position query becomes sound. Every append in the store
would then write one counter document inside its transaction and abort with a write conflict when another append does
the same. That's store-wide write contention, which ADR 21 removed and ADR 148 rejected again.

**A marker for each tag and type pair**, such as `tag:concert:k1@TicketSold`, incremented next to `tag:concert:k1`.
It removes the first case of #1231 without a query, even for two appends that run at the same time. It doesn't
remove the third case, it adds a marker write per tag to every append, and it changes which markers an append
increments. During a rolling upgrade, a 0.34 append of a `TicketSold` tagged `concert:k1` doesn't increment the new
marker, so a 0.35 condition that reads only that marker lets a matching append through. The query above has no such
gap, because a 0.34 append moves u.

**Run the append transaction at read concern `snapshot`**, so the query reads one point in time across shards. MongoDB's
documentation says that read concern gives its guarantee only when the transaction commits at write concern majority.
The store would have to override the read concern a user configured for their writes. The native store could set it on
its transaction options. The Spring and reactor stores couldn't set it through the `TransactionTemplate` or
`TransactionalOperator` they're given, because those options live on the transaction manager, so a user would have to
set it themselves. A snapshot alone also wouldn't make the query sound. A plain read's token can be torn across shards,
and the query selects an item with tags on its first tag key only, so a first tag marker from after E can make it find
nothing while E sits under another tag's marker. That part is reasoned and not probed. A sharded deployment keeps
refusing on a moved marker, and this stays open for when one asks.

**Read the token and the events in a snapshot transaction on a sharded cluster**, so that step 3 could stay there and a
read could no longer miss an event its token holds. That would fix the second case of #1231 on a sharded cluster too and close the hole
the sum check also has. It costs a transaction on every read and a branch in each store's read path, and it can't be
tested on the single-shard cluster the test suite starts. It stays open with the option above.

**Let `resumeFromMarkerVersions` turn the query off.** It would give a deployment that replaces event documents a
way back to today's check. But the setting means something else, and someone who turns it off because the read-only
transaction can't run would lose this decision for no reason.

**Ask the driver for the cluster type.** `MongoClient.getClusterDescription()` doesn't block and can answer `UNKNOWN`,
and a load balancer in front of a mongos answers `LOAD_BALANCED`. A test for `SHARDED` that keeps its first answer
would run the query on a mongos, and the Spring and reactor stores would answer differently through `hello`.

**State a replica-set-only precondition and run the query everywhere.** A sharded deployment would then get a check
that lets a matching append through, where today it refuses. That's worse than the sum check.

**For #1300, keep a marker's version when it's pruned.** Then there's nothing to delete, since the version is the only
thing in a marker.

**For #1300, a store-wide generation** in the token, increased by every prune. It only works when every prune
increases it, and a prune is something an operator runs by hand. The epoch changes whatever deletes the marker, and
ADR 148 already writes it.

**For #1300, also query for a marker the token didn't see and that is still missing.** That would find an event whose
marker was pruned against the rule. It runs a query on every conditional append whose criteria has a marker that was
never created, which includes the usual "only if this entity doesn't exist yet" append. It finds only events with
`mv`. And after a prune that ADR 21 allowed, while events with the tag still exist, it would refuse every conditional
append on that tag until an unconditional one creates the marker again.

**For #1300, a prune tool** that deletes a marker in a transaction with a check for its events. It's the rule above
written as code. Nobody has asked to prune markers, so it isn't built now, and the rule doesn't change if one is added.

**For #1300, only change ADR 21 to say that markers must not be pruned while a token read before the prune might
still be used.** No store can find the tokens a caller holds, so nobody can follow that rule.

## Consequences

- On a replica set, a conditional append no longer fails with `DcbAppendConditionNotFulfilledException` because of an
  append by a 0.35 node, committed after the read, that shares a tag with its criteria and matches none of them. That
  covers the three cases in #1231. It can still fail without a match when the query's conditions above don't hold, for
  example after a marker's epoch changed or a 0.34 node incremented a marker. On anything else, such as a sharded
  cluster, none of the three is covered, and any marker of the criteria that moved refuses the append.
- When such an append runs at the same time as the conditional one, both still increment the shared tag marker, and
  MongoDB aborts one of them with a transient transaction error. When the store runs the transaction itself, it
  retries it without a new read or decision. Inside a transaction of yours, the error aborts your transaction, as it
  does today. Removing that conflict as well would need markers finer than a tag, which ADR 21 rules out.
- On a replica set, a conditional append runs one more indexed query in its transaction when its markers moved and
  don't rule out all of its items. An append whose markers didn't move does the same work as today. No append writes more documents than today, so no new write contention
  is added.
- Outside a transaction, an append now waits for a majority on its counter update, overriding a w:1. In alternating
  before and after runs of DCB appends on a local three-member set, the median p50 of a single writer at w:1 went from
  4.0 to 3.8 ms on the native store, 3.7 to 3.7 ms on the Spring store and 4.1 to 5.2 ms on the reactor store. A client
  with no write concern, which already waited for majority there, moved by up to 1.2 ms between the same runs. With 8
  writers, throughput went from about 990 to 1030 appends a second on the native store, 1050 to 875 on the Spring store
  and 990 to 670 on the reactor store, where the ranges of the four rounds on each side didn't overlap. All members ran
  on one host, so these numbers show no network. Across a network each such append should wait about one more round trip
  to a secondary, which wasn't measured. The stream write path wasn't measured either, and it shares the update.
- A token that saw a marker that came back with another epoch or a lower version is refused, even when the versions add
  up to the same sum.
- ADR 88's table changes. With the default settings, the MongoDB stores on a replica set no longer refuse for an
  event of an excluded type that a 0.35 node appended and that shares a tag, and they still don't see a tag-scoped
  append after a `wholeStoreLock` read. Those two answers were one fact in `DcbAppendConditionModel`, and they aren't
  any more, so the model gets a third value for these stores. `TAG_MARKER` stays for a store that only compares markers, which `DcbAppendCondition` still allows.
- ADR 148 relies on nothing replacing the document of an event a 0.35 node wrote, such as a 0.34 `updateEvent`. The
  append check now relies on it too, and more is at stake. When such an event matches a condition and was committed
  after the read, the query doesn't find it, and an append that should fail passes. Replacing an event document isn't
  supported, and no setting turns the query off.
- The token gets a factory, an accessor and a nested record. Nothing that exists in 0.35 is removed or changed, so a
  `DcbEventStore` of your own keeps working with `of(DcbCriteria, long)`. A token built with that factory and passed to
  a MongoDB store throws `IllegalArgumentException`.
- The token map holds one entry per marker key, so with ADR 162 an item without tags has one entry per stripe.
- This decision changes no marker document, field or index, and neither the events' `mv` nor `version()`. A 0.34 node
  and a 0.35 node can append to the same store as far as this decision goes, and going back to 0.34 is safe for the
  data. A 0.34 increment moves u, and then the check refuses an item the markers don't rule out, as it does today. ADR
  162 keeps that with its setting off, and its setting may be turned on only once no 0.34 node runs.
