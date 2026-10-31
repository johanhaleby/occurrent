# 162. A DCB type marker is split into stripes

Date: 2026-10-30

## Status

Proposed. Decides the second part of [#1229](https://github.com/johanhaleby/occurrent/issues/1229). Amends
[ADR 21](0021-dcb-write-path-query-scoped-concurrency.md) on which markers an append increments when the setting below
is on, [ADR 147](0147-a-dcb-consistency-token-belongs-to-the-criteria-it-was-read-with.md) on the marker keys of an
item without tags, and [ADR 148](0148-dcb-snapshots-resume-from-marker-versions-not-positions.md) on what a marker key
looks like and on which keys saved versions need a marker for. Works with the append check of
[ADR 149](0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md).

## Context

Every DCB append increments one marker document per tag and one per type of its events (ADR 21). Two appends of the
same type always write the same `type:X` document, so MongoDB lets only one of them commit at a time, even when their
tags differ and no condition could ever see both. ADR 147 stopped tagged conditions from reading type markers, which
removed the false conflicts on the read side. The write side still serializes every append of a type.

A benchmark on the native store, 16 threads, conditional appends on a single-node replica set under heavy machine
load, gave an order of magnitude only. Appends of one type reached 46 to 119 per second, and appends of distinct types
320 to 822. The marker is the bottleneck for an application whose appends are mostly of one type.

The type marker is needed for one thing only. A condition with an item without tags, such as `type("TicketSold")`, has
to share a document with every append of that type, or a matching append could commit without the conditional one
noticing. ADR 147 said this part of #1229 depends on how positions are reserved. It doesn't. The argument below uses
shared marker documents only.

Released 0.34 nodes read and write `type:X` only, and nothing in a 0.34 node can be changed. Whatever 0.35 does has to
keep a 0.34 node that shares the store correct, or say plainly when it can't.

## Decision

### Stripes

The marker of a type is split into K stripes, with K = 8, a constant in the code.

- Stripe 0 keeps the key `type:X`, so every marker document and `mv` element that exists today stays valid.
- Stripe i, from 1 to K - 1, has the key `typestripe:i:X`. A key of its own prefix can't collide with a type's key,
  since `type:X#3` is the key of a legal type called `X#3`.
- The marker keys of an item without tags are every stripe of each of its types, whatever any setting says. So a read
  of such an item reads K documents per type, a conditional append on it compares and increments all K, and a read
  after versions selects on every stripe that has a marker (see "Saved versions with an absent stripe").
- By default an append increments stripe 0 of each type among its events, as 0.34 does.
- `spreadTypeMarkerWrites(true)` on the native, the Spring blocking and the Spring reactor `EventStoreConfig`, or
  `occurrent.event-store.mongodb.spread-type-marker-writes=true` in either Spring Boot starter, makes an append increment
  stripe `floorMod(appendId.value().hashCode(), K)` of each type instead. The store creates the `AppendId` once per
  append, before any retry, so every event of one append and every attempt use the same stripe.
- Tags are incremented as before.

The in-memory store derives the same keys and always writes stripe 0, so a saved `DcbMarkerVersions` means the same on
every store (ADR 148).

### Why a matching append can't get through

Take a condition with an item without tags on type X, and an append of an X event E committed after the read. E's
append increments stripe s of X, where s is 0 when its node has the setting off. The item's marker keys hold all K
stripes, so the conditional append compares stripe s with the token and increments stripe s too. If E committed before
the conditional transaction's snapshot, stripe s moved and ADR 149's check sees it. If E commits after, both
transactions write stripe s, and MongoDB lets only one commit. The stripe E picked never enters the argument, so any
function of the append would do, and so would a different setting on each node.

An item with tags doesn't read type keys at all (ADR 147), so stripes change nothing for it.

`all` and the tag markers stay as they are.

### Turning the setting on or off

Every 0.35 node reads all K stripes, whatever its own setting. So nodes on different settings can share a store, and a
rolling restart that turns the setting on or off is safe. This is the interleaving with node A on the default and node
B with the setting on:

1. A reads `type("TicketSold")`. Its token holds all eight stripes.
2. B appends a `TicketSold` whose `AppendId` picks stripe 3, so `typestripe:3:TicketSold` moves.
3. A appends with its token. Stripe 3 moved, so ADR 149's check queries it, finds the event and refuses. If B's append
   commits after A's transaction took its snapshot instead, both write stripe 3, and one of them aborts.

The other way round, a read on B and a matching append on A, is the same argument with stripe 0.

Turn the setting on only when no 0.34 node runs against the store. A 0.34 node's only key for an item without tags is
`type:X`, so an append on another stripe is invisible to it:

1. A 0.34 node reads `type("TicketSold")` and gets a token with the version of `type:TicketSold`.
2. A 0.35 node with the setting on appends a `TicketSold` whose `AppendId` picks stripe 3. It increments
   `typestripe:3:TicketSold` and doesn't write `type:TicketSold`. Seven appends in eight pick a stripe other than 0.
3. The 0.34 node appends with its token. `type:TicketSold` hasn't moved, so its check passes although a matching event
   was committed after its read. Its transaction shares no document with the 0.35 append, so nothing aborts it.

The same holds for any condition with an item without tags, such as `anyOf(tags(a), type("TicketSold"))`, for a
condition with no token, such as a 0.34 `failIfEventsMatch(type("TicketSold"))`, and for a condition an application
service builds from a read. Conditions whose items all have tags are safe, since both versions increment every tag of
an event.

With the setting off, 0.34 and 0.35 nodes can share a store in both directions. A 0.35 append writes `type:X`, which a
0.34 read sees. A 0.34 append increments `type:X`, which is stripe 0, and a 0.35 condition reads and increments every
stripe. So an upgrade from 0.34 is a rolling one again, as ADR 148 promises.

To go back to 0.34 after turning the setting on, turn it off and restart every process that appends to
the store, then downgrade. That includes a separate service, a migration or a tool with its own `EventStoreConfig`.
After the restart no process writes a stripe other than 0, so a 0.34 node sees every later append of a type. An event appended on another
stripe before that was committed before any 0.34 read that follows, so the read returns it. The stripe documents stay
behind, unused by 0.34.

### What it costs

| Operation | 0.34, one marker | 0.35, setting off | 0.35, setting on |
|---|---|---|---|
| An append | one type document written per type | the same, stripe 0 | the same, on its stripe |
| Two appends of one type with different tags | always conflict | always conflict | conflict when they pick the same stripe, about 1 in 8 |
| A read of an item without tags | one marker document per type | eight, in the same single query | eight |
| A conditional append on an item without tags | compares and increments one document per type | eight, so it writes eight documents, and still conflicts with every append of the type | the same as off |
| A read after versions on an item without tags | one `mv` branch per type | eight | eight |
| A token or saved `DcbMarkerVersions` of an item without tags | one entry per type | eight | eight |

An application that never turns the setting on pays for stripes and gets nothing back. Every read of an item without
tags reads eight marker documents per type instead of one, and every conditional append on one writes eight. The
writes can't be skipped when the local setting is off. Another node may have it on and append on stripe s during that
conditional's transaction, and only a document both transactions write makes MongoDB abort one of them. Nothing has
measured either cost.

A simulation of the benchmark above used tagged conditional appends, as with the setting on. With K = 8 it reached 202
to 446 appends of one type per second, and with K = 32 it reached 313 to 586, under the same machine load. K = 8 wins
back about half of what one marker loses against distinct types. In the three paired runs, K = 32 gave 1.49, 0.81 and
1.55 times K = 8, which is at most about half as much again, for four times the cost on every read and conditional
append of an item without tags.

Nothing has measured a conditional append on an item without tags under load either. It holds K document writes in its
transaction and conflicts with every append of the type, so I expect more retries than today, and more stripes would
make that worse.

### Saved versions with an absent stripe

ADR 148 reports versions only when every marker key of the boundary has a marker with an epoch. With the setting off,
stripes 1 to 7 come into existence only through a conditional append on an item without tags. So a snapshot of
`type(D)` or of `anyOf(tags(a), type(D))`, in an application whose appends of `D` are unconditional or conditioned on
tagged items only, would never get versions. Each snapshot would read its boundary in full, including everything from
before 0.35, for as long as the setting stays off.

So a stripe key may be saved as absent. `DcbMarkerVersions.MarkerVersion` gets an absent value, with no epoch, version 0
and an unstamped count of 0. A read that asks for versions reports a stripe it finds no marker for as absent. A tag key
still needs a marker, and a marker that exists without an epoch still gives no versions.

A read after saved versions treats an absent stripe like this:

- Still absent, it selects nothing on that stripe.
- Present now, the marker needs an epoch and an unstamped count of 0, or the read is refused with `DcbSnapshotInvalid`.
  The read then selects every `mv` element on that stripe with the marker's epoch, which is every version above 0.

This is sound for the same reason ADR 149's check can trust a marker that was absent at the read. Versions come from
one point-in-time read of the markers and the events (ADR 148). An append creates the marker of every key its events
have, in its own transaction, and the pruning rule of ADR 149 deletes a stripe only in a transaction that finds no event
of its type. So no event committed before the save has an `mv` element on a stripe that was absent at the save, and
every event that has one now was committed after it. Selecting all of them returns exactly that stripe's part of the
events after the save.

The unstamped count and the epoch cover the rest. Every event a 0.35 node writes has an `mv` element for each of its
own marker keys. A conditional append also increments the keys of its condition, but that adds no event, so with an
unstamped count of 0 every event of the stripe has an element. A 0.34 append of `X` touches only stripe 0. If it creates
`type:X` after the save, the marker has no epoch and the read is refused. A later 0.35 increment gives that marker an
epoch, but its unstamped count is then 1, so the read is still refused. If a 0.34 append increments a `type:X` that a
0.35 append created, it moves the unstamped count too, and ADR 148 refuses a moved one today. That's why stripe 0 can be
absent too. A stripe created after the save, pruned and created again has a new
epoch, and its prune removed every event of the earlier one, so the current epoch selects them all.

This removes the wait for versions on such a boundary. A type nobody has appended yet gets versions too, all of them
absent. A decider whose read saw a stripe as absent and whose append then created it skips that one save, since ADR
148's decider check counts a key the read reported without an epoch as a mismatch. Its next read sees the stripe.

### Changing K later

Nodes that disagree on K aren't safe. A writer with K = 16 can increment stripe 12, which a reader with K = 8 never
reads. The setting doesn't change that. It changes which of the K stripes an append picks, never K. So a change of K
needs two releases. To raise K, one release reads the new stripes while writers keep the old K, and the next one writes
them. To lower K, the order is the other way round. A saved `DcbMarkerVersions` of an item without tags has a different
key set after a change of K, so `afterVersions(..)` refuses it, and the snapshot is rebuilt from a full read.

### Why a setting, when ADR 148 rejected one

ADR 148 rejected "stop every 0.34 writer" because nothing enforced it, and then rejected a setting that was off by
default, since it "still relied on an operator to say the last 0.34 writer was gone". Both objections apply to turning
this setting on. Nothing detects a 0.34 node in time. A 0.34 node writes nothing that names its version, and its read
writes nothing at all. Its append shows only as a moved unstamped count on `type:X` or an event without `mv`, after it
has committed. A deployment that turns the setting on with a 0.34 node running gets no error. An append that should
have failed stays committed, such as a second invoice with the same number in the `InvoiceService` example, whose read
has only a type.

What differs is which state is the safe one. ADR 148's setting was needed for its resume to be correct, so a deployment
that left it alone wasn't. Here the default is correct with any mix of 0.34 and 0.35 nodes, and the setting buys
throughput only. An operator who never reads about it keeps a correct store and pays the cost above. The condition for
turning it on, that no 0.34 node runs, is one the operator knows at the time.

0.36.0 can turn the setting on by default. Every 0.35 node reads all stripes, so an upgrade from 0.35 to 0.36 stays
rolling, and an upgrade from 0.34 would then have to go through 0.35. Nothing in 0.35 is built for that.

## Alternatives considered

**Stripes always on, and stop every 0.34 node before starting a 0.35 node.** That gives every application the
throughput at once. It makes the upgrade unsafe by default for any application with a condition on an item without
tags, and nothing can warn it. I chose the setting, so that the default stays correct.

**Readers first, writers a release later.** 0.35 would read every stripe and write `type:X`, and 0.36 would write the
stripes. That's the default here. The setting lets an application that has no 0.34 node left take the gain now.

**Keep ADR 148's rule that every key needs a marker.** With the setting off, a boundary with an item without tags would
never get versions unless the application happens to condition on it.

**Create the missing stripes on every unconditional append.** That's seven more upserts per type on every append, which
undoes what the stripes are for.

**Saving an absent marker for tag keys too.** The argument above would hold for a tag key as well. A tag marker exists
from the first event with the tag, so nothing has needed it.

**A stripe from the event's tags.** It spreads appends with different tags just as well, and an untagged event would
still need another source. The `AppendId` exists for every append and is already created before the transaction.

**A configured list of the types that appear in conditions without tags.** Nodes that disagree on the list let a
matching append through.

**A registry document that a condition without tags creates and appenders read.** An appender whose snapshot is older
than the registry commit doesn't see it and skips the type marker.

**K = 32.** The simulation's paired runs gave 0.81 to 1.55 times K = 8, for four times the cost on every read and
conditional append of an item without tags.

## Consequences

- With the setting off, appends behave as in 0.34. A read of an item without tags reads eight marker documents per
  type, and a conditional append on one writes eight, for no gain.
- With the setting on, appends of one type with different tags conflict about one time in eight, and the tag markers
  decide the rest as before.
- An upgrade from 0.34 is rolling by default. Turning the setting on needs every 0.34 node gone, and nothing warns a
  deployment that turns it on too early. Going back needs the setting off in every process that appends to the store first.
- An absent stripe no longer stops a read from reporting versions. A missing tag marker or a marker without an epoch
  still does. `MarkerVersion` has an absent value, which only a stripe key may hold.
- `DcbMarkerVersions` no longer promises that a type's key is `type:<type>`, and the TCK no longer asserts exact key
  sets. A key is the store's own, as `version()` already is.
- The pruning rule of ADR 149 covers stripes. A stripe may be deleted only in a transaction that also finds no event of
  its type.
- K can change only over two releases.
