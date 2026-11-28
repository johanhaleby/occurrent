# 186. A save from a later position is refused once a start position moved it earlier

Date: 2026-11-26

## Status

Accepted. Amends [ADR 130](0130-a-subscriptions-first-position-race-resolves-by-order-not-by-write-order.md), whose
earliest position wins the first-position race but which says nothing about the node that already delivers from the
later one.

## Context

Node H stores its first position P_H and its version v with it, and starts delivering from P_H. Node B then starts the
same subscription live at P_B, earlier than P_H. B's `ifAbsent()` write of P_B is refused, and the two-argument
`resolveFirstCheckpointRace` writes P_B over P_H, as ADR 130 decided. It keeps the stored version v, since B has no
version of its own to store.

H's change stream still reads from P_H. Its next save is `notOlderThan(v)`, which the stored v allows, so it writes a
position after P_H over P_B. No node delivers the events between P_B and P_H. They stay undelivered until a checkpoint
that isn't an operation time is stored, because the two-argument resolve compares only operation times.

The Spring Boot starters read P_B when the event store bean is created, and ask whether a checkpoint is stored only
when the subscription registers. When H stores P_H and then the resume token of a delivered event between those two
reads, B sees a checkpoint stored, its `ifAbsent()` write is refused, and the two-argument resolve answers empty for
the resume token. B then keeps H's resume token and starts from it, and no node delivers the events between P_B and
P_H either.

## Decision

1. When the two-argument `resolveFirstCheckpointRace` of a MongoDB checkpoint storage writes over a stored checkpoint,
   it stores a `positionMovedEarlier` field with the checkpoint. The native, Spring and reactor storages share this
   through `MongoCommons`. The Redis and in-memory storages keep the default resolve, which writes nothing.
2. A `notOlderThan` write of a resume token or an operation time is refused while that field is stored. The refusal is
   the released `CheckpointWriteConditionNotFulfilledException`, with a stored version that isn't newer than the
   version of the write. A write of a catch-up or generic checkpoint is not refused, since it doesn't go on from a
   change stream position.
3. Any write that replaces the stored checkpoint removes the field. So does `storeWriteVersion`, which is how a resume
   and a subscribe at the model default store their version, and so does the three-argument
   `resolveFirstCheckpointRace` when it keeps the stored checkpoint.
4. The blocking `DurableSubscriptionModel` tells its `StartRefusalListener`s about a refused event or quiet save
   through `handleStoredPositionMovedEarlier`, or logs an error when none is added, and throws the refusal on. That
   ends delivery in the MongoDB subscription models. It doesn't resume the subscription itself.
5. The blocking `CompetingConsumerSubscriptionModel` pauses a reported subscription and resumes it while it holds the
   lease. The plain resume stores this node's version with the stored checkpoint and replays from P_B. The node stays a
   lease candidate throughout.
6. A resume at a position, in `DurableSubscriptionModel` and in the stream catch-up model, and the position a
   `DurableSubscriptionModel` stores after a lost history, are refused the same way. They store this node's version
   with the stored checkpoint first through `storeWriteVersion` and write once more, since nothing else would remove
   the field.
7. Every checkpoint a MongoDB checkpoint storage writes keeps a `lineageOrigin` field, the operation time of the first
   position its delivery started from. A write over nothing stored sets it from the operation time it writes, and
   every other write keeps the stored one. A first-position race that writes its candidate sets it to the candidate's
   operation time. A checkpoint an Occurrent version before 0.35.0 stored has none, and neither has one whose first
   write was a resume token.
8. Both `resolveFirstCheckpointRace` methods of the MongoDB storages count a stored resume token or generic checkpoint
   whose `lineageOrigin` is later than the candidate's operation time as a later first position, and write the
   candidate over it. The two-argument one stores `positionMovedEarlier` with it, as for a later operation time. A
   stored checkpoint without `lineageOrigin` is compared as in 0.34.0.
9. The blocking Spring Boot starter starts at the checkpoint the race settled when that checkpoint is the live start,
   also when a checkpoint was stored when the subscription registered. When the race keeps the stored checkpoint, it
   starts at the model default, as before.
10. A subscribe of the blocking `DurableSubscriptionModel` at `StartAt.checkpoint(..)` of the stored checkpoint stores
    this node's version with it through the three-argument `resolveFirstCheckpointRace`, with that checkpoint as the
    candidate. The starter subscribes that way without `CompetingConsumerSubscriptionModel`, and without it the
    field item 1 stores would refuse B's own first save. The model then starts from the checkpoint that call answers.
    When another node's two-argument resolve wrote an earlier checkpoint between the model's read and the call, the
    call keeps that earlier one, and a start from the checkpoint the model read would let B's saves move the stored
    checkpoint past the events between the two. A subscribe of an id the model already runs or holds paused stores no
    version, since the wrapped model refuses it as a duplicate, and the version would remove the field while the running
    subscription saves from a later position.
11. When the field refuses the `notOlderThan` save that `ReactorDurableSubscriptionModel` makes after a subscribe took
    over the checkpoint delete of a cancel of the same id, the model logs a warning and saves the checkpoint once more
    with `any()`. A refusal with a newer stored version is handled as before.

A version bump, a compare-and-set of the stored position, and never moving a position stored with a version earlier were
considered and rejected.

## Consequences

The events between P_B and P_H are delivered once H resumes from P_B. Those after P_H are delivered again.

A subscription without `CompetingConsumerSubscriptionModel` stops delivering at the refused save, with an error in the
log, until something pauses and resumes it.

A save with `any()`, which a `DurableSubscriptionModel` without a version source writes, isn't refused and removes the
field. Such a node has no fence against an older lease either. The reactor `ReactorDurableSubscriptionModel` saves an
event's checkpoint with `any()`, except after a subscribe took over the checkpoint delete of a cancel of the same id.
That save is `notOlderThan` the version the takeover writes, and without item 11 `ReactorMongoSubscriptionModel` would
retry its delivery, and run the handler for the same event again, for as long as the field stays stored. The save with
`any()` keeps the stored version, so the delete and the write back of the takeover are still refused. On the reactor
stack B starts from P_B, the checkpoint the first-position race settled, also through the reactive Spring Boot starter, so B delivers the events between P_B and P_H. The blocking Spring Boot
starter starts B from P_B the same way when the application defines a subscription model without
`CompetingConsumerSubscriptionModel`. When B stops before it has delivered them all and a save of H's is the last one
stored, B's next start resumes after P_H, and no node delivers the events between P_B and P_H that B hadn't reached.
On the reactor stack 0.34.0 skips those events the same way, and
[#1598](https://github.com/johanhaleby/occurrent/issues/1598) tracks it, with a replay from P_B as the blocking stack
runs one.

Under `CompetingConsumerSubscriptionModel` the blocking starter starts B at the model default, which reads the stored
checkpoint once B holds the lease. With `occurrent.subscription.competing-consumer.fence-checkpoints=false` the
starter's version source answers no version, so H saves with `any()`. H's next save then writes a position after P_H
over P_B, and no node delivers the events between P_B and P_H.

The three-argument call of item 10 writes its candidate when nothing is stored. So when a cancel on another node
deletes the checkpoint between the model's read and that call, the call stores B's start checkpoint again. A
subscribe at the model default doesn't, since `storeWriteVersion` writes nothing when nothing is stored.

An ordinary restart keeps its stored checkpoint, since the `lineageOrigin` of a checkpoint stored before the restart
is earlier than the live start the restart reads. A `lineageOrigin` that is later than the real first position would
only deliver events again. One that is earlier would keep a checkpoint that skips events, so no write sets it from
anything other than an operation time it writes.

The race in the second paragraph of the context stays open in two cases. A node on 0.34.0 rewrites the whole
document without `lineageOrigin`, so the next race compares as in 0.34.0. A node that stores a resume token as its
first position, such as one that reads no operation time from MongoDB, stores no `lineageOrigin` either.

A checkpoint storage that can't compare positions, such as `SpringRedisCheckpointStorage`, would keep the stored
checkpoint. So the Spring Boot starters store a live start only over the MongoDB storage of the starter, as
[ADR 172](0172-how-the-spring-boot-starters-find-and-start-annotated-handlers.md) records, and a subscription over any
other storage starts as in 0.34.0. A write a `@PostConstruct` method
makes directly to the event store before the subscription registers is then missed on a first start, as in 0.34.0, on
a single node as much as on several.

The `notOlderThan` contract of the MongoDB storages, released in 0.33.0, refuses a write it allowed before. The
two-argument `resolveFirstCheckpointRace`, released in 0.34.0, now writes over a stored resume token or generic
checkpoint whose `lineageOrigin` is later than the candidate, where it used to answer empty. No API changes for either,
so the changelog lists both as changes of behaviour shipped in 0.34.0, with a section in the upgrade guide.
