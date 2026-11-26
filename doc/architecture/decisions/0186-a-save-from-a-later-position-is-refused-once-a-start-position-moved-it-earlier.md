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

## Decision

1. When the two-argument `resolveFirstCheckpointRace` of a MongoDB checkpoint storage writes over a later operation
   time, it stores a `positionMovedEarlier` field with the checkpoint. The native, Spring and reactor storages share
   this through `MongoCommons`. The Redis and in-memory storages keep the default resolve, which writes nothing.
2. A `notOlderThan` write of a resume token or an operation time is refused while that field is stored. The refusal is
   the released `CheckpointWriteConditionNotFulfilledException`, with a stored version that isn't newer than the
   version of the write. A write of a catch-up or generic checkpoint is not refused, since it doesn't go on from a
   change stream position.
3. Any write that replaces the stored checkpoint removes the field. So does the three-argument
   `resolveFirstCheckpointRace` when it keeps the stored checkpoint, which is how a resume stores its version.
4. The blocking `DurableSubscriptionModel` tells its `StartRefusalListener`s about a refused event or quiet save
   through `handleStoredPositionMovedEarlier`, or logs an error when none is added, and throws the refusal on. That
   ends delivery in the MongoDB subscription models. It doesn't resume the subscription itself.
5. The blocking `CompetingConsumerSubscriptionModel` pauses a reported subscription and resumes it while it holds the
   lease. The plain resume stores this node's version with the stored checkpoint and replays from P_B. The node stays a
   lease candidate throughout.
6. A resume at a position, in `DurableSubscriptionModel` and in the stream catch-up model, and the position a
   `DurableSubscriptionModel` stores after a lost history, are refused the same way. They store this node's version
   with the stored checkpoint first and write once more, since nothing else would remove the field.

A version bump, a compare-and-set of the stored position, and never moving a position stored with a version earlier were
considered and rejected.

## Consequences

The events between P_B and P_H are delivered once H resumes from P_B. Those after P_H are delivered again.

A subscription without `CompetingConsumerSubscriptionModel` stops delivering at the refused save, with an error in the
log, until something pauses and resumes it.

A save with `any()`, which a `DurableSubscriptionModel` without a version source writes, isn't refused and removes the
field. Such a node has no fence against an older lease either. The reactor `ReactorDurableSubscriptionModel` saves an
event's checkpoint with `any()`. On the reactor stack B starts from P_B, the checkpoint the first-position race settled,
also through the reactive Spring Boot starter, so B delivers the events between P_B and P_H. The blocking Spring Boot
starter starts B from P_B the same way when the application defines a subscription model without
`CompetingConsumerSubscriptionModel`. When B stops before it has delivered them all and a save of H's is the last one
stored, B's next start resumes after P_H, and no node delivers the events between P_B and P_H that B hadn't reached.

Under `CompetingConsumerSubscriptionModel` the blocking starter starts B at the model default, which reads the stored
checkpoint once B holds the lease. With `occurrent.subscription.competing-consumer.fence-checkpoints=false` the
starter's version source answers no version, so H saves with `any()`. H's next save then writes a position after P_H
over P_B, and no node delivers the events between P_B and P_H.

The `notOlderThan` contract of the MongoDB storages, released in 0.33.0, refuses a write it allowed before. No API
changes for it, so the changelog lists it as a fix of behaviour shipped in 0.34.0, with a section in the upgrade guide.
