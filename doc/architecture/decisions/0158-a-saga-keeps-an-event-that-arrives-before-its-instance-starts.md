# 158. A saga keeps an event that arrives before its instance starts

Date: 2026-11-04

## Status

Accepted. Resolves [#1317](https://github.com/johanhaleby/occurrent/issues/1317). Changes what
[ADR 154](0154-a-saga-tells-a-late-stream-event-from-a-redelivery-per-event-type.md) says about an event that arrives
before its instance's start. ADR 154 had not shipped, so it is amended in place.

## Context

A saga instance begins with an event of one of its start types. Up to 0.34.0 an event of any other type that reached an
instance before its start was skipped, the subscription acknowledged it, and nothing was logged. Its reaction never ran.

That happens on any feed that can deliver the events of one instance out of order. A Kafka topic per event type, a
RabbitMQ consumer with more than one message in flight, or a listener with several threads can deliver
`PaymentReceived` before the `OrderPlaced` that starts the instance.

Refusing the event doesn't work. On a feed that delivers in position order the start comes later in that order, so a
refused event keeps the subscription from ever reaching the start. The event has to be kept until the start arrives.

## Decision

**The event is kept in the instance's own document.** `SagaEnvelope` gains `early`, the list of CloudEvents that arrived
before the start, in arrival order. When an event that is not a start reaches an instance that has not started and is
not quarantined, the saga adds it to `early` with a compare-and-set, the same write it uses for any transition. With no
document yet, the write inserts an instance with `started` set to false, as a failed first event already does. A
redelivery with the same CloudEvent id and source writes nothing. The subscription acknowledges the event after the
write.

**The kept events are applied in the save that applies the start.** Each one goes through the rules a delivery goes
through, its commands are dispatched together with the start's, and the instance that results is saved with one
compare-and-set. A started instance keeps nothing in `early`, so every later event is handled as before.

The write that keeps an event and the save that applies the start both write the same document under its version. If
the start is saved first, the keep loses its compare-and-set, reads the started instance and applies the event as an
ordinary delivery. If the keep is saved first, the start reads it from the document.

**The order is arrival across streams, and version within a stream.** The kept events of one stream take the places in
the arrival order that events of that stream had, sorted by version. Events that arrived as B3, A1, B2 are applied as
B2, A1, B3.

**An event that would complete the instance is applied last, together with the later versions of its own stream.** Every
kept event arrived while the instance could not complete, so each of them belongs to the instance. Applying the
completing event in its arrival place would leave the events after it to a completed instance, which skips them. The
later versions of the completing event's own stream still find the instance completed and are skipped, as they are when
a feed delivers that stream in order.

**A start that fails keeps the events.** The failure record is written with `early` copied over, so the redelivered start
applies them.

**An instance keeps at most 10 000 events.** One more fails the delivery with an `IllegalStateException` instead of being
acknowledged without a copy. The failure then goes the way of any failing event, so it is listed as pending (ADR 154) and
counts toward quarantine when quarantine is on.

### Alternatives

A store for held events beside the instance, with its own SPI, was built first. Its copies were written apart from the
envelope, so a fault between the two writes could lose an event or apply it twice, and the rule for when to hold depended
on whether the subscription model declares that it delivers in commit order. Keeping the events in the instance document
needs no new SPI and no such rule. The keep and the start are each one write to one document, so a fault during
either one writes all of it or none of it.

## Consequences

**These cases still lose an event:**

| Case | What happens | Visible |
|---|---|---|
| A version fails and is pending when the instance completes on a later version of the same stream | A replay of the pending version finds the instance completed and is skipped. The pending entry stays on the completed instance. A started instance behaves the same way in 0.35.0, and this decision does not change it | No |
| The state store fails on every read and write of one delivery | The event can be neither kept nor listed as pending, and the delivery fails. A source that moves past it loses it | Error in the log, naming the event |
| The converter can't read the event | The saga can't tell which instance the event belongs to, so nothing can be kept. A source that moves past it owns its replay | Warning in the log, naming the event |

**What this design gives up:**

- A failed event that the instance can't list as pending is not kept anywhere else. That is the second row above.
- A kept event that keeps failing fails the start, since it is applied in the start's save. Its failures are charged to
  the start, so with quarantine on the instance is quarantined on the start event rather than on the event that fails.
  An event that reaches a quarantined instance that never started is skipped, as in 0.34.0.
- Past 10 000 kept events a delivery is refused. On MongoDB the 16 MiB document limit can come first, since each kept
  event is stored whole as CloudEvents JSON. The save then fails and so does the delivery.

**An instance that waits for its start is stored and listed.** It has status `ACTIVE`, the saga's initial state and
`started` set to false, and `SagaInstances` lists it with the other active instances. One whose start never arrives
stays until it is deleted.

**Each keep rewrites the document**, so an instance that keeps n events writes all n of them on the next keep and again
on the start.

**A `SagaStateStore` must persist `early`.** A store that saves the envelope without it acknowledges the event and loses
it. `InMemorySagaStateStore` keeps the envelope as it is, and `SpringMongoSagaStateStore` writes an `early` field, a list
of CloudEvents JSON strings, only when the list is not empty.

**Going back to 0.34.0 loses the kept events.** A 0.34.0 node ignores `early` and replaces the document without it on its
next save. In a rolling upgrade, a 0.34.0 node that applies the start drops the kept events in the same way.

`SagaEnvelope` gains a 15th record component. The constructor without it stays, so a call site compiles unchanged, and
a record pattern over `SagaEnvelope` has to name it.
