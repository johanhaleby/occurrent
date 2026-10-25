# 153. A RabbitMQ source registers a new consumer when a recovery leaves it without one

Date: 2026-10-25

## Status

Accepted. Resolves [#1066](https://github.com/johanhaleby/occurrent/issues/1066),
[#1106](https://github.com/johanhaleby/occurrent/issues/1106),
[#1112](https://github.com/johanhaleby/occurrent/issues/1112),
[#1094](https://github.com/johanhaleby/occurrent/issues/1094) and
[#1287](https://github.com/johanhaleby/occurrent/issues/1287), and takes the two RabbitMQ points of
[#1107](https://github.com/johanhaleby/occurrent/issues/1107) that are about a nack and a stopped source. Applies to
`RabbitMqCloudEventSource`, `RabbitMqDomainEventSource` and `RabbitMqCloudEventSink`. Amends
[ADR 133](0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md) on one point, the limit
it records for a connection with topology recovery turned off.

## Context

ADR 133 records that a RabbitMQ source keeps its channel when the connection under it drops, and that it consumes
again after the recovery only if topology recovery registers the consumer on the recovered channel. A
`ConnectionFactory` has that on by default. With it turned off the source stops consuming for good, and ADR 133
records that.

It is a limit the source can remove. The source holds the tag of its consumer, and its poll starts a consumer only
while it holds none. After a recovery without topology recovery the tag names a consumer that no longer exists, so the
poll never starts another. Nothing else was wrong with that source.

I checked the order of a recovery in amqp-client 5.36.0 and against a broker. The client calls these in turn:

1. `handleRecoveryStarted` on the connection's recovery listeners.
2. The recovery of each channel, which calls the channel's own `handleRecovery`.
3. The recovery of the topology, if it is turned on, which registers each consumer again under its old tag and then
   calls `consumerRecovered(oldTag, newTag)`.
4. `handleRecovery` on the connection's recovery listeners.

So a channel's recovery listener runs before the consumer is registered again and can't tell the two cases apart. The
connection's `handleRecovery` runs after, and it is the first place where a consumer that is still missing is
missing for good.

Reviewing the failure paths of the same classes for 0.35.0 turned up four more defects of the same kind, where the
source believes something that stopped being true.

- **A failed cancel leaves the tag.** `basicCancel` throws `IOException` with "Unknown consumerTag" for a tag the
  channel doesn't have, and the source kept the tag, so it believed it was consuming. After a recovery that left the
  tag stale this was the second way to the same stuck source.
- **A second nack closes the channel.** A nack is a negative acknowledgement with requeue. `releaseHeldDeferredDelivery` put a tag back in its queue when the release
  threw anything. `ChannelN.basicNack` sends the frame and then calls the application's `MetricsCollector`, so a
  collector that throws makes the release fail after the nack was sent. The next poll nacked the tag again, and the
  broker closed the channel with `406 PRECONDITION_FAILED` on RabbitMQ 4.1. I ran this against the Testcontainers
  image of that version.
- **A cleanup replaces the failure.** `close()`, `stopPermanently()` and `build()` ran their cleanup steps from a
  `finally`, and a step that threw replaced the exception already leaving. Java discards the replaced one.
- **The close deadline overflows.** `close()` read `closeTimeout.toNanos()` first, and `Duration.toNanos()` throws
  `ArithmeticException` beyond about 292 years. The builder refuses only a negative timeout, so `close()` failed
  before it cancelled the consumer.
- **A publisher keeps a closed channel.** A repair of `RabbitMqConfirmPublisher` that failed to open a new channel
  left the closed one in place, and every later publish failed on it, also after the connection was back.

## Decision

1. **A source follows the recoveries of the connection it was built with.** `RabbitMqConsumerRecovery` adds a
   connection-level `RecoveryListener` and a `ConsumerRecoveryListener` when the source starts. It takes the tag the
   source holds when the recovery starts. If the client reports that consumer as registered again, the source holds the
   tag the client reports. If the connection's `handleRecovery` runs without that report, the source clears the tag,
   logs it at `info`, and the poll registers a new consumer on its next tick. The listeners come off when the source
   closes, stops for good or fails to build.
2. **It has to be the connection the builder was given.** `Channel.getConnection()` on an `AutorecoveringChannel`
   returns the plain connection underneath it, which never calls a recovery listener. The listeners go on the
   `AutorecoveringConnection` the source was built with, and a connection that isn't one gets none, since it never
   recovers by itself.
3. **The tag is written by compare-and-set.** The poll writes it from its own thread while a recovery runs, and
   the lock the poll holds around a blocking `basicConsume` would make the recovery wait for it. The source keeps the
   tag in an `AtomicReference`. A consumer the poll registered after the recovery started has another tag than the one
   taken at the start and is never cleared. A report about another source's consumer on the same connection doesn't
   match the tag either.
4. **A cancel that fails clears the tag, except one that timed out.** A `ChannelContinuationTimeoutException` means
   the reply didn't come, so the consumer may still exist, and the next poll cancels again. Any other failure means the
   channel has no such consumer or no longer works, and keeping the tag only blocks the next registration.
5. **A held delivery is nacked once.** The release puts a tag back only for a `RabbitMqSourceException` or a
   `ShutdownSignalException`, which are how a nack that wasn't sent fails. Anything else comes from the
   `MetricsCollector` after the frame was sent, and the tag is not put back. An `Error` thrown before the frame was
   sent loses the tag. The channel close of a stop requeues it.
6. **A failure after a permanent stop is logged at `debug`.** The source doesn't stop again and doesn't log at
   `error`, and it passes an interrupt on to the thread, since the interrupt comes from `close()`.
7. **A cleanup step never replaces the failure that is leaving.** `RabbitMqTeardown` runs every step and adds a failure
   from a later step to the one already leaving as suppressed, or throws the first failed step when nothing was
   leaving. A failure that is the leaving one, or one of its causes, isn't added, since `addSuppressed` refuses it.
8. **A close timeout longer than about 146 years counts as that long.** The deadline is `System.nanoTime()` plus the
   timeout, and half the longest span a nano time difference can express is the longest timeout that can't make the
   comparison wrong. The builder keeps accepting what it accepted, since refusing a value that worked until `close()`
   changes a contract released in 0.34.0.
9. **A publish replaces a closed channel first.** `RabbitMqConfirmPublisher` checks `isOpen()` under its lock before
   it sends and retires the channel if it is closed, so a repair that failed once is made on the next publish.
10. **Two log lines claim what the code did.** The park line says a copy was parked and the acknowledgement for the
    original was sent, and the failure line says a negative acknowledgement was sent. After a recovery the client
    ignores an acknowledgement for a tag from before it, and neither line claims more than that.

## Consequences

A source on a connection with topology recovery turned off consumes again after a recovery. The delivery that was in
flight is requeued by the broker and arrives on the new consumer, the same at-least-once delivery as with topology
recovery. The limit in ADR 133 is gone, and a deployment that turned topology recovery off on purpose needs no other
change.

With topology recovery on the source keeps the consumer the client registered and starts no second one. The tests
check that the broker shows exactly one consumer after the recovery.

`ConsumerRecoveryListener` is marked in the client as not part of its public API. The source depends on it for one
thing, telling a consumer the client registered again from one it didn't. If a later amqp-client removes it, the
source can't tell the cases apart and would have to ask the broker with a passive `queueDeclare` and its consumer
count. A client upgrade that changes the order above breaks the source in the same way, which is why the real-broker
tests run both settings of topology recovery.

The compare-and-set rules out a lock the recovery would wait on. They don't rule out a recovery that starts while the
poll is registering a consumer. That consumer is recorded by the client and recovered under topology recovery like any
other, and the poll's tag stays right because nothing clears a tag it didn't hold when the recovery started.

Nothing persisted changes. No event, checkpoint, header or message body is written differently.

## Alternatives considered

**Ask the broker for the consumer count after each recovery.** A passive `queueDeclare` returns the number of
consumers on the queue, including other applications' consumers on the same queue, so a source can't tell its own from
them. The listener says which consumer was registered again.

**Cancel the old tag after a recovery and register a new consumer every time.** `basicCancel` of the old tag fails
with "Unknown consumerTag" without topology recovery and works with it, which would tell the cases apart using only
public API. With topology recovery on it cancels a consumer the client registered and registers another, so every
recovery restarts consumption. I didn't measure how long that takes.

**A builder option for topology recovery.** The source could be told how its connection was configured. The
`Connection` doesn't expose that, only the `ConnectionFactory` does, and a setting that has to match another one
will be wrong in some deployment.

**Stop the source when the tag is stale.** Closing the channel takes it out of the connection's recovery for good, as
ADR 133 explains for #1060, and the source would never consume again.

**Refuse a close timeout beyond what a nano time can express in the builder.** It turns a value that closed cleanly
into an error at startup. The saturating timeout closes the same way for every value.
