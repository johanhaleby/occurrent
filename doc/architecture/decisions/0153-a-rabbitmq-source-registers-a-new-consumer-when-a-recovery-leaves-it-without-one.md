# 153. A RabbitMQ source registers a new consumer when a recovery leaves it without one

Date: 2026-10-25

## Status

Accepted. Resolves [#1066](https://github.com/johanhaleby/occurrent/issues/1066),
[#1106](https://github.com/johanhaleby/occurrent/issues/1106),
[#1112](https://github.com/johanhaleby/occurrent/issues/1112),
[#1094](https://github.com/johanhaleby/occurrent/issues/1094) and
[#1287](https://github.com/johanhaleby/occurrent/issues/1287), and takes the two RabbitMQ points of
[#1107](https://github.com/johanhaleby/occurrent/issues/1107) that are about a negative acknowledgement and a stopped source. Applies to
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

I checked the order of a recovery in amqp-client 5.36.0 and in 5.37.0, the version the project builds with, and
against a broker. The client calls these in turn:

1. `handleRecoveryStarted` on the connection's recovery listeners.
2. The recovery of each channel, which calls the channel's own `handleRecovery`.
3. The recovery of the topology, if it is turned on, which registers each consumer again under its old tag and then
   calls `consumerRecovered(oldTag, newTag)`.
4. `handleRecovery` on the connection's recovery listeners.

So a channel's recovery listener runs before the consumer is registered again and can't tell the two cases apart. The
connection's `handleRecovery` runs after, and it is the first place where a consumer that is still missing is
missing for good.

Step 3 copies the client's records of the connection's consumers before it registers any of them again. A consumer
the source cancels after that copy is registered again, under the tag it was recorded with. A consumer the source
registers after step 1 has a tag the source didn't hold when the recovery started, so step 4 can't tell whether that
consumer survived.

Reviewing the failure paths of the same classes for 0.35.0 turned up five more defects of the same kind, where the
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
3. **The poll registers and cancels a consumer only between recoveries.** `RabbitMqConsumerRecovery` counts each
   recovery that starts and each that ends. A consumer the source no longer wants is retired, which means the source
   stops holding its tag and keeps it for the next cancel. When the client reports a retired consumer registered
   again, the new tag replaces the old one, and the next poll cancels it. A consumer the poll registered while a
   recovery started is retired the same way instead of held. When a recovery ends without the client having
   registered the consumer again, the source retires its tag, and the cancel removes the client's record of that
   consumer before the poll registers a new one. The recovery callbacks take only a short lock of their own and never
   wait for the poll's blocking calls. The source keeps its tag in an `AtomicReference`, and the callbacks compare a
   reported tag with it by value and write it by compare-and-set, so a report about another source's consumer on the
   same connection doesn't match it. A tag the client decodes from a frame can equal the one the source holds
   without being the same `String`. A consumer the broker cancels, for example because its queue was deleted, is
   retired too, since the client keeps its record and a recovery with topology recovery on would register it again.
4. **A cancel forgets a consumer only when the client no longer has it.** A cancel that returns, or fails with a
   `ShutdownSignalException` or with an `IOException` such as "Unknown consumerTag", means the channel has no such
   consumer or no longer works. A `ChannelContinuationTimeoutException` means the reply didn't come, and any other
   failure, such as one from an application's `TrafficListener` before the frame was written, says nothing about the
   consumer. In those cases the consumer may still exist, so the next poll cancels it again and registers no new one
   until a cancel shows the consumer is gone. A cancel that a recovery overlapped is made again after the recovery.
5. **A release that may have sent its nack stops the source.** The release puts a tag back only for a
   `RabbitMqSourceException` or a `ShutdownSignalException`, which are how a nack that wasn't sent fails, and the next
   poll releases it again. Any other failure, an `Error` included, can come from the application's `TrafficListener`,
   which `AMQChannel.quiescingTransmit` calls before it writes the frame, or from its `MetricsCollector`, which
   `ChannelN.basicNack` calls after. A second nack makes the broker close the channel, and no nack leaves the delivery
   unanswered, so at the default `prefetchCount` of one the broker sends nothing further. The poll logs the failure at
   `error` and stops the source, and the channel close requeues the delivery without a second nack. The stop takes no
   new work into the worker from the start, as `close()` does, so a delivery the client dispatches after the channel
   close starts no handler.
6. **A failure after a permanent stop doesn't stop the source again.** It is logged at `debug` when the handler was
   interrupted or `close()` has stopped waiting for it, and at `warn` otherwise, since nothing else reports an
   `Error`, a checked exception thrown from Kotlin or a failed acknowledgement then. An interrupt is passed on to the
   thread.
7. **A cleanup step never replaces the failure that is leaving, and a failed step skips none of the others.**
   `close()`, `stopPermanently()` and a failed `build()` run each step through `RabbitMqTeardown`, so a recovery
   listener whose removal throws no longer skips the consumer cancel, the wait for a running handler, the fence on
   acknowledgements or the interrupt before the channel closes. `RabbitMqDeliveryWorker.stop` waits for a running
   handler even when that removal throws, and throws once the wait is over. `RabbitMqTeardown` adds a failure from a
   later step to the one already leaving as suppressed, or throws the first failed step when nothing was leaving. A
   failure that is the leaving one, or one of its causes, isn't added, since `addSuppressed` refuses it.
8. **A close timeout longer than about 146 years counts as that long.** The deadline is `System.nanoTime()` plus the
   timeout, and half the longest span a nano time difference can express is the longest timeout that can't make the
   comparison wrong. The builder keeps accepting what it accepted, since refusing a value that worked until `close()`
   changes a contract released in 0.34.0.
9. **A publish replaces a closed channel first.** `RabbitMqConfirmPublisher` checks `isOpen()` under its lock before
   it sends and retires the channel if it is closed, so a repair that failed once is made on the next publish. After
   `close()` a publish fails instead, so it opens no channel that nothing would close.
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

The poll waits out a recovery before it registers or cancels a consumer. A source paused while a recovery runs keeps
the consumer the recovery registers again until its first poll after the recovery, and a source that should resume
during one starts consuming on that poll. A consumer the poll registered as a recovery started is cancelled by the
next poll, which then registers a new one if the source should still consume, since the source can't tell whether
the recovery kept the first one.

A cancel that keeps failing in a way that says nothing about the consumer, such as a `TrafficListener` that throws on
every frame, keeps the source from registering a new consumer for as long as it fails. Registering one anyway could
leave two consumers on the queue. The poll still releases a held delivery while the cancel fails.

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
