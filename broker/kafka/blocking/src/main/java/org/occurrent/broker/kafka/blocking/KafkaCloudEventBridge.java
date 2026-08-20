/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.broker.kafka.blocking;

import io.cloudevents.CloudEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * Bridges a Kafka topic into a {@link PushSubscriptionModel}, the CloudEvent-level consume side ADR 133 decision 1
 * describes. Rebuilds each record as a {@link CloudEvent} through {@link KafkaCloudEventMapper}, hands it to
 * {@link PushSubscriptionModel#accept(CloudEvent)}, and commits only once the {@link RoutingOutcome} that
 * {@code accept(...)} reported through a shared {@link RoutingOutcomeChannel} says the event was actually consumed.
 * <p>
 * <strong>Holds a {@link PushSubscriptionModel}, never a {@link CatchupThenPushSubscriptionModel}</strong>, for the
 * same reason {@code RabbitMqCloudEventBridge} does. ADR 133 decision 1 is explicit that a bridge feeds the live
 * model, not the catch-up wrapper in front of it.
 * <p>
 * <strong>Acknowledgement.</strong> {@code accept(...)} throwing (a handler exception, or a subscription filter
 * that failed to evaluate) never commits. A normal return with {@link RoutingOutcome#DELIVERED} or
 * {@link RoutingOutcome#FILTERED} stages this record's offset for the next commit. A normal return with
 * {@link RoutingOutcome#NOT_DELIVERABLE} never does. In every case that does not commit, this bridge's configured
 * {@link DeliveryFailurePolicy} applies. {@link DeliveryFailurePolicy#REDELIVER} (the default) seeks the consumer
 * back to this record's offset, {@link DeliveryFailurePolicy#PARK} republishes to a parking destination and only
 * once that publish is confirmed treats this record as resolved, exactly as a delivered one.
 * <p>
 * <strong>One dedicated thread owns the {@code Consumer} end to end</strong>, unlike {@code RabbitMqCloudEventBridge}'s
 * split between a scheduler thread and an AMQP callback thread. A Kafka {@code Consumer} is not thread-safe, so this
 * bridge runs one loop, on one thread, that polls, decides the coarse lifecycle gate, feeds the model, and commits.
 * {@link Builder#pollTimeout(Duration)}'s own javadoc explains why one setting serves both the poll bound and the
 * lifecycle recheck cadence.
 * <p>
 * <strong>Only the loop thread itself ever closes the {@code Consumer}.</strong> {@link #close()} sets the running
 * flag, calls {@code wakeup()} (the one {@code Consumer} method Kafka documents as safe to call from another
 * thread), and waits for the loop thread to finish, but the actual {@code Consumer.close()} call happens inside the
 * loop thread's own {@code finally} block once its loop exits, never on the caller's thread. A caller thread closing
 * the {@code Consumer} while the loop thread might still be inside a poll, a handler, or a commit would violate
 * Kafka's single-thread-access contract. The cost of that is that {@link #close()} can return before the
 * {@code Consumer} is actually closed, if the loop thread is still busy past its join wait, most often a slow
 * handler. The loop thread closes it as soon as that work finishes and the loop notices the running flag is false.
 * <p>
 * <strong>Commit batching, and what a crash costs.</strong> Every record in one {@code poll()} batch that resolves
 * (delivered, filtered, or a confirmed park) stages {@code record.offset() + 1} for its partition. One
 * {@code commitSync(Map)} call commits every partition that made progress once the whole batch is walked, never the
 * no-argument form. A crash between processing a poll's records and that batch commit redelivers whatever prefix of
 * that poll's records already resolved but was not yet committed. That is a replay of already-succeeded work, not a
 * skip. At-least-once delivery already requires every handler here to tolerate a repeat, and the batching only
 * changes how much of a poll gets redelivered after a crash, never whether an unresolved record could be skipped. A
 * {@code commitSync} failure Kafka itself marks retriable (a broker outage, most often) is retried with exponential
 * backoff, attempts uncapped, until it succeeds or this bridge closes, the same retry shape
 * {@code KafkaCloudEventSink} uses for a publish. A crash during that retry leaves the offset exactly as uncommitted
 * as an ordinary crash would, so redelivery covers it. Nothing is acknowledged or skipped because a commit was being
 * retried. A non-retriable commit failure (the consumer fenced out of its group, for example), or a
 * {@link Builder#commitRetryStrategy(RetryStrategy)} narrower than the default exhausting its own attempts, is
 * logged and this bridge tries again on a later commit, without seeking anything back first. Every partition's
 * position is already exactly where it should be once the batch is fully walked, past the last record this batch
 * actually handled, so the records a later commit would cover are precisely the ones already delivered, and a
 * rewind here would only redeliver the whole batch again on every poll for as long as the commit keeps failing,
 * forever under a persistent failure, for no safety this bridge does not already have. A throw from inside the
 * per-record loop itself is different, and does rewind, since it means some record in this batch, on this
 * partition or one the loop had not reached yet, was never handled at all while {@code poll()} had already
 * advanced every partition's position regardless.
 * <p>
 * <strong>Per-partition failure isolation.</strong> A record that does not resolve makes the consumer {@code seek}
 * back to that record's offset and this bridge stops processing that partition's remaining records for this poll,
 * so a later record in the same partition is never committed past the one that failed. Other partitions in the same
 * poll are unaffected, since their offsets are independent.
 * <p>
 * <strong>Coarse lifecycle.</strong> Before every poll, this bridge reads {@link PushSubscriptionModel#subscriptionIds()}
 * and {@link PushSubscriptionModel#isRunning(String)} and pauses or resumes its own assignment to match, fetching
 * records while the model has a running subscription and not fetching otherwise. This is deliberately coarse, a small
 * delay either way is harmless, and it exists so this bridge never feeds a stopped or paused model, which per ADR 85
 * and ADR 104 drops the event rather than holding it. {@code poll()} still runs while paused, since a paused consumer
 * has to keep polling to heartbeat and complete rebalances. Kafka simply returns no records for paused partitions.
 * Never used to decide a single record. That decision comes from the {@link RoutingOutcome} above. Pausing can only
 * pause an assignment that already exists, and the assignment itself is only created by a {@code poll()} call, so a
 * fresh {@code Consumer}'s very first fetch can still return records before pausing has ever had a chance to apply.
 * When that happens this bridge seeks every affected partition back to the earliest record that poll returned,
 * rather than silently dropping what it already fetched but was never entitled to feed the model.
 * <p>
 * <strong>Ordering.</strong> A partitioned topic gives no global order. Two events on different partitions can be
 * processed in either order by this bridge, whatever their publish order was. Events for one stream stay in order
 * against each other only when the publisher keyed by stream id onto one partition, which
 * {@code KafkaSharedTopicDestinationResolver} (the shipped default) does. A projection that accumulates state per
 * stream is fine under that default. One that depends on order across streams is not, and needs a single partition or a
 * different feed.
 */
public final class KafkaCloudEventBridge implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaCloudEventBridge.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final PushSubscriptionModel model;
    private final RoutingOutcomeChannel outcomeChannel;
    private final Duration pollTimeout;
    private final Duration closeTimeout;
    private final RetryStrategy commitRetryStrategy;
    private final KafkaDeliveryFailureAction failureAction;
    private final Thread loopThread;

    private volatile boolean running = true;

    private KafkaCloudEventBridge(KafkaConsumer<String, byte[]> consumer, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel,
                                   Duration pollTimeout, Duration closeTimeout, RetryStrategy commitRetryStrategy,
                                   KafkaDeliveryFailureAction failureAction, String groupId) {
        this.consumer = consumer;
        this.model = model;
        this.outcomeChannel = outcomeChannel;
        this.pollTimeout = pollTimeout;
        this.closeTimeout = closeTimeout;
        this.commitRetryStrategy = commitRetryStrategy;
        this.failureAction = failureAction;
        this.loopThread = new Thread(this::runLoop, "kafka-cloudevent-bridge-" + groupId);
        this.loopThread.setDaemon(true);
    }

    /**
     * A {@code commitSync} failure Kafka itself marks retriable is retried under this by default, attempts
     * uncapped, until it succeeds or this bridge closes. The same shape {@code KafkaCloudEventSink} uses for a
     * publish, see the class javadoc's commit-batching paragraph for what a crash during that retry costs, and
     * {@link Builder#commitRetryStrategy(RetryStrategy)} for how to replace it.
     */
    private static RetryStrategy defaultCommitRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f)
                .retryIf(throwable -> throwable instanceof RetriableException);
    }

    /**
     * @param consumerConfig Kafka consumer configuration, {@code bootstrap.servers} and {@code group.id} at
     *                       minimum. Read once, at {@link Builder#build()}, to construct and own this bridge's own
     *                       {@code Consumer}. Refused when {@code group.id} is absent, and when
     *                       {@code enable.auto.commit} is anything other than exactly {@code "false"}, per ADR 133,
     *                       since seeking only works if nothing else commits.
     * @param model          The live model this bridge feeds. Never a {@link CatchupThenPushSubscriptionModel}, see
     *                       the class javadoc.
     * @param outcomeChannel Shared with {@code model}'s own constructor, see {@link RoutingOutcomeChannel}.
     */
    public static Builder builder(Map<String, Object> consumerConfig, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel) {
        return new Builder(consumerConfig, model, outcomeChannel);
    }

    private void runLoop() {
        try {
            while (running) {
                try {
                    boolean shouldConsume = shouldConsume();
                    reconcilePauseResume(shouldConsume);
                    ConsumerRecords<String, byte[]> records;
                    try {
                        records = consumer.poll(pollTimeout);
                    } catch (WakeupException e) {
                        continue; // running is re-checked at the top of the loop. False here means close() woke it up to exit.
                    }
                    if (records.isEmpty()) {
                        continue;
                    }
                    if (!shouldConsume) {
                        // reconcilePauseResume(...) above can only pause an assignment that already exists, and the
                        // very first poll() of a fresh Consumer is what creates that assignment in the first place. A
                        // record fetched in that same call, before pause has ever had a chance to apply, must not be
                        // silently dropped, since poll() already advanced this Consumer's own read position past it.
                        // Without a seek back here it would never be offered to this bridge again, without ever
                        // having been committed either. Rewinding every affected partition to its earliest fetched
                        // record undoes that advance, so the next poll() (once genuinely paused) simply refetches it.
                        seekToEarliestFetched(records);
                        continue;
                    }
                    processBatch(records);
                } catch (RuntimeException e) {
                    // A commitSync failure, retriable or not, is already handled inside processBatch and never
                    // reaches here. This catches everything else, poll() itself throwing (an authentication error,
                    // most notably) or a throw from the per-record loop (already rewound by processBatch before
                    // rethrowing), so one bad iteration never kills the only consume thread. The next poll retries
                    // from wherever the Consumer's own position and this bridge's seeks left it, which is safe
                    // exactly the way a crash mid-iteration already has to be, see the class javadoc's
                    // commit-batching paragraph. Backs off for pollTimeout first, since a persistent poll()
                    // failure would otherwise spin this loop as fast as the JVM allows instead of at the cadence
                    // every other path through this loop already keeps to.
                    if (running) {
                        log.warn("The Kafka consume loop for group \"{}\" failed this iteration. Retrying after pollTimeout.",
                                consumer.groupMetadata().groupId(), e);
                        sleepUninterruptibly(pollTimeout);
                    }
                }
            }
        } finally {
            // Only this thread ever closes the Consumer, see the class javadoc.
            try {
                consumer.close(closeTimeout);
            } catch (RuntimeException e) {
                log.warn("Failed to close the Kafka consumer cleanly during shutdown.", e);
            }
            // This bridge has no permanent-stop path of its own today, but the loop can still exit here without
            // close() ever having run, an uncaught Error escaping the try above, most notably. Closing
            // failureAction here too, independently of the Consumer close above, means the parking producer it
            // owns is never left open past this thread's own teardown. KafkaDeliveryFailureAction#close()
            // already does nothing on a second call, so close() calling it again afterward, on an ordinary
            // shutdown, is harmless.
            failureAction.close();
        }
    }

    private boolean shouldConsume() {
        Set<String> subscriptionIds = model.subscriptionIds();
        String subscriptionId = subscriptionIds.isEmpty() ? null : subscriptionIds.iterator().next();
        return subscriptionId != null && model.isRunning(subscriptionId);
    }

    private void reconcilePauseResume(boolean shouldConsume) {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            return;
        }
        if (shouldConsume) {
            consumer.resume(assignment);
        } else {
            consumer.pause(assignment);
        }
    }

    // Best effort. One partition's seek throwing (a rebalance that just took it away, most often) must not stop
    // every other partition in this batch from being rewound too, or the very failure this exists to route around
    // would abort the rewind after the first partition it happens to hit, in whatever order records.partitions()
    // iterates. A partition whose own seek keeps failing stays at risk until its assignment is resolved, but the
    // rest of the batch is not held hostage to it.
    private void seekToEarliestFetched(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition partition : records.partitions()) {
            try {
                consumer.seek(partition, records.records(partition).get(0).offset());
            } catch (RuntimeException e) {
                log.warn("Failed to rewind partition {} back to its earliest fetched offset in this batch.", partition, e);
            }
        }
    }

    // Returns true when this batch staged nothing to commit but did seek at least one partition back, the poison
    // record shape: the next poll() would refetch and redeliver the identical record with nothing else to show
    // for it. Backs off for pollTimeout in that case before returning, since this throws nothing the outer catch
    // could back off for on its own.
    private void processBatch(ConsumerRecords<String, byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        boolean seekBackHappened = false;
        try {
            for (TopicPartition partition : records.partitions()) {
                for (ConsumerRecord<String, byte[]> record : records.records(partition)) {
                    if (!handleRecord(record, toCommit)) {
                        consumer.seek(partition, record.offset());
                        seekBackHappened = true;
                        break; // Stop this partition's remaining records for this poll. Other partitions are unaffected.
                    }
                }
            }
        } catch (RuntimeException e) {
            // A throw here, a failing seek most often, means some record in this batch was never handed to the
            // model at all, on this partition or any partition the loop had not reached yet, while poll() already
            // advanced every partition's position regardless of whether the loop ever got to it. Rewinding every
            // partition this batch touched back to its own earliest fetched record, the same seek
            // seekToEarliestFetched(records) already applies for the paused case above, undoes that advance, so
            // the next poll() refetches and reprocesses this whole batch from the start instead of silently
            // skipping whatever was never handled.
            seekToEarliestFetched(records);
            throw e;
        }
        if (!toCommit.isEmpty()) {
            try {
                commitWithRetry(toCommit);
            } catch (RuntimeException e) {
                // commitRetryStrategy exhausted, or the failure was never retriable to begin with. Unlike a throw
                // from the loop above, nothing here needs a rewind. Every partition's position is already exactly
                // where it should be, past the last record this batch actually handled, or at the seek-and-break
                // point for one that failed, so the records a later successful commit would cover are precisely
                // the ones this batch already delivered. Rewinding here would instead redeliver the whole batch
                // again on every poll for as long as the commit keeps failing, forever under a persistent
                // failure, for no safety this bridge does not already have. Logged and left for the loop to try
                // again on its own next commit. A crash before that later commit succeeds is plain at-least-once
                // redelivery, the same as any other crash between a poll's deliveries and its commit.
                log.warn("Failed to commit for group \"{}\" after this batch resolved. Retrying on a later commit.",
                        consumer.groupMetadata().groupId(), e);
            }
        }
        if (seekBackHappened && toCommit.isEmpty()) {
            // A poison record. Every partition that fetched anything ended in a seek back to the exact offset it
            // started this poll at, so the next poll() would refetch and redeliver the identical record
            // immediately. Backing off for pollTimeout keeps that at this loop's normal cadence instead of
            // spinning it at the JVM's own maximum rate against a record that can never resolve.
            sleepUninterruptibly(pollTimeout);
        }
    }

    // Retries a retriable commitSync failure with commitRetryStrategy, attempts uncapped by the default, until it
    // succeeds or this bridge closes (running turns false). A non-retriable failure propagates immediately.
    private void commitWithRetry(Map<TopicPartition, OffsetAndMetadata> toCommit) {
        try {
            executeWithRetry(() -> consumer.commitSync(toCommit), __ -> running, commitRetryStrategy).run();
        } catch (WakeupException e) {
            // close() calling wakeup() while this thread was between blocking calls, most often a handler that
            // just called close() on itself, arms exactly this: the next blocking call raises it, and that next
            // call is this batch's own commitSync, not anything the caller of that blocking call chose to
            // interrupt. Every record in this batch already resolved, so losing this commit to a signal that was
            // never actually about the commit itself would replay the whole batch on the next start for no
            // reason. Kafka documents wakeup() as arming a single pending interrupt, consumed by the first
            // blocking call it raises in, so retrying the same commit once more proceeds normally.
            consumer.commitSync(toCommit);
        }
    }

    // Sleeps for duration, restoring the interrupt flag rather than propagating it, since the loop thread has
    // nothing above it to hand an InterruptedException to and running is what it already checks to decide whether
    // to keep going.
    private static void sleepUninterruptibly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Returns true when record resolved (stages its offset into toCommit), false when the caller should seek back
    // to record and stop that partition's remaining records for this poll.
    private boolean handleRecord(ConsumerRecord<String, byte[]> record, Map<TopicPartition, OffsetAndMetadata> toCommit) {
        CloudEvent cloudEvent;
        try {
            cloudEvent = KafkaCloudEventMapper.toCloudEvent(record);
        } catch (RuntimeException e) {
            log.warn("Failed to rebuild a CloudEvent from a record on topic \"{}\" partition {} offset {}.",
                    record.topic(), record.partition(), record.offset(), e);
            return resolve(record, toCommit, failureAction.applyToUndecodable(record));
        }
        RoutingOutcome outcome;
        try {
            model.accept(cloudEvent);
            outcome = outcomeChannel.takeLastOutcome();
        } catch (RuntimeException | AssertionError e) {
            // Catches AssertionError too, since a filter or the handler can throw one, and an uncaught Error here
            // would leave the loop thread dead with the partition never advancing past this record.
            outcomeChannel.takeLastOutcome();
            return resolve(record, toCommit, failureAction.apply(record, cloudEvent));
        }
        if (outcome == RoutingOutcome.DELIVERED || outcome == RoutingOutcome.FILTERED) {
            stage(record, toCommit);
            return true;
        }
        return resolve(record, toCommit, failureAction.apply(record, cloudEvent));
    }

    private boolean resolve(ConsumerRecord<String, byte[]> record, Map<TopicPartition, OffsetAndMetadata> toCommit,
                             KafkaDeliveryFailureAction.Outcome outcome) {
        if (outcome == KafkaDeliveryFailureAction.Outcome.RESOLVED) {
            stage(record, toCommit);
            return true;
        }
        return false;
    }

    private static void stage(ConsumerRecord<String, byte[]> record, Map<TopicPartition, OffsetAndMetadata> toCommit) {
        toCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
    }

    /**
     * Signals the poll loop to stop and waits up to {@link Builder#closeTimeout(Duration)} for it to finish, then
     * closes the parking producer this bridge created, if {@link DeliveryFailurePolicy#PARK} was configured. Never
     * closes the {@code Consumer} itself. See the class javadoc for why, and for what a loop thread still busy past
     * the wait means for when the {@code Consumer} actually closes.
     * <p>
     * Called from the loop thread itself, most often a handler that reacts to what it was just delivered by
     * closing this bridge, this skips the join. The loop thread can never finish this same iteration while
     * blocked waiting for itself, so joining here would only wait out the full timeout for nothing, and the loop
     * thread's own {@code finally} block still closes the {@code Consumer} once this call returns and the loop
     * next notices {@code running} is false.
     */
    @Override
    public void close() {
        running = false;
        try {
            consumer.wakeup();
        } catch (RuntimeException ignored) {
            // Already closed by the loop thread's own finally. Nothing left to wake up.
        }
        if (Thread.currentThread() != loopThread) {
            try {
                loopThread.join(closeTimeout.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        failureAction.close();
    }

    public static final class Builder {
        private final Map<String, Object> consumerConfig;
        private final PushSubscriptionModel model;
        private final RoutingOutcomeChannel outcomeChannel;
        private @Nullable DestinationResolver<KafkaDestination> resolver;
        private @Nullable SubscriptionFilter bindingFilter;
        private @Nullable Set<KafkaDestination> bindings;
        private DeliveryFailurePolicy deliveryFailurePolicy = DeliveryFailurePolicy.REDELIVER;
        private @Nullable KafkaDestination parkingDestination;
        private Duration pollTimeout = Duration.ofSeconds(1);
        private Duration closeTimeout = Duration.ofSeconds(30);
        private RetryStrategy commitRetryStrategy = defaultCommitRetryStrategy();

        private Builder(Map<String, Object> consumerConfig, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel) {
            requireNonNull(consumerConfig, "consumerConfig cannot be null");
            this.consumerConfig = new HashMap<>(consumerConfig);
            this.model = requireNonNull(model, PushSubscriptionModel.class.getSimpleName() + " cannot be null");
            this.outcomeChannel = requireNonNull(outcomeChannel, RoutingOutcomeChannel.class.getSimpleName() + " cannot be null");
        }

        /**
         * Derives the subscribed topics from {@link #bindingFilter(SubscriptionFilter)} or, absent one, from
         * {@link DestinationResolver#catchAllDestination()}. Required unless {@link #bindings(Set)} is given.
         */
        public Builder resolver(DestinationResolver<KafkaDestination> resolver) {
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * Narrows the subscribed topics to {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for this
         * filter, falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot derive
         * one. Requires {@link #resolver(DestinationResolver)}. Per ADR 133 decision 5, this filter narrows what
         * arrives. It must be at least as inclusive as the subscription's own filter, or events the subscription
         * would have accepted never arrive at all.
         */
        public Builder bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Subscribes to exactly these destinations instead of deriving any from a resolver, the explicit escape
         * hatch for a subscription scheme a resolver cannot express. Only {@code topic()} and
         * {@code topicIsPattern()} are read. A key or headers on a given destination are ignored, since a Kafka
         * subscription has no per-message components. Every destination must agree on
         * {@link KafkaDestination#topicIsPattern()}. {@link #build()} refuses a set mixing literal and
         * pattern-typed destinations.
         */
        public Builder bindings(Set<KafkaDestination> bindings) {
            this.bindings = Set.copyOf(requireNonNull(bindings, "bindings cannot be null"));
            return this;
        }

        /**
         * What this bridge does with a record it will not commit. {@link DeliveryFailurePolicy#REDELIVER} by
         * default.
         */
        public Builder onDeliveryFailure(DeliveryFailurePolicy deliveryFailurePolicy) {
            this.deliveryFailurePolicy = requireNonNull(deliveryFailurePolicy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * The destination {@link DeliveryFailurePolicy#PARK} publishes a failed delivery to. Required when
         * {@link #onDeliveryFailure(DeliveryFailurePolicy)} is {@code PARK} ({@link #build()} refuses otherwise).
         * Given without {@code PARK}, this is accepted but unused, not refused, the same choice
         * {@link KafkaDeliveryFailureAction#create(Map, DeliveryFailurePolicy, KafkaDestination, Logger)} makes, so
         * switching {@link #onDeliveryFailure} back to {@code REDELIVER} in application config never has to strip
         * this call out along with it.
         */
        public Builder parkingDestination(KafkaDestination parkingDestination) {
            this.parkingDestination = requireNonNull(parkingDestination, "parkingDestination cannot be null");
            return this;
        }

        /**
         * How long each {@code poll()} call blocks waiting for records, and, since this bridge's coarse lifecycle
         * gate (see the class javadoc) is rechecked once per loop iteration rather than on a separate schedule,
         * also how often that recheck happens. One second by default. Kafka requires this bridge to keep polling
         * even while paused, to heartbeat and complete rebalances, so the poll bound and the lifecycle recheck
         * cadence could not be split apart without this bridge running two competing timers against the same
         * single-threaded {@code Consumer}.
         * A small delay either way is harmless, see the class javadoc, so this rarely needs changing.
         */
        public Builder pollTimeout(Duration pollTimeout) {
            requireNonNull(pollTimeout, "pollTimeout cannot be null");
            if (pollTimeout.toMillis() <= 0) {
                throw new IllegalArgumentException("pollTimeout must be at least 1 millisecond, was " + pollTimeout);
            }
            this.pollTimeout = pollTimeout;
            return this;
        }

        /**
         * How long {@link KafkaCloudEventBridge#close()} waits for the poll loop thread to finish before returning
         * anyway. Thirty seconds by default. Only that thread ever closes the {@code Consumer}, see the class
         * javadoc, so a loop thread still busy past this wait, most often a slow handler, means {@code close()}
         * returns before the {@code Consumer} is actually closed. It still closes as soon as that work finishes.
         */
        public Builder closeTimeout(Duration closeTimeout) {
            requireNonNull(closeTimeout, "closeTimeout cannot be null");
            if (closeTimeout.toMillis() <= 0) {
                throw new IllegalArgumentException("closeTimeout must be at least 1 millisecond, was " + closeTimeout);
            }
            this.closeTimeout = closeTimeout;
            return this;
        }

        /**
         * How a retriable {@code commitSync} failure is retried. Exponential backoff from 100 ms up to 2 seconds by
         * default, attempts uncapped until this bridge closes, the same shape {@link KafkaCloudEventSink.Builder#retryStrategy(RetryStrategy)}
         * defaults to for a publish. Retries only a {@code commitSync} failure Kafka itself marks
         * {@code org.apache.kafka.common.errors.RetriableException}, a broker outage most often. Passing a
         * {@link RetryStrategy} here replaces that predicate too, so a caller that wants a narrower or wider retry
         * configures its own.
         */
        public Builder commitRetryStrategy(RetryStrategy commitRetryStrategy) {
            this.commitRetryStrategy = requireNonNull(commitRetryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        public KafkaCloudEventBridge build() {
            if (bindings == null && resolver == null) {
                throw new IllegalStateException("A resolver(...), or explicit bindings(...), is required");
            }
            if (deliveryFailurePolicy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
                throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
            }
            if (deliveryFailurePolicy == DeliveryFailurePolicy.PARK && parkingDestination.topicIsPattern()) {
                throw new IllegalStateException("parkingDestination \"" + parkingDestination.topic() + "\" is " +
                        "pattern-typed (topicIsPattern() is true), meant for subscribing, never for publishing. " +
                        "PARK needs a literal topic name to park a failed delivery to.");
            }
            Object groupId = consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
            if (groupId == null || groupId.toString().isBlank()) {
                throw new IllegalStateException("consumerConfig must set \"" + ConsumerConfig.GROUP_ID_CONFIG +
                        "\" to a non-blank value, since this bridge's committed offsets, and its consume identity, " +
                        "are keyed by it. Absent or blank, KafkaConsumer construction still succeeds and this fails " +
                        "later, invisibly, as an InvalidGroupIdException the first time this bridge tries to commit " +
                        "or poll.");
            }
            Object configuredAutoCommit = consumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            if (configuredAutoCommit == null || !"false".equals(configuredAutoCommit.toString())) {
                throw new IllegalStateException("consumerConfig sets \"" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG +
                        "\" to \"" + configuredAutoCommit + "\", but this bridge requires exactly \"false\", since " +
                        "auto-commit advances the offset on a timer regardless of what this bridge decided, and a " +
                        "seek back after a delivery failure would still be committed past by it");
            }
            Map<String, Object> config = new HashMap<>(consumerConfig);
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer());
            KafkaDeliveryFailureAction failureAction = null;
            try {
                failureAction = KafkaDeliveryFailureAction.create(consumerConfig, deliveryFailurePolicy, parkingDestination, log);
                Set<KafkaDestination> destinations = KafkaTopology.topicsToSubscribe(resolver, bindingFilter, bindings);
                KafkaTopology.subscribe(consumer, destinations);
                KafkaCloudEventBridge bridge = new KafkaCloudEventBridge(consumer, model, outcomeChannel, pollTimeout, closeTimeout, commitRetryStrategy, failureAction, groupId.toString());
                bridge.loopThread.start();
                return bridge;
            } catch (RuntimeException e) {
                consumer.close();
                if (failureAction != null) {
                    failureAction.close();
                }
                throw e;
            }
        }
    }
}
