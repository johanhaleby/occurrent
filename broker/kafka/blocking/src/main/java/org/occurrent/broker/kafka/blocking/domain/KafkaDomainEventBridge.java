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

package org.occurrent.broker.kafka.blocking.domain;

import io.cloudevents.CloudEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CloseOptions;
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
import org.occurrent.broker.kafka.blocking.KafkaCloudEventMapper;
import org.occurrent.broker.kafka.blocking.KafkaDeliveryFailureAction;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.KafkaTopology;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnreadableLiveFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * Bridges a Kafka topic into a {@link DomainEventFeed}, the domain-level consume side ADR 133 decision 5 describes.
 * Rebuilds each record as a {@link CloudEvent} through {@link KafkaCloudEventMapper} and calls
 * {@link DomainEventFeed#acceptCloudEvent(CloudEvent)}, which is where the matching, the decoding and the delivery
 * all happen. This bridge does no filtering of its own, since the feed is the only thing that can decide per ADR 133
 * decision 5.
 * <p>
 * <strong>Acknowledgement</strong> follows the {@link RoutingOutcome} {@code acceptCloudEvent(...)} returns, exactly
 * as {@code KafkaCloudEventBridge} follows the one its own model reports. {@link RoutingOutcome#DELIVERED} or
 * {@link RoutingOutcome#FILTERED} stages this record's offset for the next commit, {@link RoutingOutcome#NOT_DELIVERABLE}
 * and a thrown exception both apply this bridge's configured {@link DeliveryFailurePolicy} instead.
 * <p>
 * <strong>{@link UnreadableLiveFilterException} is different, and permanent.</strong> It means the projection this
 * feed carries was registered with a {@code data} payload filter this feed has no
 * {@link org.occurrent.filtermatching.DataFieldReader} for, a configuration error that cannot change without a new
 * registration, and the same exception instance is thrown again on every later call. On catching it, this bridge
 * logs the failure, seeks the consumer back to the triggering record exactly as a {@link DeliveryFailurePolicy#REDELIVER}
 * failure would, commits whatever else resolved in the same poll (other partitions, and earlier records in the same
 * partition), and then <strong>sets this bridge to stop for good</strong>, rather than committing past the
 * triggering record or looping the poll again. The poll loop thread then closes its own {@code Consumer} as it
 * exits, the same single exit path {@link #close()} itself uses, so the permanent stop survives whether or not that
 * commit above succeeds on the first attempt. See the thread-ownership paragraph below. Stopping here, immediately,
 * is deliberate. A {@code Consumer} that keeps its assignment but stops polling is evicted from the group only after
 * {@code max.poll.interval.ms} (five minutes by default), which would leave this permanent, intentional stop
 * indistinguishable from a hung consumer for that whole window, log noise and a pointless rebalance included.
 * Closing sends Kafka's own clean group-departure request instead, so the next consumer in this group picks up
 * starting exactly at the triggering record's offset, the same one this bridge sought back to and never committed
 * past. That departure is forced with {@link org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation#LEAVE_GROUP},
 * not the default {@link #close()} otherwise leaves in place, since a consumer configured with
 * {@code group.instance.id} for static membership keeps its assignment through an ordinary close by design, correct
 * for a caller restarting the same bridge, since a restart should not trigger a rebalance, but wrong here. This
 * stop is permanent, nothing is coming back to reclaim the assignment, so it must free it immediately rather than
 * hold it open until an operator both fixes the registration and starts a new bridge. The triggering record is not
 * requeued anywhere and not parked. Parking would still publish and then commit past it, and this must never commit
 * past it at all. It stays exactly where the last successful commit left it until an operator fixes the
 * registration and starts a new bridge, or a rebalance hands this group's partitions to another consumer, so the
 * event survives rather than being lost.
 * <p>
 * <strong>One dedicated thread owns the {@code Consumer} end to end</strong>, unlike {@code RabbitMqDomainEventBridge}'s
 * split between a scheduler thread and an AMQP callback thread. A Kafka {@code Consumer} is not thread-safe, so this
 * bridge runs one loop, on one thread, that polls, decides the coarse lifecycle gate, feeds the feed, and commits.
 * See {@code KafkaCloudEventBridge}'s class javadoc for why {@link Builder#pollTimeout(Duration)} serves both the
 * poll bound and the lifecycle recheck cadence, and for the commit-batching design, the commit retry, and what a
 * crash between a poll's deliveries and its commit costs. All of that applies here unchanged.
 * <p>
 * <strong>Only the loop thread itself ever closes the {@code Consumer}</strong>, the same rule and the same reason
 * {@code KafkaCloudEventBridge} states. {@link #close()} only signals and waits, the actual {@code Consumer.close()}
 * call lives in the loop thread's own {@code finally} block, reached whether the loop exits because {@link #close()}
 * was called or because of the permanent stop above, so a caller thread never touches the {@code Consumer} while
 * the loop thread might still be inside a poll, a handler, or a commit. Which of those two the loop exited for
 * decides which close options that {@code finally} block uses, the forced {@code LEAVE_GROUP} above for the
 * permanent stop, the default otherwise, see that paragraph for why the two must differ.
 * <p>
 * <strong>Coarse lifecycle.</strong> Before every poll, this bridge reads {@link DomainEventFeed#hasProjection()}
 * and {@link DomainEventFeed#isReadyForLiveDelivery()} and pauses or resumes its own assignment to match, consuming
 * only once a projection is registered and its catch-up-then-live transition has actually reached live, not merely
 * started. This exists for the same reason {@code RabbitMqDomainEventBridge} polls the same two methods. Without
 * the registration half, a record arriving before the application registers its projection would hit
 * {@code acceptCloudEvent(...)}'s {@link IllegalStateException} refusal on every delivery, and under
 * {@link DeliveryFailurePolicy#REDELIVER} that is an instant seek-and-redeliver loop, not a wait. Without the
 * readiness half, a record arriving before the application calls {@code catchUpAll()}/{@code catchUp(...)} or
 * {@code goLive(...)}, or while a replay is still actively running, would only ever buffer with nothing behind it,
 * which {@code acceptCloudEvent(...)} answers with {@link RoutingOutcome#NOT_DELIVERABLE} for exactly that reason
 * (see its own javadoc), and under {@code REDELIVER} that is the same instant loop, this time against a buffer that
 * never drains until live is actually reached. {@code poll()} still runs while paused, for the same heartbeat and
 * rebalance reason {@code KafkaCloudEventBridge} states, and the same rewind-to-earliest-fetched it applies to a
 * record its very first, pre-pause fetch can still return applies here too.
 * <p>
 * <strong>Ordering.</strong> See {@code KafkaCloudEventBridge}'s own class javadoc. The same caveat applies here
 * unchanged, since it is a property of the transport, not of which level a bridge feeds.
 */
public final class KafkaDomainEventBridge<E> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaDomainEventBridge.class);

    private enum HandleResult {
        RESOLVED, REDELIVER, PERMANENT_STOP
    }

    private final KafkaConsumer<String, byte[]> consumer;
    private final DomainEventFeed<E> feed;
    private final Duration pollTimeout;
    private final Duration closeTimeout;
    private final RetryStrategy commitRetryStrategy;
    private final KafkaDeliveryFailureAction failureAction;
    private final Thread loopThread;

    private volatile boolean running = true;
    private volatile boolean permanentlyStopped = false;

    private KafkaDomainEventBridge(KafkaConsumer<String, byte[]> consumer, DomainEventFeed<E> feed, Duration pollTimeout,
                                    Duration closeTimeout, RetryStrategy commitRetryStrategy,
                                    KafkaDeliveryFailureAction failureAction, String groupId) {
        this.consumer = consumer;
        this.feed = feed;
        this.pollTimeout = pollTimeout;
        this.closeTimeout = closeTimeout;
        this.commitRetryStrategy = commitRetryStrategy;
        this.failureAction = failureAction;
        this.loopThread = new Thread(this::runLoop, "kafka-domainevent-bridge-" + groupId);
        this.loopThread.setDaemon(true);
    }

    /**
     * A {@code commitSync} failure Kafka itself marks retriable is retried under this by default, attempts
     * uncapped, until it succeeds or this bridge closes. Same shape as {@code KafkaCloudEventBridge}'s own default,
     * see its class javadoc, and {@link Builder#commitRetryStrategy(RetryStrategy)} for how to replace it.
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
     * @param feed           The feed this bridge calls {@link DomainEventFeed#acceptCloudEvent(CloudEvent)} on.
     */
    public static <E> Builder<E> builder(Map<String, Object> consumerConfig, DomainEventFeed<E> feed) {
        return new Builder<>(consumerConfig, feed);
    }

    private void runLoop() {
        try {
            while (running) {
                try {
                    boolean shouldConsume = feed.hasProjection() && feed.isReadyForLiveDelivery();
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
                        // See KafkaCloudEventBridge's own runLoop for why this rewind matters. pause(...) above can
                        // only pause an assignment that already exists, and the very first poll() of a fresh
                        // Consumer is what creates that assignment. A record fetched in that same call, before
                        // pause ever had a chance to apply, must not be silently dropped, since poll() already
                        // advanced this Consumer's own read position past it.
                        seekToEarliestFetched(records);
                        continue;
                    }
                    if (!processBatch(records)) {
                        break; // A permanent stop happened this batch. The finally below closes the Consumer.
                    }
                } catch (RuntimeException e) {
                    // A commitSync failure, retriable or not, is already handled inside processBatch and never
                    // reaches here. This catches everything else, poll() itself throwing (an authentication error,
                    // most notably) or a throw from the per-record loop (already rewound by processBatch before
                    // rethrowing), so one bad iteration never kills the only consume thread. Backs off for
                    // pollTimeout first, since a persistent poll() failure would otherwise spin this loop as fast
                    // as the JVM allows instead of at the cadence every other path through this loop already
                    // keeps to.
                    if (running) {
                        log.warn("The Kafka consume loop for group \"{}\" failed this iteration. Retrying after pollTimeout.",
                                consumer.groupMetadata().groupId(), e);
                        sleepUninterruptibly(pollTimeout);
                    }
                }
            }
        } finally {
            // Only this thread ever closes the Consumer, see the class javadoc. Reached whether the loop above
            // exits because running turned false or because of the permanent-stop break. A permanent stop forces
            // LEAVE_GROUP so a static member (group.instance.id configured) still departs immediately, since
            // nothing is coming back to reclaim its assignment, unlike an ordinary close of the same bridge.
            try {
                if (permanentlyStopped) {
                    consumer.close(CloseOptions.timeout(closeTimeout)
                            .withGroupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP));
                } else {
                    consumer.close(closeTimeout);
                }
            } catch (RuntimeException e) {
                log.warn("Failed to close the Kafka consumer cleanly during shutdown.", e);
            }
            // A permanent stop never calls close() itself, nothing is coming back to trigger it, so the parking
            // producer failureAction owns would otherwise leak until some other caller happens to close this
            // bridge. Closed here too, independently of the Consumer close above, so one failing does not skip
            // the other. KafkaDeliveryFailureAction#close() already does nothing on a second call, so close()
            // calling it again afterward, on an ordinary shutdown, is harmless.
            failureAction.close();
        }
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

    // Returns false when a permanent stop occurred this batch. running is now false and the loop must exit rather
    // than poll again, closing the Consumer from its own finally as it does. Whatever resolved before the
    // permanent-stop trigger, in this partition or any other, is still committed first, exactly as an ordinary
    // REDELIVER failure would leave it. That commit failing no longer loses the permanent stop, it is applied
    // below regardless, since the triggering record's own seek already happened either way and nothing about
    // stopping for good depends on whether an unrelated, already-resolved record in the same batch got committed.
    private boolean processBatch(ConsumerRecords<String, byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        boolean permanentStop = false;
        boolean seekBackHappened = false;
        try {
            for (TopicPartition partition : records.partitions()) {
                for (ConsumerRecord<String, byte[]> record : records.records(partition)) {
                    HandleResult result = handleRecord(record, toCommit);
                    if (result == HandleResult.PERMANENT_STOP) {
                        permanentStop = true;
                        consumer.seek(partition, record.offset());
                        break;
                    } else if (result == HandleResult.REDELIVER) {
                        consumer.seek(partition, record.offset());
                        seekBackHappened = true;
                        break;
                    }
                }
            }
        } catch (RuntimeException e) {
            // A throw here, a failing seek most often, means some record in this batch was never handed to the
            // feed at all, on this partition or any partition the loop had not reached yet, while poll() already
            // advanced every partition's position regardless of whether the loop ever got to it. Rewinding every
            // partition this batch touched back to its own earliest fetched record undoes that advance, so the
            // next poll() refetches and reprocesses this whole batch from the start instead of silently skipping
            // whatever was never handled.
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
        if (permanentStop) {
            running = false;
            permanentlyStopped = true;
            return false;
        }
        if (seekBackHappened && toCommit.isEmpty()) {
            // A poison record. Every partition that fetched anything ended in a seek back to the exact offset it
            // started this poll at, so the next poll() would refetch and redeliver the identical record
            // immediately. Backing off for pollTimeout keeps that at this loop's normal cadence instead of
            // spinning it at the JVM's own maximum rate against a record that can never resolve. Skipped when
            // this batch is stopping for good instead, since nothing gains from delaying an exit already
            // underway.
            sleepUninterruptibly(pollTimeout);
        }
        return true;
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

    private HandleResult handleRecord(ConsumerRecord<String, byte[]> record, Map<TopicPartition, OffsetAndMetadata> toCommit) {
        CloudEvent cloudEvent;
        try {
            cloudEvent = KafkaCloudEventMapper.toCloudEvent(record);
        } catch (RuntimeException e) {
            log.warn("Failed to rebuild a CloudEvent from a record on topic \"{}\" partition {} offset {}.",
                    record.topic(), record.partition(), record.offset(), e);
            return toHandleResult(record, toCommit, failureAction.applyToUndecodable(record));
        }

        RoutingOutcome outcome;
        try {
            outcome = feed.acceptCloudEvent(cloudEvent);
        } catch (UnreadableLiveFilterException e) {
            log.error("The registration on this feed has a data payload filter this feed cannot answer live. This " +
                    "is a permanent configuration error; stopping this bridge and leaving its consumer group " +
                    "rather than redelivering into the same failure. The triggering record's offset is left " +
                    "uncommitted so it survives for the next consumer once the registration is fixed. Register a " +
                    "new DomainEventFeed with a Filter that does not reference the field, or with a " +
                    "DataFieldReader that can read it.", e);
            return HandleResult.PERMANENT_STOP;
        } catch (RuntimeException | AssertionError e) {
            // Either the projection handler itself threw, or the narrow registeredProjection() race the class
            // javadoc describes (an IllegalStateException that is not an UnreadableLiveFilterException). Both are
            // ordinary failure-policy cases, unlike the permanent one caught above.
            return toHandleResult(record, toCommit, failureAction.apply(record, cloudEvent));
        }
        if (outcome == RoutingOutcome.DELIVERED || outcome == RoutingOutcome.FILTERED) {
            stage(record, toCommit);
            return HandleResult.RESOLVED;
        }
        return toHandleResult(record, toCommit, failureAction.apply(record, cloudEvent));
    }

    private static HandleResult toHandleResult(ConsumerRecord<String, byte[]> record, Map<TopicPartition, OffsetAndMetadata> toCommit,
                                                 KafkaDeliveryFailureAction.Outcome outcome) {
        if (outcome == KafkaDeliveryFailureAction.Outcome.RESOLVED) {
            stage(record, toCommit);
            return HandleResult.RESOLVED;
        }
        return HandleResult.REDELIVER;
    }

    private static void stage(ConsumerRecord<String, byte[]> record, Map<TopicPartition, OffsetAndMetadata> toCommit) {
        toCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
    }

    /**
     * Signals the poll loop to stop, if a permanent stop has not already done so, and waits up to
     * {@link Builder#closeTimeout(Duration)} for it to finish, then closes the parking producer this bridge
     * created, if {@link DeliveryFailurePolicy#PARK} was configured. Never closes the {@code Consumer} itself. See
     * the class javadoc for why, and for what a loop thread still busy past the wait means for when the
     * {@code Consumer} actually closes.
     * <p>
     * Called from the loop thread itself, most often a projection reacting to what it was just delivered by
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
            // Already closed by a permanent stop's own loop-thread finally. Nothing left to wake up.
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

    public static final class Builder<E> {
        private final Map<String, Object> consumerConfig;
        private final DomainEventFeed<E> feed;
        private @Nullable DestinationResolver<KafkaDestination> resolver;
        private @Nullable SubscriptionFilter bindingFilter;
        private @Nullable Set<KafkaDestination> bindings;
        private DeliveryFailurePolicy deliveryFailurePolicy = DeliveryFailurePolicy.REDELIVER;
        private @Nullable KafkaDestination parkingDestination;
        private Duration pollTimeout = Duration.ofSeconds(1);
        private Duration closeTimeout = Duration.ofSeconds(30);
        private RetryStrategy commitRetryStrategy = defaultCommitRetryStrategy();

        private Builder(Map<String, Object> consumerConfig, DomainEventFeed<E> feed) {
            requireNonNull(consumerConfig, "consumerConfig cannot be null");
            this.consumerConfig = new HashMap<>(consumerConfig);
            this.feed = requireNonNull(feed, DomainEventFeed.class.getSimpleName() + " cannot be null");
        }

        /**
         * Derives the subscribed topics from {@link #bindingFilter(SubscriptionFilter)} or, absent one, from
         * {@link DestinationResolver#catchAllDestination()}. Required unless {@link #bindings(Set)} is given.
         */
        public Builder<E> resolver(DestinationResolver<KafkaDestination> resolver) {
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * Narrows the subscribed topics to {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for this
         * filter, falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot derive
         * one. Requires {@link #resolver(DestinationResolver)}. Per ADR 133 decision 5, this filter narrows what
         * arrives. It must be at least as inclusive as the registered projection's own replay filter, or events the
         * projection would have accepted never arrive at all.
         */
        public Builder<E> bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Subscribes to exactly these destinations instead of deriving any from a resolver, the explicit escape
         * hatch for a subscription scheme a resolver cannot express. Only {@code topic()} and
         * {@code topicIsPattern()} are read. A key or headers on a given destination are ignored. Every destination
         * must agree on {@link KafkaDestination#topicIsPattern()}. {@link #build()} refuses a set mixing literal
         * and pattern-typed destinations.
         */
        public Builder<E> bindings(Set<KafkaDestination> bindings) {
            this.bindings = Set.copyOf(requireNonNull(bindings, "bindings cannot be null"));
            return this;
        }

        /**
         * What this bridge does with a record it will not commit. {@link DeliveryFailurePolicy#REDELIVER} by
         * default. Never consulted for {@link UnreadableLiveFilterException}, see the class javadoc.
         */
        public Builder<E> onDeliveryFailure(DeliveryFailurePolicy deliveryFailurePolicy) {
            this.deliveryFailurePolicy = requireNonNull(deliveryFailurePolicy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * The destination {@link DeliveryFailurePolicy#PARK} publishes a failed delivery to. Required when
         * {@link #onDeliveryFailure(DeliveryFailurePolicy)} is {@code PARK} ({@link #build()} refuses otherwise).
         */
        public Builder<E> parkingDestination(KafkaDestination parkingDestination) {
            this.parkingDestination = requireNonNull(parkingDestination, "parkingDestination cannot be null");
            return this;
        }

        /**
         * How long each {@code poll()} call blocks waiting for records, and, since this bridge's coarse lifecycle
         * gate is rechecked once per loop iteration rather than on a separate schedule, also how often that
         * recheck happens. One second by default. See {@code KafkaCloudEventBridge.Builder#pollTimeout(Duration)}'s
         * own javadoc for why the two share one setting.
         */
        public Builder<E> pollTimeout(Duration pollTimeout) {
            requireNonNull(pollTimeout, "pollTimeout cannot be null");
            if (pollTimeout.toMillis() <= 0) {
                throw new IllegalArgumentException("pollTimeout must be at least 1 millisecond, was " + pollTimeout);
            }
            this.pollTimeout = pollTimeout;
            return this;
        }

        /**
         * How long {@link KafkaDomainEventBridge#close()} waits for the poll loop thread to finish before
         * returning anyway. Thirty seconds by default. Only that thread ever closes the {@code Consumer}, see the
         * class javadoc, so a loop thread still busy past this wait, most often a slow handler, means
         * {@code close()} returns before the {@code Consumer} is actually closed. It still closes as soon as that
         * work finishes.
         */
        public Builder<E> closeTimeout(Duration closeTimeout) {
            requireNonNull(closeTimeout, "closeTimeout cannot be null");
            if (closeTimeout.toMillis() <= 0) {
                throw new IllegalArgumentException("closeTimeout must be at least 1 millisecond, was " + closeTimeout);
            }
            this.closeTimeout = closeTimeout;
            return this;
        }

        /**
         * How a retriable {@code commitSync} failure is retried. Exponential backoff from 100 ms up to 2 seconds by
         * default, attempts uncapped until this bridge closes, the same shape {@code KafkaCloudEventBridge.Builder}'s
         * own {@code commitRetryStrategy(RetryStrategy)} defaults to. Retries only a {@code commitSync} failure
         * Kafka itself marks {@code org.apache.kafka.common.errors.RetriableException}, a broker outage most often.
         * Passing a {@link RetryStrategy} here replaces that predicate too, so a caller that wants a narrower or
         * wider retry configures its own.
         */
        public Builder<E> commitRetryStrategy(RetryStrategy commitRetryStrategy) {
            this.commitRetryStrategy = requireNonNull(commitRetryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        public KafkaDomainEventBridge<E> build() {
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
                KafkaDomainEventBridge<E> bridge = new KafkaDomainEventBridge<>(consumer, feed, pollTimeout, closeTimeout, commitRetryStrategy, failureAction, groupId.toString());
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
