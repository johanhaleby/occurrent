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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnreadableLiveFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Bridges a Kafka topic into a {@link DomainEventFeed}, the domain-level consume side ADR 133 decision 5 describes.
 * Rebuilds each record as a {@link CloudEvent} through {@link KafkaCloudEventMapper} and calls
 * {@link DomainEventFeed#acceptCloudEvent(CloudEvent)}, which is where the matching, the decoding and the delivery
 * all happen; this bridge does no filtering of its own, since the feed is the only thing that can decide per ADR 133
 * decision 5.
 * <p>
 * <strong>Acknowledgement</strong> follows the {@link RoutingOutcome} {@code acceptCloudEvent(...)} returns, exactly
 * as {@code KafkaCloudEventBridge} follows the one its own model reports: {@link RoutingOutcome#DELIVERED} or
 * {@link RoutingOutcome#FILTERED} stages this record's offset for the next commit, {@link RoutingOutcome#NOT_DELIVERABLE}
 * and a thrown exception both apply this bridge's configured {@link DeliveryFailurePolicy} instead.
 * <p>
 * <strong>{@link UnreadableLiveFilterException} is different, and permanent.</strong> It means the projection this
 * feed carries was registered with a {@code data} payload filter this feed has no
 * {@link org.occurrent.filtermatching.DataFieldReader} for, a configuration error that cannot change without a new
 * registration, and the same exception instance is thrown again on every later call. On catching it, this bridge
 * logs the failure, seeks the consumer back to the triggering record exactly as a {@link DeliveryFailurePolicy#REDELIVER}
 * failure would, commits whatever else resolved in the same poll (other partitions, and earlier records in the same
 * partition), and then <strong>closes its own {@code Consumer} and stops for good</strong>, rather than committing
 * past the triggering record or looping the poll again. Closing here, immediately, is deliberate: a
 * {@code Consumer} that keeps its assignment but stops polling is evicted from the group only after
 * {@code max.poll.interval.ms} (five minutes by default), which would leave this permanent, intentional stop
 * indistinguishable from a hung consumer for that whole window, log noise and a pointless rebalance included.
 * Closing sends Kafka's own clean group-departure request immediately instead, so the next consumer in this group
 * picks up starting exactly at the triggering record's offset, the same one this bridge seeked back to and never
 * committed past. The triggering record is not requeued anywhere and not parked; parking would still publish and
 * then commit past it, and this must never commit past it at all. It stays exactly where the last successful commit
 * left it until an operator fixes the registration and starts a new bridge, or a rebalance hands this group's
 * partitions to another consumer, so the event survives rather than being lost.
 * <p>
 * <strong>One dedicated thread owns the {@code Consumer} end to end</strong>, unlike {@code RabbitMqDomainEventBridge}'s
 * split between a scheduler thread and an AMQP callback thread. A Kafka {@code Consumer} is not thread-safe, so this
 * bridge runs one loop, on one thread, that polls, decides the coarse lifecycle gate, feeds the feed, and commits.
 * See {@code KafkaCloudEventBridge}'s class javadoc for why {@link Builder#pollTimeout(Duration)} serves both the
 * poll bound and the lifecycle recheck cadence, and for the commit-batching design and what a crash between a
 * poll's deliveries and its commit costs; both apply here unchanged.
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
 * <strong>Ordering.</strong> See {@code KafkaCloudEventBridge}'s own class javadoc; the same caveat applies here
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
    private final KafkaDeliveryFailureAction failureAction;
    private final Thread loopThread;
    private final AtomicBoolean consumerClosed = new AtomicBoolean(false);

    private volatile boolean running = true;

    private KafkaDomainEventBridge(KafkaConsumer<String, byte[]> consumer, DomainEventFeed<E> feed, Duration pollTimeout,
                                    KafkaDeliveryFailureAction failureAction, String groupId) {
        this.consumer = consumer;
        this.feed = feed;
        this.pollTimeout = pollTimeout;
        this.failureAction = failureAction;
        this.loopThread = new Thread(this::runLoop, "kafka-domainevent-bridge-" + groupId);
        this.loopThread.setDaemon(true);
    }

    /**
     * @param consumerConfig Kafka consumer configuration, {@code bootstrap.servers} and {@code group.id} at
     *                       minimum. Read once, at {@link Builder#build()}, to construct and own this bridge's own
     *                       {@code Consumer}. Refused when {@code group.id} is absent, and when
     *                       {@code enable.auto.commit} is anything other than exactly {@code "false"}, per ADR 133:
     *                       seeking only works if nothing else commits.
     * @param feed           The feed this bridge calls {@link DomainEventFeed#acceptCloudEvent(CloudEvent)} on.
     */
    public static <E> Builder<E> builder(Map<String, Object> consumerConfig, DomainEventFeed<E> feed) {
        return new Builder<>(consumerConfig, feed);
    }

    private void runLoop() {
        try {
            while (running) {
                boolean shouldConsume = feed.hasProjection() && feed.isReadyForLiveDelivery();
                reconcilePauseResume(shouldConsume);
                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(pollTimeout);
                } catch (WakeupException e) {
                    continue; // running is re-checked at the top of the loop; false here means close() woke it up to exit.
                }
                if (records.isEmpty()) {
                    continue;
                }
                if (!shouldConsume) {
                    // See KafkaCloudEventBridge's own runLoop for why this rewind matters: pause(...) above can
                    // only pause an assignment that already exists, and the very first poll() of a fresh Consumer
                    // is what creates that assignment. A record fetched in that same call, before pause ever had a
                    // chance to apply, must not be silently dropped, since poll() already advanced this Consumer's
                    // own read position past it.
                    seekToEarliestFetched(records);
                    continue;
                }
                if (!processBatch(records)) {
                    break; // A permanent stop happened this batch; the Consumer is already closed.
                }
            }
        } catch (RuntimeException e) {
            if (running) {
                log.error("The Kafka consume loop for group \"{}\" stopped unexpectedly.", consumer.groupMetadata().groupId(), e);
            }
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

    private void seekToEarliestFetched(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition partition : records.partitions()) {
            consumer.seek(partition, records.records(partition).get(0).offset());
        }
    }

    // Returns false when a permanent stop occurred this batch: the Consumer is already closed and the loop must
    // exit rather than poll again. Whatever resolved before the permanent-stop trigger, in this partition or any
    // other, is still committed first, exactly as an ordinary REDELIVER failure would leave it.
    private boolean processBatch(ConsumerRecords<String, byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        boolean permanentStop = false;
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, byte[]> record : records.records(partition)) {
                HandleResult result = handleRecord(record, toCommit);
                if (result == HandleResult.PERMANENT_STOP) {
                    permanentStop = true;
                    consumer.seek(partition, record.offset());
                    break;
                } else if (result == HandleResult.REDELIVER) {
                    consumer.seek(partition, record.offset());
                    break;
                }
            }
        }
        if (!toCommit.isEmpty()) {
            consumer.commitSync(toCommit);
        }
        if (permanentStop) {
            running = false;
            closeConsumerOnce();
            return false;
        }
        return true;
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

    private void closeConsumerOnce() {
        if (consumerClosed.compareAndSet(false, true)) {
            try {
                consumer.close(Duration.ofSeconds(30));
            } catch (RuntimeException e) {
                log.warn("Failed to close the Kafka consumer cleanly during shutdown.", e);
            }
        }
    }

    /**
     * Stops the poll loop and closes the {@code Consumer} (and, with {@link DeliveryFailurePolicy#PARK}, the
     * parking producer) this bridge created, if an {@link UnreadableLiveFilterException} has not already done so.
     */
    @Override
    public void close() {
        running = false;
        try {
            consumer.wakeup();
        } catch (RuntimeException ignored) {
            // Already closed by a permanent stop; nothing left to wake up.
        }
        try {
            loopThread.join(Duration.ofSeconds(30).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closeConsumerOnce();
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
         * arrives; it must be at least as inclusive as the registered projection's own replay filter, or events the
         * projection would have accepted never arrive at all.
         */
        public Builder<E> bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Subscribes to exactly these destinations instead of deriving any from a resolver, the explicit escape
         * hatch for a subscription scheme a resolver cannot express. Only {@code topic()} and
         * {@code topicIsPattern()} are read; a key or headers on a given destination are ignored. Every destination
         * must agree on {@link KafkaDestination#topicIsPattern()}; {@link #build()} refuses a set mixing literal
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

        public KafkaDomainEventBridge<E> build() {
            if (bindings == null && resolver == null) {
                throw new IllegalStateException("A resolver(...), or explicit bindings(...), is required");
            }
            if (deliveryFailurePolicy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
                throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
            }
            Object groupId = consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
            if (groupId == null) {
                throw new IllegalStateException("consumerConfig must set \"" + ConsumerConfig.GROUP_ID_CONFIG +
                        "\", since this bridge's committed offsets, and its consume identity, are keyed by it. " +
                        "Absent, KafkaConsumer construction still succeeds and this fails later, invisibly, as an " +
                        "InvalidGroupIdException the first time this bridge tries to commit or poll.");
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
                KafkaDomainEventBridge<E> bridge = new KafkaDomainEventBridge<>(consumer, feed, pollTimeout, failureAction, groupId.toString());
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
