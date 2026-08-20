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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
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

/**
 * Bridges a Kafka topic into a {@link PushSubscriptionModel}, the CloudEvent-level consume side ADR 133 decision 1
 * describes. Rebuilds each record as a {@link CloudEvent} through {@link KafkaCloudEventMapper}, hands it to
 * {@link PushSubscriptionModel#accept(CloudEvent)}, and commits only once the {@link RoutingOutcome} that
 * {@code accept(...)} reported through a shared {@link RoutingOutcomeChannel} says the event was actually consumed.
 * <p>
 * <strong>Holds a {@link PushSubscriptionModel}, never a {@link CatchupThenPushSubscriptionModel}</strong>, for the
 * same reason {@code RabbitMqCloudEventBridge} does: ADR 133 decision 1 is explicit that a bridge feeds the live
 * model, not the catch-up wrapper in front of it.
 * <p>
 * <strong>Acknowledgement.</strong> {@code accept(...)} throwing (a handler exception, or a subscription filter
 * that failed to evaluate) never commits. A normal return with {@link RoutingOutcome#DELIVERED} or
 * {@link RoutingOutcome#FILTERED} stages this record's offset for the next commit. A normal return with
 * {@link RoutingOutcome#NOT_DELIVERABLE} never does. In every case that does not commit, this bridge's configured
 * {@link DeliveryFailurePolicy} applies: {@link DeliveryFailurePolicy#REDELIVER} (the default) seeks the consumer
 * back to this record's offset, {@link DeliveryFailurePolicy#PARK} republishes to a parking destination and only
 * once that publish is confirmed treats this record as resolved, exactly as a delivered one.
 * <p>
 * <strong>One dedicated thread owns the {@code Consumer} end to end</strong>, unlike {@code RabbitMqCloudEventBridge}'s
 * split between a scheduler thread and an AMQP callback thread. A Kafka {@code Consumer} is not thread-safe, so this
 * bridge runs one loop, on one thread, that polls, decides the coarse lifecycle gate, feeds the model, and commits.
 * {@link Builder#pollTimeout(Duration)}'s own javadoc explains why one setting serves both the poll bound and the
 * lifecycle recheck cadence.
 * <p>
 * <strong>Commit batching, and what a crash costs.</strong> Every record in one {@code poll()} batch that resolves
 * (delivered, filtered, or a confirmed park) stages {@code record.offset() + 1} for its partition; one
 * {@code commitSync(Map)} call commits every partition that made progress once the whole batch is walked, never the
 * no-argument form. A crash between processing a poll's records and that batch commit redelivers whatever prefix of
 * that poll's records already resolved but was not yet committed. That is a replay of already-succeeded work, not a
 * skip: at-least-once delivery already requires every handler here to tolerate a repeat, and the batching only
 * changes how much of a poll gets redelivered after a crash, never whether an unresolved record could be skipped.
 * <p>
 * <strong>Per-partition failure isolation.</strong> A record that does not resolve makes the consumer {@code seek}
 * back to that record's offset and this bridge stops processing that partition's remaining records for this poll,
 * so a later record in the same partition is never committed past the one that failed. Other partitions in the same
 * poll are unaffected, since their offsets are independent.
 * <p>
 * <strong>Coarse lifecycle.</strong> Before every poll, this bridge reads {@link PushSubscriptionModel#subscriptionIds()}
 * and {@link PushSubscriptionModel#isRunning(String)} and pauses or resumes its own assignment to match: fetching
 * records while the model has a running subscription, not fetching otherwise. This is deliberately coarse, a small
 * delay either way is harmless, and it exists so this bridge never feeds a stopped or paused model, which per ADR 85
 * and ADR 104 drops the event rather than holding it. {@code poll()} still runs while paused, since a paused consumer
 * has to keep polling to heartbeat and complete rebalances; Kafka simply returns no records for paused partitions.
 * Never used to decide a single record; that decision comes from the {@link RoutingOutcome} above. Pausing can only
 * pause an assignment that already exists, and the assignment itself is only created by a {@code poll()} call, so a
 * fresh {@code Consumer}'s very first fetch can still return records before pausing has ever had a chance to apply.
 * When that happens this bridge seeks every affected partition back to the earliest record that poll returned,
 * rather than silently dropping what it already fetched but was never entitled to feed the model.
 * <p>
 * <strong>Ordering.</strong> A partitioned topic gives no global order. Two events on different partitions can be
 * processed in either order by this bridge, whatever their publish order was. Events for one stream stay in order
 * against each other only when the publisher keyed by stream id onto one partition, which
 * {@code KafkaSharedTopicDestinationResolver} (the shipped default) does. A projection that folds per stream is
 * fine under that default. One that depends on order across streams is not, and needs a single partition or a
 * different feed.
 */
public final class KafkaCloudEventBridge implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaCloudEventBridge.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final PushSubscriptionModel model;
    private final RoutingOutcomeChannel outcomeChannel;
    private final Duration pollTimeout;
    private final KafkaDeliveryFailureAction failureAction;
    private final Thread loopThread;

    private volatile boolean running = true;

    private KafkaCloudEventBridge(KafkaConsumer<String, byte[]> consumer, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel,
                                   Duration pollTimeout, KafkaDeliveryFailureAction failureAction, String groupId) {
        this.consumer = consumer;
        this.model = model;
        this.outcomeChannel = outcomeChannel;
        this.pollTimeout = pollTimeout;
        this.failureAction = failureAction;
        this.loopThread = new Thread(this::runLoop, "kafka-cloudevent-bridge-" + groupId);
        this.loopThread.setDaemon(true);
    }

    /**
     * @param consumerConfig Kafka consumer configuration, {@code bootstrap.servers} and {@code group.id} at
     *                       minimum. Read once, at {@link Builder#build()}, to construct and own this bridge's own
     *                       {@code Consumer}. Refused when {@code group.id} is absent, and when
     *                       {@code enable.auto.commit} is anything other than exactly {@code "false"}, per ADR 133:
     *                       seeking only works if nothing else commits.
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
                boolean shouldConsume = shouldConsume();
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
                    // reconcilePauseResume(...) above can only pause an assignment that already exists, and the
                    // very first poll() of a fresh Consumer is what creates that assignment in the first place. A
                    // record fetched in that same call, before pause has ever had a chance to apply, must not be
                    // silently dropped: poll() already advanced this Consumer's own read position past it, so
                    // without a seek back here it would never be offered to this bridge again, without ever having
                    // been committed either. Rewinding every affected partition to its earliest fetched record
                    // undoes that advance, so the next poll() (once genuinely paused) simply refetches it later.
                    seekToEarliestFetched(records);
                    continue;
                }
                processBatch(records);
            }
        } catch (RuntimeException e) {
            if (running) {
                log.error("The Kafka consume loop for group \"{}\" stopped unexpectedly.", consumer.groupMetadata().groupId(), e);
            }
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

    private void seekToEarliestFetched(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition partition : records.partitions()) {
            consumer.seek(partition, records.records(partition).get(0).offset());
        }
    }

    private void processBatch(ConsumerRecords<String, byte[]> records) {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, byte[]> record : records.records(partition)) {
                if (!handleRecord(record, toCommit)) {
                    consumer.seek(partition, record.offset());
                    break; // Stop this partition's remaining records for this poll; other partitions are unaffected.
                }
            }
        }
        if (!toCommit.isEmpty()) {
            consumer.commitSync(toCommit);
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
     * Stops the poll loop and closes the {@code Consumer} (and, with {@link DeliveryFailurePolicy#PARK}, the
     * parking producer) this bridge created.
     */
    @Override
    public void close() {
        running = false;
        consumer.wakeup();
        try {
            loopThread.join(Duration.ofSeconds(30).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            consumer.close(Duration.ofSeconds(30));
        } catch (RuntimeException ignored) {
            // Best effort: this bridge is tearing down either way.
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
         * arrives; it must be at least as inclusive as the subscription's own filter, or events the subscription
         * would have accepted never arrive at all.
         */
        public Builder bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Subscribes to exactly these destinations instead of deriving any from a resolver, the explicit escape
         * hatch for a subscription scheme a resolver cannot express. Only {@code topic()} and
         * {@code topicIsPattern()} are read; a key or headers on a given destination are ignored, since a Kafka
         * subscription has no per-message components. Every destination must agree on
         * {@link KafkaDestination#topicIsPattern()}; {@link #build()} refuses a set mixing literal and
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
         * even while paused, to heartbeat and complete rebalances, so the two could not be split into separate
         * knobs without this bridge running two competing timers against the same single-threaded {@code Consumer}.
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

        public KafkaCloudEventBridge build() {
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
                KafkaCloudEventBridge bridge = new KafkaCloudEventBridge(consumer, model, outcomeChannel, pollTimeout, failureAction, groupId.toString());
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
