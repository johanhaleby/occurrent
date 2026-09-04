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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.kafka.blocking.KafkaDeliveryFailureAction;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A permanent stop ({@code UnreadableLiveFilterException}) commits whatever else resolved in the same poll before
 * departing, and that commit must be a single best-effort attempt rather than an unbounded retry, since the
 * permanent-stop flags are set before it runs. Without that ordering, a coordinator outage retries with
 * exponential backoff, uncapped, before this bridge ever gets to depart, leaving the promised immediate departure
 * waiting on Kafka availability.
 * <p>
 * Partition 0 carries an undecodable record parked through a mocked {@link Producer}, which resolves and stages
 * an offset without ever touching the feed. Partition 1 carries a decodable record on a registration whose filter
 * needs a {@code DataFieldReader} this feed does not have, so it is refused live on this, its first ever call for
 * the registration, the permanent-stop trigger. That combination is what makes "already resolved" and "permanent
 * stop" coexist in one batch, since a registration's live match answer, work or refuse, is decided once, on
 * whichever record reaches it first, and never changes afterward. Exercised the same way
 * {@code KafkaCloudEventBridgeRewindTest} exercises {@code KafkaCloudEventBridge}, a mocked {@link KafkaConsumer}
 * and the private constructor reached through reflection.
 */
class KafkaDomainEventBridgePermanentStopTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);
    private static final TopicPartition PARTITION_2 = new TopicPartition("test-topic", 2);

    @Test
    @Timeout(10)
    void a_permanent_stops_final_commit_is_a_single_best_effort_attempt_not_an_unbounded_retry() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("test-group"));
        // A coordinator outage: every commit attempt fails with a retriable error, persistent, exactly the shape
        // that would retry forever if this final commit were not already bounded by the permanent-stop flags.
        doThrow(new NotCoordinatorException("simulated coordinator outage")).when(consumer).commitSync(any(Map.class));

        DomainEventFeed<String> feed = new DomainEventFeed<>(new InMemoryEventStore(), new IdentityConverter(), orderId -> orderId);
        // A data-field filter with the default (refusing) DataFieldReader. Refused live on the first
        // acceptCloudEvent for this registration, whichever record reaches it first.
        feed.register("proj", orderId -> {
        }, Filter.data("amount", Condition.eq(42)));
        feed.goLive("proj");

        // A parking producer whose send() completes immediately, so the undecodable record on partition 0 resolves
        // deterministically without any real broker.
        @SuppressWarnings("unchecked")
        Producer<String, byte[]> parkingProducer = mock(Producer.class);
        when(parkingProducer.send(any())).thenReturn(CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("parking-topic", 0), 0L, 0, 0L, 0, 0)));
        KafkaDeliveryFailureAction failureAction = failureActionForTesting(
                DeliveryFailurePolicy.PARK, parkingProducer, KafkaDestination.of("parking-topic"));

        KafkaDomainEventBridge<String> bridge = bridgeForTesting(consumer, feed, failureAction);

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(undecodableRecord(PARTITION_0, 5L)));
        batch.put(PARTITION_1, List.of(decodableRecord(PARTITION_1, 7L, "id-1")));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        boolean shouldContinue = invokeProcessBatch(bridge, records);

        // processBatch returns false once permanentStop is set, the same signal the poll loop above it uses to stop
        // calling in. A regression that fell through to the generic catch instead of the UnreadableLiveFilterException
        // one would still commit once and leave this assertion the only thing catching the wrong branch.
        assertThat(shouldContinue).isFalse();

        // With the flags set before the commit, running already reads false when commitWithRetry's shutdown
        // predicate is evaluated, so the retry gives up after this one attempt instead of retrying the persistent
        // NotCoordinatorException with backoff, uncapped, forever.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCaptor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, times(1)).commitSync(commitCaptor.capture());
        // Only partition 0's undecodable record ever staged an offset. Partition 1 never reached the feed
        // successfully, so nothing on it belongs in this commit. A stray entry here would mean the permanent-stop
        // record itself got staged instead of triggering the stop.
        assertThat(commitCaptor.getValue())
                .containsExactly(Map.entry(PARTITION_0, new OffsetAndMetadata(6L)));
    }

    /**
     * The bug behind #948: once partition 1's record decides the permanent stop above, partition 2's own record,
     * later in the same batch, must never reach the feed at all. Before the fix, this loop kept walking the rest
     * of the batch into the same permanent refusal, one partition at a time, offering a feed that had already
     * given up a record it could only ever refuse again. Proven with a spy on the feed itself rather than on a
     * mocked {@code Consumer} call, since the defect was calling {@code acceptCloudEvent} at all, not any seek or
     * commit around it.
     */
    @Test
    @Timeout(10)
    void a_permanent_stop_mid_batch_never_reaches_the_feed_for_a_partition_it_had_not_yet_processed() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("test-group"));

        DomainEventFeed<String> realFeed = new DomainEventFeed<>(new InMemoryEventStore(), new IdentityConverter(), orderId -> orderId);
        realFeed.register("proj", orderId -> {
        }, Filter.data("amount", Condition.eq(42)));
        realFeed.goLive("proj");
        DomainEventFeed<String> feed = spy(realFeed);

        @SuppressWarnings("unchecked")
        Producer<String, byte[]> parkingProducer = mock(Producer.class);
        when(parkingProducer.send(any())).thenReturn(CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("parking-topic", 0), 0L, 0, 0L, 0, 0)));
        KafkaDeliveryFailureAction failureAction = failureActionForTesting(
                DeliveryFailurePolicy.PARK, parkingProducer, KafkaDestination.of("parking-topic"));

        KafkaDomainEventBridge<String> bridge = bridgeForTesting(consumer, feed, failureAction);

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(undecodableRecord(PARTITION_0, 5L)));
        batch.put(PARTITION_1, List.of(decodableRecord(PARTITION_1, 7L, "id-1")));
        batch.put(PARTITION_2, List.of(decodableRecord(PARTITION_2, 9L, "id-2")));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        boolean shouldContinue = invokeProcessBatch(bridge, records);

        assertThat(shouldContinue).isFalse();
        // Partition 0's record never decodes, so it never reaches the feed either. Only partition 1's record, the
        // one that actually triggered the permanent stop, may.
        verify(feed, times(1)).acceptCloudEvent(any());
        // Rewound to its own earliest fetched record instead, so the next consumer in this group refetches it,
        // rather than left at whatever position poll() already advanced it to.
        verify(consumer).seek(PARTITION_2, 9L);
    }

    private static ConsumerRecord<String, byte[]> decodableRecord(TopicPartition partition, long offset, String id) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_id", id.getBytes(StandardCharsets.UTF_8));
        headers.add("ce_source", "urn:test".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_type", "com.acme.OrderPlaced".getBytes(StandardCharsets.UTF_8));
        byte[] value = "order-1".getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, ConsumerRecord.NO_TIMESTAMP,
                TimestampType.CREATE_TIME, ConsumerRecord.NULL_SIZE, value.length, "stream-1", value, headers, Optional.empty());
    }

    // No ce_ headers at all, Kafka's own binary-mode prefix, so KafkaCloudEventMapper.toCloudEvent throws before
    // this record ever reaches the feed, the same shape KafkaCloudEventMapperTest already proves for the mapper
    // itself.
    private static ConsumerRecord<String, byte[]> undecodableRecord(TopicPartition partition, long offset) {
        byte[] value = "not a cloud event".getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, ConsumerRecord.NO_TIMESTAMP,
                TimestampType.CREATE_TIME, ConsumerRecord.NULL_SIZE, value.length, "stream-1", value, new RecordHeaders(), Optional.empty());
    }

    private static KafkaDeliveryFailureAction failureActionForTesting(DeliveryFailurePolicy policy, Producer<String, byte[]> parkingProducer,
                                                                        KafkaDestination parkingDestination) {
        try {
            Constructor<KafkaDeliveryFailureAction> constructor = KafkaDeliveryFailureAction.class.getDeclaredConstructor(
                    DeliveryFailurePolicy.class, Producer.class, KafkaDestination.class, org.slf4j.Logger.class);
            constructor.setAccessible(true);
            return constructor.newInstance(policy, parkingProducer, parkingDestination,
                    LoggerFactory.getLogger(KafkaDomainEventBridgePermanentStopTest.class));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not construct " + KafkaDeliveryFailureAction.class.getSimpleName() + " for testing", unwrap(e));
        }
    }

    private static KafkaDomainEventBridge<String> bridgeForTesting(KafkaConsumer<String, byte[]> consumer, DomainEventFeed<String> feed,
                                                                     KafkaDeliveryFailureAction failureAction) {
        try {
            Constructor<KafkaDomainEventBridge> constructor = KafkaDomainEventBridge.class.getDeclaredConstructor(
                    KafkaConsumer.class, DomainEventFeed.class, Duration.class, Duration.class, RetryStrategy.class,
                    KafkaDeliveryFailureAction.class, String.class);
            constructor.setAccessible(true);
            @SuppressWarnings("unchecked")
            KafkaDomainEventBridge<String> bridge = constructor.newInstance(consumer, feed, Duration.ofSeconds(1),
                    Duration.ofSeconds(5), defaultCommitRetryStrategyForTesting(), failureAction, "test-group");
            return bridge;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not construct " + KafkaDomainEventBridge.class.getSimpleName() + " for testing", unwrap(e));
        }
    }

    private static RetryStrategy defaultCommitRetryStrategyForTesting() {
        try {
            Method method = KafkaDomainEventBridge.class.getDeclaredMethod("defaultCommitRetryStrategy");
            method.setAccessible(true);
            return (RetryStrategy) method.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read the default commit retry strategy for testing", unwrap(e));
        }
    }

    private static boolean invokeProcessBatch(KafkaDomainEventBridge<String> bridge, ConsumerRecords<String, byte[]> records) throws Exception {
        try {
            Method method = KafkaDomainEventBridge.class.getDeclaredMethod("processBatch", ConsumerRecords.class);
            method.setAccessible(true);
            return (boolean) method.invoke(bridge, records);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw e;
        }
    }

    private static Throwable unwrap(ReflectiveOperationException e) {
        return e instanceof InvocationTargetException invocationTargetException ? invocationTargetException.getTargetException() : e;
    }

    private static final class IdentityConverter implements CloudEventConverter<String> {
        @Override
        public CloudEvent toCloudEvent(String domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:test"))
                    .withType("com.acme.OrderPlaced")
                    .withDataContentType("text/plain")
                    .withData(domainEvent.getBytes(StandardCharsets.UTF_8))
                    .build();
        }

        @Override
        public String toDomainEvent(CloudEvent cloudEvent) {
            byte[] data = cloudEvent.getData() == null ? new byte[0] : cloudEvent.getData().toBytes();
            return new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public String getCloudEventType(Class<? extends String> type) {
            return "com.acme.OrderPlaced";
        }
    }
}
