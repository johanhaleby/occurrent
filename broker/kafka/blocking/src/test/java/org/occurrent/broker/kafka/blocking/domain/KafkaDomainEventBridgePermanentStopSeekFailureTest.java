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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.kafka.blocking.KafkaDeliveryFailureAction;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code KafkaDomainEventBridge.processBatch(..)}'s permanent-stop branch must set {@code running = false;
 * permanentlyStopped = true;} before it even attempts to seek the consumer back to the triggering record, not
 * after. A seek can throw, a rebalance that revoked the partition at the exact moment the record was permanently
 * refused, most often, and the surrounding {@code catch (RuntimeException e) { seekToEarliestFetched(records);
 * throw e; }} rewinds and rethrows the whole batch. Set only after the seek, that rethrow would happen before
 * {@code running}/{@code permanentlyStopped} were ever set, silently losing the permanent stop: the bridge's own
 * {@code runLoop} would log a warning, sleep {@code pollTimeout}, and poll again, refetching the exact same
 * permanently-unreadable record forever instead of stopping for good.
 * <p>
 * Invokes {@code processBatch(..)} directly (the same reflective, mocked-{@code KafkaConsumer} technique
 * {@code KafkaDomainEventBridgePermanentStopTest} already uses) with the permanent-stop trigger's own {@code seek}
 * call stubbed to throw, and asserts that even though this one call fails and its exception propagates, the
 * bridge still ends up permanently stopped ({@code running == false}, {@code permanentlyStopped == true}), never
 * silently reverting to "still running" for a record that can never become readable.
 */
class KafkaDomainEventBridgePermanentStopSeekFailureTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);

    @Test
    @Timeout(10)
    void a_seek_failure_on_the_permanent_stop_trigger_still_leaves_the_bridge_permanently_stopped() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("test-group"));
        // The permanent-stop trigger's own seek call, made to throw as if a rebalance took the partition away at
        // the exact moment this bridge tried to seek back to the record it is about to permanently refuse.
        doThrow(new RuntimeException("simulated seek failure on the permanent-stop path")).when(consumer).seek(eq(PARTITION_0), eq(7L));

        DomainEventFeed<String> feed = new DomainEventFeed<>(new InMemoryEventStore(), new IdentityConverter(), orderId -> orderId);
        // A data-field filter with the default (refusing) DataFieldReader: refused live (UnreadableLiveFilterException,
        // the permanent-stop trigger) on the first record that reaches this registration.
        feed.register("proj", orderId -> {
        }, Filter.data("amount", Condition.eq(42)));
        feed.goLive("proj");

        KafkaDeliveryFailureAction failureAction = KafkaDeliveryFailureAction.create(
                Map.of(), DeliveryFailurePolicy.REDELIVER, null, LoggerFactory.getLogger(getClass()));
        KafkaDomainEventBridge<String> bridge = bridgeForTesting(consumer, feed, failureAction);

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(decodableRecord(PARTITION_0, 7L)));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        assertThatThrownBy(() -> invokeProcessBatch(bridge, records))
                .as("the seek failure itself is expected to propagate, the same as any other failing seek")
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("simulated seek failure");

        assertThat(readBooleanField(bridge, "permanentlyStopped"))
                .as("a seek failure on the permanent-stop trigger's own rewind must not silently undo the "
                        + "permanent stop; the record that caused it can never become readable and must not be "
                        + "retried forever")
                .isTrue();
        assertThat(readBooleanField(bridge, "running"))
                .as("the poll loop must not keep running after a permanent-stop trigger, even if its own seek failed")
                .isFalse();
    }

    private static ConsumerRecord<String, byte[]> decodableRecord(TopicPartition partition, long offset) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_id", "id-1".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_source", "urn:test".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_type", "com.acme.OrderPlaced".getBytes(StandardCharsets.UTF_8));
        byte[] value = "order-1".getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, ConsumerRecord.NO_TIMESTAMP,
                TimestampType.CREATE_TIME, ConsumerRecord.NULL_SIZE, value.length, "stream-1", value, headers, Optional.empty());
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

    private static void invokeProcessBatch(KafkaDomainEventBridge<String> bridge, ConsumerRecords<String, byte[]> records) throws Exception {
        try {
            Method method = KafkaDomainEventBridge.class.getDeclaredMethod("processBatch", ConsumerRecords.class);
            method.setAccessible(true);
            method.invoke(bridge, records);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw e;
        }
    }

    private static boolean readBooleanField(KafkaDomainEventBridge<String> bridge, String fieldName) throws Exception {
        Field field = KafkaDomainEventBridge.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getBoolean(bridge);
    }

    private static Throwable unwrap(ReflectiveOperationException e) {
        return e instanceof InvocationTargetException invocationTargetException ? invocationTargetException.getTargetException() : e;
    }

    private static final class IdentityConverter implements org.occurrent.application.converter.CloudEventConverter<String> {
        @Override
        public io.cloudevents.CloudEvent toCloudEvent(String domainEvent) {
            return io.cloudevents.core.builder.CloudEventBuilder.v1()
                    .withId(java.util.UUID.randomUUID().toString())
                    .withSource(java.net.URI.create("urn:test"))
                    .withType("com.acme.OrderPlaced")
                    .withDataContentType("text/plain")
                    .withData(domainEvent.getBytes(StandardCharsets.UTF_8))
                    .build();
        }

        @Override
        public String toDomainEvent(io.cloudevents.CloudEvent cloudEvent) {
            byte[] data = cloudEvent.getData() == null ? new byte[0] : cloudEvent.getData().toBytes();
            return new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public String getCloudEventType(Class<? extends String> type) {
            return "com.acme.OrderPlaced";
        }
    }
}
