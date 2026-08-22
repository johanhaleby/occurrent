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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for {@link KafkaCloudEventBridge}'s poll loop with no other in-repo test, since the bridge
 * holds its {@code Consumer} privately and none of these failures (a seek call itself throwing mid-batch, a commit
 * that exhausts its retry, {@code close()}'s {@code wakeup()} landing mid-commit, a poison record on one partition
 * outpacing a healthy one on another) is something a real broker or Testcontainers can force on demand. Exercised
 * by constructing the bridge through its private constructor with a mocked {@link KafkaConsumer}, the same
 * reflective technique {@code KafkaCloudEventSinkRetryTest} already proved for the sink, and invoking its private
 * {@code processBatch(ConsumerRecords)} directly rather than driving a real poll loop thread.
 * {@code KafkaDomainEventBridge} shares this exact shape for every path covered here, so it is not duplicated.
 */
class KafkaCloudEventBridgeRewindTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    /**
     * The bug: an exception escaping the per-record loop, a failing {@code seek} most often, must rewind every
     * partition this batch touched, not only the one that failed. {@code poll()} already advanced every partition's
     * read position regardless of whether the loop ever reached it, so a partition never rewound here is silently
     * skipped rather than redelivered. Partition 1's record here always resolves cleanly (no subscription throws,
     * no seek call of its own in the ordinary path), so the only way it is ever seeked back to its own earliest
     * fetched offset is through the catch-all rewind, proving the fix regardless of which partition this batch
     * happens to process first.
     */
    @Test
    void an_exception_escaping_the_per_record_loop_rewinds_every_partition_this_batch_touched() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mockConsumer();
        // Partition 0's record is never deliverable (no subscription registered), so handleRecord seeks it back.
        // That seek is made to throw, simulating a rebalance taking the partition away mid-batch.
        doThrow(new RuntimeException("simulated rebalance")).when(consumer).seek(eq(PARTITION_0), eq(5L));

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        // No subscription is registered on model, so both records report NOT_DELIVERABLE and neither ever commits.
        // Only partition 0 throws on its own seek; partition 1 must still be safely rewound.
        KafkaCloudEventBridge bridge = bridgeForTesting(consumer, model, outcomeChannel);

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(record(PARTITION_0, 5L, "id-1")));
        batch.put(PARTITION_1, List.of(record(PARTITION_1, 7L, "id-2")));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        assertThatThrownBy(() -> invokeProcessBatch(bridge, records))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("simulated rebalance");

        verify(consumer).seek(PARTITION_1, 7L);
    }

    /**
     * The other half of the same design: a commit failure after the batch already resolved must never rewind
     * anything. Every partition's position is already exactly where it should be, past the last record this batch
     * actually handled, so a rewind here would redeliver the whole batch again on every poll for as long as the
     * commit keeps failing, forever under a persistent failure. {@code KafkaException} is not a
     * {@code RetriableException}, so the default {@code commitRetryStrategy} gives up after the first attempt.
     */
    @Test
    void a_commit_failure_after_the_batch_resolved_does_not_rewind_any_partition() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mockConsumer();
        doThrow(new KafkaException("simulated non-retriable commit failure")).when(consumer).commitSync(anyMapArg());

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
        });
        KafkaCloudEventBridge bridge = bridgeForTesting(consumer, model, outcomeChannel);

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(record(PARTITION_0, 5L, "id-1")));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        invokeProcessBatch(bridge, records);

        verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
    }

    /**
     * {@code close()} calling {@code wakeup()} while this thread is between blocking calls can raise a
     * {@code WakeupException} out of the very {@code commitSync} call for a batch that already fully resolved.
     * Losing that commit to a signal that was never actually about the commit itself would replay the whole batch
     * on the next start for no reason, so the loop retries the same commit once more instead of treating the
     * {@code WakeupException} as a failure. That retry uses the bounded {@code commitSync(Map, Duration)} overload
     * rather than the bare one, aligned to whatever close budget remains, so an unreachable broker cannot block
     * this retry past {@link KafkaCloudEventBridge.Builder#closeTimeout(Duration)} the way the bare overload's own
     * {@code default.api.timeout.ms} fallback could.
     */
    @Test
    void a_wakeup_during_commit_retries_the_same_commit_once_more_bounded_by_the_remaining_close_budget() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mockConsumer();
        doThrow(new WakeupException()).when(consumer).commitSync(anyMapArg());

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
        });
        Duration closeTimeout = Duration.ofSeconds(5);
        KafkaCloudEventBridge bridge = bridgeForTesting(consumer, model, outcomeChannel, closeTimeout);
        // Mirrors what close() itself does immediately before calling wakeup(), so the bounded retry below gets a
        // real, positive budget to work with rather than the unset Long.MAX_VALUE default.
        bridge.close();

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(record(PARTITION_0, 5L, "id-1")));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        invokeProcessBatch(bridge, records);

        verify(consumer, times(1)).commitSync(anyMapArg());
        ArgumentCaptor<Duration> timeoutCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(consumer).commitSync(anyMapArg(), timeoutCaptor.capture());
        assertThat(timeoutCaptor.getValue())
                .as("the retry's own timeout must stay within the close budget rather than falling back to " +
                        "the client's much larger default.api.timeout.ms")
                .isLessThanOrEqualTo(closeTimeout);
        verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
    }

    /**
     * A poison record on one partition must throttle only that partition, not the whole loop, or a healthy
     * partition committing offsets in the same poll gets slowed to the failing one's pace for no reason. The
     * seeked-back partition is paused here, immediately, rather than left for the next lifecycle-gate recheck to
     * notice, so the very next {@code poll()} already excludes it.
     */
    @Test
    void a_poison_record_pauses_only_its_own_partition_not_a_healthy_one_resolving_in_the_same_batch() throws Exception {
        KafkaConsumer<String, byte[]> consumer = mockConsumer();

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("sub", cloudEvent -> {
            if ("id-2".equals(cloudEvent.getId())) {
                throw new RuntimeException("simulated permanently failing handler");
            }
        });
        KafkaCloudEventBridge bridge = bridgeForTesting(consumer, model, outcomeChannel);

        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> batch = new LinkedHashMap<>();
        batch.put(PARTITION_0, List.of(record(PARTITION_0, 5L, "id-1")));
        batch.put(PARTITION_1, List.of(record(PARTITION_1, 7L, "id-2")));
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(batch);

        invokeProcessBatch(bridge, records);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<java.util.Collection<TopicPartition>> pausedCaptor = ArgumentCaptor.forClass(java.util.Collection.class);
        verify(consumer).pause(pausedCaptor.capture());
        assertThat(pausedCaptor.getValue())
                .as("only the partition whose record kept failing should be paused")
                .containsExactly(PARTITION_1);
    }

    private static Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> anyMapArg() {
        return any();
    }

    private static ConsumerRecord<String, byte[]> record(TopicPartition partition, long offset, String id) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_id", id.getBytes(StandardCharsets.UTF_8));
        headers.add("ce_source", "urn:test".getBytes(StandardCharsets.UTF_8));
        headers.add("ce_type", "com.acme.OrderPlaced".getBytes(StandardCharsets.UTF_8));
        byte[] value = "{}".getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, ConsumerRecord.NO_TIMESTAMP,
                TimestampType.CREATE_TIME, ConsumerRecord.NULL_SIZE, value.length, "stream-1", value, headers, Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static KafkaConsumer<String, byte[]> mockConsumer() {
        KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);
        // A warn log on the failure paths under test names the consumer group, so a bare mock's default null
        // answer for groupMetadata() would NPE before the assertion this test actually cares about ever runs.
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("test-group"));
        return consumer;
    }

    private static KafkaCloudEventBridge bridgeForTesting(KafkaConsumer<String, byte[]> consumer, PushSubscriptionModel model,
                                                            RoutingOutcomeChannel outcomeChannel) {
        return bridgeForTesting(consumer, model, outcomeChannel, Duration.ofSeconds(5));
    }

    private static KafkaCloudEventBridge bridgeForTesting(KafkaConsumer<String, byte[]> consumer, PushSubscriptionModel model,
                                                            RoutingOutcomeChannel outcomeChannel, Duration closeTimeout) {
        try {
            KafkaDeliveryFailureAction failureAction = KafkaDeliveryFailureAction.create(
                    Map.of(), DeliveryFailurePolicy.REDELIVER, null, LoggerFactory.getLogger(KafkaCloudEventBridgeRewindTest.class));
            Constructor<KafkaCloudEventBridge> constructor = KafkaCloudEventBridge.class.getDeclaredConstructor(
                    KafkaConsumer.class, PushSubscriptionModel.class, RoutingOutcomeChannel.class, Duration.class,
                    Duration.class, RetryStrategy.class, KafkaDeliveryFailureAction.class, String.class, Predicate.class);
            constructor.setAccessible(true);
            // readinessSource fixed at "always ready" here: none of this class's cases are about the catch-up
            // readiness gate, which KafkaCloudEventBridgeReadinessTest covers on its own, through the public
            // builder rather than reflection.
            Predicate<String> alwaysReady = subscriptionId -> true;
            return constructor.newInstance(consumer, model, outcomeChannel, Duration.ofSeconds(1), closeTimeout,
                    defaultCommitRetryStrategyForTesting(), failureAction, "test-group", alwaysReady);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not construct " + KafkaCloudEventBridge.class.getSimpleName() + " for testing", unwrap(e));
        }
    }

    private static RetryStrategy defaultCommitRetryStrategyForTesting() {
        try {
            Method method = KafkaCloudEventBridge.class.getDeclaredMethod("defaultCommitRetryStrategy");
            method.setAccessible(true);
            return (RetryStrategy) method.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read the default commit retry strategy for testing", unwrap(e));
        }
    }

    private static void invokeProcessBatch(KafkaCloudEventBridge bridge, ConsumerRecords<String, byte[]> records) throws Exception {
        try {
            Method method = KafkaCloudEventBridge.class.getDeclaredMethod("processBatch", ConsumerRecords.class);
            method.setAccessible(true);
            method.invoke(bridge, records);
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
}
