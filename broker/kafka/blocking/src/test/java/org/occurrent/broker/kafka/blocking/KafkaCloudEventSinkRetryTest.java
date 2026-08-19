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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.retry.RetryStrategy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link KafkaCloudEventSink#publish(CloudEvent)}'s acknowledgement wait, its retry loop, and
 * {@link KafkaCloudEventSink#close()}'s cancellation of the retry loop, exercised against a mocked {@link Producer}
 * rather than a real broker. A real broker cannot prove the acknowledgement wait deterministically either, since an
 * integration test can only poll for the message to arrive after {@code publish} returns, which a fire-and-forget
 * {@code publish} that never waited at all would also pass, because the message would still turn up during the poll
 * window. Forcing a {@link NotEnoughReplicasException} that resolves itself needs a multi-broker cluster a single
 * Testcontainers node cannot model, and forcing one that never resolves needs a producer whose failure never stops,
 * which a real broker cannot promise either. {@link KafkaCloudEventSink}'s constructor is private, on purpose, so
 * this file reaches it through reflection instead of adding a method production code in the same package could call
 * by mistake, the same reason {@code RabbitMqCloudEventSinkChannelRetirementTest} mocks a {@code Connection} and a
 * {@code Channel} instead of running its own broker.
 */
class KafkaCloudEventSinkRetryTest {

    private final KafkaDestination destination = KafkaDestination.of("test-topic");
    private final KafkaCloudEventSinkTest.FixedDestinationResolver resolver = new KafkaCloudEventSinkTest.FixedDestinationResolver(destination);

    private static KafkaCloudEventSink sinkForTesting(Producer<String, byte[]> producer, DestinationResolver<KafkaDestination> resolver, Duration acknowledgementTimeout, RetryStrategy retryStrategy) {
        try {
            Constructor<KafkaCloudEventSink> constructor = KafkaCloudEventSink.class.getDeclaredConstructor(
                    Producer.class, DestinationResolver.class, Duration.class, RetryStrategy.class);
            constructor.setAccessible(true);
            return constructor.newInstance(producer, resolver, acknowledgementTimeout, retryStrategy);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not construct " + KafkaCloudEventSink.class.getSimpleName() + " for testing", unwrap(e));
        }
    }

    private static RetryStrategy defaultRetryStrategyForTesting() {
        try {
            Method method = KafkaCloudEventSink.Builder.class.getDeclaredMethod("defaultRetryStrategy");
            method.setAccessible(true);
            return (RetryStrategy) method.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read the default retry strategy for testing", unwrap(e));
        }
    }

    private static Throwable unwrap(ReflectiveOperationException e) {
        return e instanceof InvocationTargetException invocationTargetException ? invocationTargetException.getTargetException() : e;
    }

    @Test
    void publish_stays_blocked_until_the_broker_acknowledges_the_send() throws Exception {
        Producer<String, byte[]> producer = mock(Producer.class);
        CompletableFuture<RecordMetadata> acknowledgement = new CompletableFuture<>();
        when(producer.send(any())).thenReturn(acknowledgement);

        KafkaCloudEventSink sink = sinkForTesting(producer, resolver, Duration.ofSeconds(5), defaultRetryStrategyForTesting());

        Thread publishing = new Thread(() -> sink.publish(orderPlaced("id-1")));
        publishing.start();
        publishing.join(Duration.ofMillis(300).toMillis());

        assertThat(publishing.isAlive())
                .as("publish should still be waiting on the acknowledgement future, which has not completed yet")
                .isTrue();

        acknowledgement.complete(new RecordMetadata(new TopicPartition("test-topic", 0), 0L, 0, 0L, 0, 0));
        publishing.join(Duration.ofSeconds(5).toMillis());

        assertThat(publishing.isAlive())
                .as("publish should have returned promptly once the future completed")
                .isFalse();
    }

    @Test
    void a_retriable_failure_is_retried_and_publish_eventually_succeeds() throws Exception {
        Producer<String, byte[]> producer = mock(Producer.class);

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> failingFuture = mock(Future.class);
        when(failingFuture.get(anyLong(), any()))
                .thenThrow(new ExecutionException(new NotEnoughReplicasException("expected, not enough in-sync replicas")));

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("test-topic", 0), 0L, 0, 0L, 0, 0);
        @SuppressWarnings("unchecked")
        Future<RecordMetadata> succeedingFuture = mock(Future.class);
        when(succeedingFuture.get(anyLong(), any())).thenReturn(recordMetadata);

        when(producer.send(any())).thenReturn(failingFuture, succeedingFuture);

        KafkaCloudEventSink sink = sinkForTesting(producer, resolver, Duration.ofSeconds(5), defaultRetryStrategyForTesting());

        sink.publish(orderPlaced("id-1"));

        verify(producer, times(2)).send(any());
    }

    @Test
    void close_stops_an_in_flight_retry_loop_from_attempting_again() throws Exception {
        Producer<String, byte[]> producer = mock(Producer.class);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch secondAttemptStarted = new CountDownLatch(2);

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> alwaysFailingFuture = mock(Future.class);
        when(alwaysFailingFuture.get(anyLong(), any()))
                .thenThrow(new ExecutionException(new NotEnoughReplicasException("expected, never resolves")));

        when(producer.send(any())).thenAnswer(invocation -> {
            attempts.incrementAndGet();
            secondAttemptStarted.countDown();
            return alwaysFailingFuture;
        });

        KafkaCloudEventSink sink = sinkForTesting(producer, resolver, Duration.ofSeconds(5), defaultRetryStrategyForTesting());

        Thread publishing = new Thread(() -> {
            try {
                sink.publish(orderPlaced("id-1"));
            } catch (RuntimeException ignored) {
                // Expected once close() stops the retry loop. Assertions below are on the thread's timing, not on
                // catching this here.
            }
        });
        publishing.start();

        assertThat(secondAttemptStarted.await(5, TimeUnit.SECONDS))
                .as("the retry loop should have started at least a second attempt before close() is called")
                .isTrue();

        sink.close();
        publishing.join(Duration.ofSeconds(5).toMillis());

        assertThat(publishing.isAlive())
                .as("close() should have stopped the retry loop from attempting again, so the publishing thread " +
                        "should have finished promptly instead of still retrying")
                .isFalse();
        int attemptsAtCloseTime = attempts.get();
        Thread.sleep(500);
        assertThat(attempts.get())
                .as("no further attempts should happen after close(), only the one already in flight when it was called")
                .isLessThanOrEqualTo(attemptsAtCloseTime + 1);
    }

    @Test
    void a_producer_that_never_gets_used_is_still_closed_by_close() {
        Producer<String, byte[]> producer = mock(Producer.class);
        KafkaCloudEventSink sink = sinkForTesting(producer, resolver, Duration.ofSeconds(5), defaultRetryStrategyForTesting());

        sink.close();

        verify(producer).close(any());
    }

    private static CloudEvent orderPlaced(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .build();
    }

    private static final class OrderPlaced {
    }
}
