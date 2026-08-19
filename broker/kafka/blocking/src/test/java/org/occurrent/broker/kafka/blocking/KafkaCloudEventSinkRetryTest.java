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

import java.net.URI;
import java.time.Duration;
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
 * {@link KafkaCloudEventSink#publish(CloudEvent)}'s retry loop and {@link KafkaCloudEventSink#close()}'s
 * cancellation of it, exercised against a mocked {@link Producer} rather than a real broker, since forcing a
 * {@link NotEnoughReplicasException} that resolves itself needs a multi-broker cluster a single Testcontainers
 * node cannot model, and forcing one that never resolves needs a producer whose failure never stops, which a real
 * broker cannot promise either. {@link KafkaCloudEventSink#forTesting} is the package-private constructor this
 * file uses to inject the mock, the same reason {@code RabbitMqCloudEventSinkChannelRetirementTest} mocks a
 * {@code Connection} and a {@code Channel} instead of running its own broker.
 */
class KafkaCloudEventSinkRetryTest {

    private final KafkaDestination destination = KafkaDestination.of("test-topic");
    private final KafkaCloudEventSinkTest.FixedDestinationResolver resolver = new KafkaCloudEventSinkTest.FixedDestinationResolver(destination);

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

        KafkaCloudEventSink sink = KafkaCloudEventSink.forTesting(producer, resolver, Duration.ofSeconds(5), KafkaCloudEventSink.Builder.defaultRetryStrategy());

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

        KafkaCloudEventSink sink = KafkaCloudEventSink.forTesting(producer, resolver, Duration.ofSeconds(5), KafkaCloudEventSink.Builder.defaultRetryStrategy());

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
    void forTesting_producer_that_never_gets_used_is_still_closed_by_close() {
        Producer<String, byte[]> producer = mock(Producer.class);
        KafkaCloudEventSink sink = KafkaCloudEventSink.forTesting(producer, resolver, Duration.ofSeconds(5), KafkaCloudEventSink.Builder.defaultRetryStrategy());

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
