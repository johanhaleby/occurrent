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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A confirm wait that ends without a definite answer, by timeout or by interruption, abandons its publish on the
 * channel rather than resolving it, so these two recovery paths are exercised directly against a mocked
 * {@link Channel} and {@link Connection} instead of a real broker, which has no way to force either outcome on
 * demand. The same setup also forces a persistent-but-retriable nack on demand, which a real broker cannot either,
 * to prove {@link RabbitMqCloudEventSink#close()} stops a retry loop that would otherwise keep going forever.
 */
class RabbitMqCloudEventSinkChannelRetirementTest {

    private static final String EXCHANGE = "test-exchange";

    private final RabbitMqTopicExchangeDestinationResolver resolver =
            new RabbitMqTopicExchangeDestinationResolver(EXCHANGE, ReflectionCloudEventTypeMapper.qualified());

    @Test
    void a_confirm_wait_timeout_retires_the_channel_and_publishes_on_a_confirm_mode_replacement() throws Exception {
        Connection connection = mock(Connection.class);
        Channel timedOutChannel = mock(Channel.class);
        Channel replacementChannel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(timedOutChannel), Optional.of(replacementChannel));
        when(timedOutChannel.waitForConfirms(anyLong())).thenThrow(new TimeoutException("no confirm in time"));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        assertThatThrownBy(() -> sink.publish(orderPlaced())).isInstanceOf(RabbitMqPublishTimeoutException.class);

        verify(replacementChannel).confirmSelect();
        verify(replacementChannel).addReturnListener(any(ReturnCallback.class));
        verify(replacementChannel, never()).close();
        // The old channel's close runs on its own thread rather than blocking this call, so it is awaited here
        // rather than asserted immediately.
        verify(timedOutChannel, timeout(2000)).close();
    }

    /**
     * A channel-level shutdown, {@code isHardError() == false}, the connection itself and every other channel on
     * it stay usable, only this one channel closed (a protocol violation on it, most often). That is what
     * {@code connection.openChannel()} succeeding right after models. A hard, connection-level shutdown is a
     * different case this retirement cannot recover from at all, since the connection the replacement channel
     * would open on is itself already gone. See {@link RabbitMqConfirmPublisher}'s own javadoc for that boundary.
     */
    @Test
    void a_shutdown_signal_during_the_confirm_wait_retires_the_channel_and_publishes_on_a_confirm_mode_replacement() throws Exception {
        Connection connection = mock(Connection.class);
        Channel shutdownChannel = mock(Channel.class);
        Channel replacementChannel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(shutdownChannel), Optional.of(replacementChannel));
        when(shutdownChannel.waitForConfirms(anyLong())).thenThrow(new ShutdownSignalException(false, false, null, null));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        assertThatThrownBy(() -> sink.publish(orderPlaced()))
                .isInstanceOf(RabbitMqPublishException.class)
                .hasCauseInstanceOf(ShutdownSignalException.class);

        verify(replacementChannel).confirmSelect();
        verify(replacementChannel).addReturnListener(any(ReturnCallback.class));
        // The old channel's close runs on its own thread rather than blocking this call, so it is awaited here
        // rather than asserted immediately, mirroring the timeout and interrupted cases above.
        verify(shutdownChannel, timeout(2000)).close();

        // With connection auto-recovery off, this is the only way the sink ever recovers from a shut-down channel:
        // a later publish must land on the replacement rather than failing forever against the retired one.
        when(replacementChannel.waitForConfirms(anyLong())).thenReturn(true);
        sink.publish(orderPlaced());
        verify(replacementChannel).basicPublish(any(), any(), eq(true), any(), any());
    }

    /**
     * The other half of the boundary the test above documents. {@code isHardError() == true} means the connection
     * itself is gone, so {@code connection.openChannel()} for the replacement fails too, exactly as it would
     * against a real closed connection. The sink reports that failure rather than pretending to have recovered,
     * with the retirement failure attached as suppressed on the original shutdown.
     */
    @Test
    void a_hard_connection_level_shutdown_signal_fails_the_publish_since_no_replacement_channel_can_open() throws Exception {
        Connection connection = mock(Connection.class);
        Channel shutdownChannel = mock(Channel.class);
        when(connection.openChannel())
                .thenReturn(Optional.of(shutdownChannel))
                .thenThrow(new ShutdownSignalException(true, false, null, null));
        when(shutdownChannel.waitForConfirms(anyLong())).thenThrow(new ShutdownSignalException(true, false, null, null));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        assertThatThrownBy(() -> sink.publish(orderPlaced()))
                .isInstanceOf(RabbitMqPublishException.class)
                .hasCauseInstanceOf(ShutdownSignalException.class)
                .satisfies(exception -> assertThat(exception.getSuppressed())
                        .as("the failed channel replacement should be attached rather than silently swallowed")
                        .hasSize(1)
                        .allMatch(RabbitMqPublishException.class::isInstance));
    }

    @Test
    void an_interrupted_confirm_wait_retires_the_channel_and_restores_the_interrupt_status() throws Exception {
        Connection connection = mock(Connection.class);
        Channel interruptedChannel = mock(Channel.class);
        Channel replacementChannel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(interruptedChannel), Optional.of(replacementChannel));
        when(interruptedChannel.waitForConfirms(anyLong())).thenThrow(new InterruptedException("interrupted"));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        try {
            assertThatThrownBy(() -> sink.publish(orderPlaced()))
                    .isInstanceOf(RabbitMqPublishException.class)
                    .isNotInstanceOf(RabbitMqPublishTimeoutException.class);
            assertThat(Thread.interrupted()).as("interrupt status should be restored on the calling thread").isTrue();
        } finally {
            Thread.interrupted();
        }

        verify(replacementChannel).confirmSelect();
        verify(interruptedChannel, timeout(2000)).close();
    }

    @Test
    void a_replacement_channel_failure_is_suppressed_on_the_timeout_it_actually_happened_on() throws Exception {
        Connection connection = mock(Connection.class);
        Channel timedOutChannel = mock(Channel.class);
        when(connection.openChannel())
                .thenReturn(Optional.of(timedOutChannel))
                .thenThrow(new ShutdownSignalException(true, true, null, null));
        when(timedOutChannel.waitForConfirms(anyLong())).thenThrow(new TimeoutException("no confirm in time"));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        assertThatThrownBy(() -> sink.publish(orderPlaced()))
                .isInstanceOf(RabbitMqPublishTimeoutException.class)
                .satisfies(exception -> assertThat(exception.getSuppressed())
                        .as("the failed replacement should not replace the timeout the caller actually asked about")
                        .hasSize(1)
                        .allMatch(RabbitMqPublishException.class::isInstance));
    }

    @Test
    void close_stops_a_publish_that_is_still_retrying_a_persistent_but_retriable_nack() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        // A nack on every attempt, with no cause the default retry predicate excludes, is retried forever by
        // design once the attempt cap is gone, so only close() can end it.
        when(channel.waitForConfirms(anyLong())).thenReturn(false);

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch publishReturned = new CountDownLatch(1);
        Thread publishing = new Thread(() -> {
            try {
                sink.publish(orderPlaced());
            } catch (Throwable e) {
                caught.set(e);
            } finally {
                publishReturned.countDown();
            }
        });
        publishing.start();

        // Let a real retry happen before asking the sink to stop, so this proves an in-flight retry is aborted
        // rather than a first attempt that never got the chance to retry at all.
        verify(channel, timeout(2000).atLeast(2)).waitForConfirms(anyLong());

        sink.close();

        assertThat(publishReturned.await(5, TimeUnit.SECONDS))
                .as("publish should stop once close() is called instead of retrying the nack forever")
                .isTrue();
        assertThat(caught.get()).isInstanceOf(RabbitMqPublishException.class);

        publishing.join(5000);
    }

    private static CloudEvent orderPlaced() {
        return CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .build();
    }

    private static final class OrderPlaced {
    }
}
