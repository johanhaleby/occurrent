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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link RabbitMqConfirmPublisher} is the one confirmed-publish implementation both {@link RabbitMqCloudEventSink}
 * and the consume-side parking path in {@link RabbitMqDeliveryFailureAction} share. Its confirm-wait timeout,
 * interruption and channel-retirement behaviour are already exercised through the sink's own public API in
 * {@code RabbitMqCloudEventSinkChannelRetirementTest}, unchanged by the extraction since this class is exactly what
 * that test already ran against. This class covers the two things that test does not: a failure opening the
 * channel after it was already created, and a plain, direct publish of already-built properties and a body, the
 * shape the parking path calls with.
 */
class RabbitMqConfirmPublisherTest {

    /**
     * {@code connection.openChannel()} can succeed and {@code channel.confirmSelect()} can still fail. Before this,
     * the channel that call already opened was never closed, since only the case where {@code openChannel()} itself
     * fails was handled.
     */
    @Test
    void a_confirmSelect_failure_closes_the_channel_that_was_already_opened() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        doThrow(new IOException("confirmSelect failed")).when(channel).confirmSelect();

        assertThatThrownBy(() -> new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5)))
                .isInstanceOf(RabbitMqPublishException.class);

        verify(channel).close();
    }

    @Test
    void publish_sends_the_given_properties_and_body_unchanged_to_the_given_exchange_and_routingKey() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.waitForConfirms(anyLong())).thenReturn(true);
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5));
        byte[] body = "raw body".getBytes(StandardCharsets.UTF_8);
        BasicProperties properties = new BasicProperties.Builder().contentType("text/plain").build();

        publisher.publish("exchange", "routingKey", properties, body);

        verify(channel).basicPublish(eq("exchange"), eq("routingKey"), eq(true), any(BasicProperties.class), eq(body));
    }

    /**
     * Reproduces, deterministically enough for a regression test, a {@code close()} racing a timed-out publish's
     * own channel retirement. {@code retireChannel()} reassigns the channel field in two steps, retire the old one
     * and open its replacement, and a {@code close()} landing between those two steps used to read the field
     * before the replacement was assigned, closing the channel about to be retired anyway and never seeing the
     * replacement at all, which leaked it. Forces the interleaving by blocking the retirement's own
     * {@code openChannel()} call on a latch, starting {@code close()} on its own thread while that is still
     * blocked, and confirming it has not returned yet before releasing the latch.
     */
    @Test
    void close_racing_a_timed_out_publishs_channel_retirement_closes_the_replacement_not_the_retiring_channel() throws Exception {
        Connection connection = mock(Connection.class);
        Channel retiringChannel = mock(Channel.class);
        Channel replacementChannel = mock(Channel.class);
        AtomicInteger openChannelCalls = new AtomicInteger();
        CountDownLatch retirementBlocked = new CountDownLatch(1);
        CountDownLatch releaseRetirement = new CountDownLatch(1);
        when(connection.openChannel()).thenAnswer(invocation -> {
            if (openChannelCalls.incrementAndGet() == 2) {
                retirementBlocked.countDown();
                releaseRetirement.await(5, TimeUnit.SECONDS);
                return Optional.of(replacementChannel);
            }
            return Optional.of(retiringChannel);
        });
        when(retiringChannel.waitForConfirms(anyLong())).thenThrow(new TimeoutException("confirm timed out"));
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofMillis(50));
        BasicProperties properties = new BasicProperties.Builder().contentType("text/plain").build();

        Thread timedOutPublish = new Thread(() -> {
            try {
                publisher.publish("exchange", "routingKey", properties, "body".getBytes(StandardCharsets.UTF_8));
            } catch (RabbitMqPublishTimeoutException ignored) {
                // Expected. This is the failure retireChannel() runs to recover from.
            }
        });
        timedOutPublish.start();
        assertThat(retirementBlocked.await(2, TimeUnit.SECONDS)).as("the retirement's openChannel() call should be blocked").isTrue();

        Thread closeCall = new Thread(() -> {
            try {
                publisher.close();
            } catch (IOException | TimeoutException ignored) {
                // Nothing to assert on here; the outcome this test checks is which channel actually got closed.
            }
        });
        closeCall.start();
        // close() shares publishLock with the still-in-flight publish/retirement, so it must still be blocked too.
        closeCall.join(500);
        assertThat(closeCall.isAlive()).as("close() must wait for the in-flight retirement rather than racing it").isTrue();
        verify(replacementChannel, never()).close();

        releaseRetirement.countDown();
        timedOutPublish.join(TimeUnit.SECONDS.toMillis(5));
        closeCall.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(timedOutPublish.isAlive()).isFalse();
        assertThat(closeCall.isAlive()).isFalse();
        // The replacement retireChannel() assigned is the one close() must close, not the one it retired.
        verify(replacementChannel, timeout(2000)).close();
    }
}
