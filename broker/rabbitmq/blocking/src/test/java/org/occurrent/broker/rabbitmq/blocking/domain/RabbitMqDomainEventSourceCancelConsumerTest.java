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

package org.occurrent.broker.rabbitmq.blocking.domain;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ChannelContinuationTimeoutException;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.ConsumerRecoveryListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqConsumerRecovery;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.subscription.RoutingOutcome;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What the lifecycle poll believes about its consumer after a cancel that threw. A cancel that fails with the
 * connection recovered underneath it throws for a consumer the client no longer has, and a source that kept the tag
 * would think it was still consuming and never register another. A cancel whose reply timed out, or that failed in a
 * way that says nothing about the consumer, may have left it registered, so the next poll cancels again. A cancel
 * made while a recovery runs could miss the consumer the recovery registers again, so it waits for the recovery to
 * finish and cancels that consumer under its new tag. The poll is driven directly
 * against a mocked {@link Channel}, since nothing schedules it on a source built this way.
 */
class RabbitMqDomainEventSourceCancelConsumerTest {

    private static final String CONSUMER_TAG = "consumer-tag-1";
    private static final String RECOVERED_CONSUMER_TAG = "consumer-tag-2";

    private final Channel channel = mock(Channel.class);
    private final AtomicBoolean ready = new AtomicBoolean(true);
    private RabbitMqDomainEventSource<Object> source;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void startConsuming() throws Exception {
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn(CONSUMER_TAG);
        DomainEventFeed<Object> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        when(feed.isReadyForLiveDelivery()).thenAnswer(invocation -> ready.get());
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqDomainEventSourceCancelConsumerTest.class));
        source = new RabbitMqDomainEventSource<>(feed, channel, "queue", 1, Duration.ofSeconds(1), failureAction, Duration.ofSeconds(1));
        poll();
        verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @AfterEach
    void closeSource() {
        source.close();
    }

    @Test
    void a_successful_cancel_clears_the_consumer_so_the_next_poll_that_should_consume_registers_another() throws Exception {
        ready.set(false);
        poll();

        ready.set(true);
        poll();

        verify(channel).basicCancel(CONSUMER_TAG);
        verify(channel, times(2)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_cancel_that_fails_with_an_io_exception_clears_the_consumer_so_the_next_poll_that_should_consume_registers_another() throws Exception {
        doThrow(new IOException("Unknown consumerTag")).doNothing().when(channel).basicCancel(CONSUMER_TAG);
        ready.set(false);
        poll();

        ready.set(true);
        poll();

        verify(channel).basicCancel(CONSUMER_TAG);
        verify(channel, times(2)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_cancel_that_fails_with_a_runtime_exception_is_made_again_before_the_next_poll_that_should_consume_registers_another() throws Exception {
        doThrow(new IllegalStateException("thrown by a TrafficListener")).doNothing().when(channel).basicCancel(CONSUMER_TAG);
        ready.set(false);
        poll();

        ready.set(true);
        poll();

        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel, times(2)).basicCancel(CONSUMER_TAG);
        inOrder.verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_consumer_paused_while_a_recovery_runs_is_cancelled_under_the_tag_the_recovery_registered_it_with_before_another_is_registered() throws Exception {
        AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
        consumerRecovery().attachTo(connection);
        RecoveryListener recoveryListener = captured(RecoveryListener.class, captor -> verify(connection).addRecoveryListener(captor.capture()));
        ConsumerRecoveryListener consumerListener = captured(ConsumerRecoveryListener.class, captor -> verify(connection).addConsumerRecoveryListener(captor.capture()));
        // The recovered channel has no consumer under the old tag until topology recovery registers it again
        doThrow(new IOException("Unknown consumerTag")).when(channel).basicCancel(CONSUMER_TAG);

        recoveryListener.handleRecoveryStarted(connection);
        ready.set(false);
        poll();
        consumerListener.consumerRecovered(CONSUMER_TAG, RECOVERED_CONSUMER_TAG);
        recoveryListener.handleRecovery(connection);
        poll();
        ready.set(true);
        poll();

        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel).basicCancel(RECOVERED_CONSUMER_TAG);
        inOrder.verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_cancel_whose_reply_timed_out_keeps_the_consumer_so_the_next_poll_cancels_it_again_instead_of_registering_another() throws Exception {
        doThrow(new ChannelContinuationTimeoutException(new TimeoutException("no reply"), channel, 1, null))
                .doNothing().when(channel).basicCancel(CONSUMER_TAG);
        ready.set(false);
        poll();

        poll();

        verify(channel, times(2)).basicCancel(CONSUMER_TAG);
        verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    /**
     * The client decodes the tag of a {@code basic.cancel} from the broker's own frame, so it is a separate
     * {@code String} from the one {@code basicConsume} returned. The client also keeps its record of the consumer,
     * which a recovery with topology recovery on would register again, so the next poll cancels it before registering
     * another.
     */
    @Test
    void a_consumer_the_broker_cancelled_is_cancelled_by_the_next_poll_which_then_registers_another() throws Exception {
        ArgumentCaptor<CancelCallback> cancelCallback = ArgumentCaptor.forClass(CancelCallback.class);
        verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), cancelCallback.capture());
        // The channel no longer has a consumer the broker cancelled
        doThrow(new IOException("Unknown consumerTag")).when(channel).basicCancel(CONSUMER_TAG);

        cancelCallback.getValue().handle(new String(CONSUMER_TAG));
        poll();

        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
        inOrder.verify(channel).basicCancel(CONSUMER_TAG);
        inOrder.verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    /**
     * At the default {@code prefetchCount} of one the broker sends nothing further until a held delivery is answered,
     * so a cancel that fails on every poll must not keep the release from running.
     */
    @Test
    void a_cancel_that_keeps_failing_still_releases_a_held_delivery() throws Exception {
        doThrow(new IllegalStateException("thrown by a TrafficListener")).when(channel).basicCancel(CONSUMER_TAG);
        source.route(RoutingOutcome.DEFERRED, 1L, new BasicProperties(), new byte[0]);
        ready.set(false);

        poll();

        verify(channel).basicCancel(CONSUMER_TAG);
        verify(channel).basicNack(1L, false, true);
    }

    private void poll() throws ReflectiveOperationException {
        Method reconcileConsumption = RabbitMqDomainEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }

    private RabbitMqConsumerRecovery consumerRecovery() throws ReflectiveOperationException {
        Field consumerRecovery = RabbitMqDomainEventSource.class.getDeclaredField("consumerRecovery");
        consumerRecovery.setAccessible(true);
        return (RabbitMqConsumerRecovery) consumerRecovery.get(source);
    }

    private static <T> T captured(Class<T> type, Consumer<ArgumentCaptor<T>> verification) {
        ArgumentCaptor<T> captor = ArgumentCaptor.forClass(type);
        verification.accept(captor);
        return captor.getValue();
    }
}
