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
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqConsumerRecovery;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.subscription.RoutingOutcome;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A nack or an ack that fails with an {@code IOException} on a recovering connection. A failed socket write throws one,
 * and so does a {@code TrafficListener} or {@code MetricsCollector} written in Kotlin, with the connection still up.
 * The source keeps running when the channel has closed for a connection recovery by the next poll a full poll interval
 * later, or a recovery has started or ended since, because the dropped connection put the delivery back on the queue.
 * Otherwise it stops and closes the channel, which puts the delivery back. It never sends a second nack.
 */
class RabbitMqDomainEventSourceIoFailureTest {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final long DELIVERY_TAG = 1L;

    private final Channel channel = mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
    private final RabbitMqDomainEventSource<Object> source = sourceOn(channel);

    @Test
    void a_release_that_fails_with_an_io_exception_keeps_the_source_when_the_channel_closed_for_a_connection_recovery_by_the_next_poll() throws Exception {
        doThrow(new IOException("Connection reset")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        source.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll();
        connectionDropped();
        waitAPollInterval();
        poll();

        verify(channel, never()).close();
        verify(channel, times(1)).basicNack(DELIVERY_TAG, false, true);
    }

    @Test
    void a_release_that_fails_with_an_io_exception_keeps_the_source_when_a_recovery_started_by_the_next_poll() throws Exception {
        doThrow(new IOException("Connection reset")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        when(channel.isOpen()).thenReturn(true);
        AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
        consumerRecovery().attachTo(connection, channel);
        source.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll();
        recoveryListenerOf(connection).handleRecoveryStarted(connection);
        waitAPollInterval();
        poll();

        verify(channel, never()).close();
        verify(channel, times(1)).basicNack(DELIVERY_TAG, false, true);
    }

    @Test
    void a_release_that_fails_with_an_io_exception_stops_the_source_on_the_next_poll_when_the_channel_is_still_open() throws Exception {
        doThrow(new IOException("thrown by a MetricsCollector written in Kotlin")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        when(channel.isOpen()).thenReturn(true);
        source.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll();
        verify(channel, never()).close();
        waitAPollInterval();
        poll();

        verify(channel).close();
        verify(channel, times(1)).basicNack(DELIVERY_TAG, false, true);
    }

    /**
     * The broker closes a channel on its own after a channel error, such as {@code 406 PRECONDITION_FAILED}, and nothing
     * reopens it. Closing it put the delivery back on the queue, and a source that kept retrying would log on every poll.
     */
    @Test
    void a_release_on_a_channel_the_broker_closed_stops_the_source() throws Exception {
        doThrow(new AlreadyClosedException(new ShutdownSignalException(false, false, null, channel)))
                .when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        source.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll();

        verify(channel).close();
    }

    @Test
    void a_release_on_a_closed_channel_of_a_connection_without_automatic_recovery_stops_the_source() throws Exception {
        Channel plainChannel = mock(Channel.class);
        doThrow(new AlreadyClosedException(new ShutdownSignalException(true, false, null, mock(Connection.class))))
                .when(plainChannel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        RabbitMqDomainEventSource<Object> plainSource = sourceOn(plainChannel);
        plainSource.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll(plainSource);

        verify(plainChannel).close();
    }

    @Test
    void a_release_on_a_channel_closed_by_a_dropped_connection_is_made_again_on_the_next_poll() throws Exception {
        doThrow(new AlreadyClosedException(droppedConnection())).doNothing()
                .when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        source.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll();
        poll();

        verify(channel, never()).close();
        verify(channel, times(2)).basicNack(DELIVERY_TAG, false, true);
    }

    /**
     * At the default {@code prefetchCount} of one the broker sends nothing further to a consumer with an unanswered
     * delivery, so an ack that never went out on a live connection would leave the source stuck.
     */
    @Test
    void an_ack_that_fails_with_an_io_exception_stops_the_source_a_poll_interval_later_when_the_channel_is_still_open() throws Exception {
        doThrow(new IOException("Frame enqueuing failed")).when(channel).basicAck(anyLong(), anyBoolean());
        when(channel.isOpen()).thenReturn(true);

        handleDeliveryOrStop();
        poll();
        verify(channel, never()).close();
        waitAPollInterval();
        poll();

        verify(channel).close();
    }

    @Test
    void an_ack_that_fails_with_an_io_exception_keeps_the_source_when_the_channel_closed_for_a_connection_recovery() throws Exception {
        doThrow(new IOException("Connection reset")).when(channel).basicAck(anyLong(), anyBoolean());

        handleDeliveryOrStop();
        connectionDropped();
        waitAPollInterval();
        poll();

        verify(channel, never()).close();
    }

    @Test
    void the_source_keeps_running_after_an_ack_failure_the_connection_explained() throws Exception {
        doThrow(new IOException("Connection reset")).doNothing().when(channel).basicAck(anyLong(), anyBoolean());

        handleDeliveryOrStop();
        connectionDropped();
        waitAPollInterval();
        poll();
        when(channel.isOpen()).thenReturn(true);
        waitAPollInterval();
        poll();

        verify(channel, never()).close();
    }

    private void connectionDropped() {
        when(channel.isOpen()).thenReturn(false);
        when(channel.getCloseReason()).thenReturn(droppedConnection());
    }

    private static ShutdownSignalException droppedConnection() {
        return new ShutdownSignalException(true, false, null, mock(Connection.class));
    }

    @SuppressWarnings("unchecked")
    private static RabbitMqDomainEventSource<Object> sourceOn(Channel channel) {
        DomainEventFeed<Object> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        when(feed.isReadyForLiveDelivery()).thenReturn(true);
        when(feed.acceptOrDeferCloudEvent(any(CloudEvent.class))).thenReturn(RoutingOutcome.DELIVERED);
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqDomainEventSourceIoFailureTest.class));
        return new RabbitMqDomainEventSource<>(feed, channel, "queue", 1, POLL_INTERVAL, failureAction, Duration.ofSeconds(1));
    }

    private static void waitAPollInterval() throws InterruptedException {
        Thread.sleep(POLL_INTERVAL.toMillis() + 100);
    }

    private void poll() throws ReflectiveOperationException {
        poll(source);
    }

    private static void poll(RabbitMqDomainEventSource<Object> source) throws ReflectiveOperationException {
        Method reconcileConsumption = RabbitMqDomainEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }

    private void handleDeliveryOrStop() throws ReflectiveOperationException {
        Method method = RabbitMqDomainEventSource.class.getDeclaredMethod("handleDeliveryOrStop", Delivery.class, long.class);
        method.setAccessible(true);
        method.invoke(source, delivery(), DELIVERY_TAG);
    }

    private RabbitMqConsumerRecovery consumerRecovery() throws ReflectiveOperationException {
        Field field = RabbitMqDomainEventSource.class.getDeclaredField("consumerRecovery");
        field.setAccessible(true);
        return (RabbitMqConsumerRecovery) field.get(source);
    }

    private static RecoveryListener recoveryListenerOf(AutorecoveringConnection connection) {
        ArgumentCaptor<RecoveryListener> captor = ArgumentCaptor.forClass(RecoveryListener.class);
        verify(connection).addRecoveryListener(captor.capture());
        return captor.getValue();
    }

    private static Delivery delivery() {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .build();
        return new Delivery(new Envelope(DELIVERY_TAG, false, "exchange", "routingKey"),
                RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of()), RabbitMqCloudEventMapper.toBody(cloudEvent));
    }
}
