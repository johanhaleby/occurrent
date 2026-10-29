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
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A nack or an ack that fails with an {@code IOException} on a recovering connection. A failed socket write throws one,
 * and so does a {@code TrafficListener} or {@code MetricsCollector} written in Kotlin, with the connection still up.
 * The source keeps running when the connection dropped after the call, because the dropped connection put the delivery
 * back on the queue. Otherwise it stops at its first poll a poll interval or more after the oldest failure, and closes
 * the channel, which puts the delivery back. It never sends a second nack.
 */
class RabbitMqCloudEventSourceIoFailureTest {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final long DELIVERY_TAG = 1L;

    private final Channel channel = mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
    private final RabbitMqCloudEventSource source = sourceOn(channel);

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

    /**
     * With a short {@code networkRecoveryInterval} a whole recovery can finish while the release still holds the lock,
     * and the recovered channel is open by the next poll.
     */
    @Test
    void a_release_that_fails_with_an_io_exception_keeps_the_source_when_the_connection_dropped_and_recovered_during_the_call() throws Exception {
        AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
        consumerRecovery().attachTo(connection, channel);
        RecoveryListener recoveryListener = recoveryListenerOf(connection);
        when(channel.isOpen()).thenReturn(true);
        doAnswer(invocation -> {
            channelShutDown(droppedConnection());
            recoveryListener.handleRecoveryStarted(connection);
            recoveryListener.handleRecovery(connection);
            throw new IOException("Connection reset");
        }).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        source.route(RoutingOutcome.DEFERRED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        poll();
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
        RabbitMqCloudEventSource plainSource = sourceOn(plainChannel);
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

    @Test
    void an_ack_that_fails_with_an_io_exception_keeps_the_source_when_the_connection_dropped_and_recovered_during_the_call() throws Exception {
        AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
        consumerRecovery().attachTo(connection, channel);
        RecoveryListener recoveryListener = recoveryListenerOf(connection);
        when(channel.isOpen()).thenReturn(true);
        doAnswer(invocation -> {
            channelShutDown(droppedConnection());
            recoveryListener.handleRecoveryStarted(connection);
            recoveryListener.handleRecovery(connection);
            throw new IOException("Connection reset");
        }).when(channel).basicAck(anyLong(), anyBoolean());

        handleDeliveryOrStop();
        waitAPollInterval();
        poll();

        verify(channel, never()).close();
    }

    /**
     * Topology recovery registers the consumer again before the client reports the end of the recovery, so a delivery
     * can arrive on the recovered channel while the recovery still runs. The end of that recovery is no drop.
     */
    @Test
    void an_ack_that_fails_with_an_io_exception_on_the_recovered_channel_stops_the_source_although_the_recovery_ends_afterwards() throws Exception {
        AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
        consumerRecovery().attachTo(connection, channel);
        RecoveryListener recoveryListener = recoveryListenerOf(connection);
        when(channel.isOpen()).thenReturn(true);
        channelShutDown(droppedConnection());
        recoveryListener.handleRecoveryStarted(connection);
        doThrow(new IOException("Frame enqueuing failed")).when(channel).basicAck(anyLong(), anyBoolean());

        handleDeliveryOrStop();
        recoveryListener.handleRecovery(connection);
        waitAPollInterval();
        poll();

        verify(channel).close();
    }

    /**
     * At a {@code prefetchCount} above one, a {@code MetricsCollector} that throws after every ack would otherwise put
     * the judgement off for as long as deliveries keep coming, while an earlier delivery whose ack never went out
     * waits unanswered.
     */
    @Test
    void a_later_ack_failure_does_not_put_off_stopping_the_source_for_an_earlier_one() throws Exception {
        doThrow(new IOException("thrown by a MetricsCollector written in Kotlin")).when(channel).basicAck(anyLong(), anyBoolean());
        when(channel.isOpen()).thenReturn(true);

        handleDeliveryOrStop();
        Thread.sleep(POLL_INTERVAL.toMillis() * 3 / 5);
        handleDeliveryOrStop();
        Thread.sleep(POLL_INTERVAL.toMillis() * 3 / 5);
        poll();

        verify(channel).close();
    }

    /**
     * The drop requeued the earlier delivery, but the later one arrived on the recovered channel and may still wait
     * for an answer.
     */
    @Test
    void a_drop_between_two_ack_failures_does_not_explain_the_later_one() throws Exception {
        doThrow(new IOException("thrown by a MetricsCollector written in Kotlin")).when(channel).basicAck(anyLong(), anyBoolean());
        when(channel.isOpen()).thenReturn(true);

        handleDeliveryOrStop();
        channelShutDown(droppedConnection());
        handleDeliveryOrStop();
        waitAPollInterval();
        poll();

        verify(channel).close();
    }

    /**
     * Nothing is held and no handler runs, so no release or acknowledgement touches the closed channel. Without the
     * stop the source would consume nothing and say nothing.
     */
    @Test
    void a_channel_the_broker_closes_stops_an_idle_source_on_the_next_poll() throws Exception {
        channelShutDown(new ShutdownSignalException(false, false, null, channel));

        poll();

        verify(channel).close();
    }

    @Test
    void a_connection_drop_does_not_stop_an_idle_source() throws Exception {
        connectionDropped();

        poll();

        verify(channel, never()).close();
    }

    private void connectionDropped() {
        when(channel.isOpen()).thenReturn(false);
        when(channel.getCloseReason()).thenReturn(droppedConnection());
        channelShutDown(droppedConnection());
    }

    // Calls every shutdown listener the source added to the channel, the way the client does when the channel closes
    private void channelShutDown(ShutdownSignalException cause) {
        mockingDetails(channel).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("addShutdownListener"))
                .map(invocation -> (ShutdownListener) invocation.getArgument(0))
                .toList()
                .forEach(listener -> listener.shutdownCompleted(cause));
    }

    private static ShutdownSignalException droppedConnection() {
        return new ShutdownSignalException(true, false, null, mock(Connection.class));
    }

    private static RabbitMqCloudEventSource sourceOn(Channel channel) {
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> { });
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceIoFailureTest.class));
        return new RabbitMqCloudEventSource(model, channel, "queue", 1, POLL_INTERVAL, failureAction, subscriptionId -> true, Duration.ofSeconds(1));
    }

    private static void waitAPollInterval() throws InterruptedException {
        Thread.sleep(POLL_INTERVAL.toMillis() + 100);
    }

    private void poll() throws ReflectiveOperationException {
        poll(source);
    }

    private static void poll(RabbitMqCloudEventSource source) throws ReflectiveOperationException {
        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }

    private void handleDeliveryOrStop() throws ReflectiveOperationException {
        Method method = RabbitMqCloudEventSource.class.getDeclaredMethod("handleDeliveryOrStop", Delivery.class, long.class);
        method.setAccessible(true);
        method.invoke(source, delivery(), DELIVERY_TAG);
    }

    private RabbitMqConsumerRecovery consumerRecovery() throws ReflectiveOperationException {
        Field field = RabbitMqCloudEventSource.class.getDeclaredField("consumerRecovery");
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
