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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.subscription.RoutingOutcome;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Closing the consume channel is what puts a delivery this bridge is holding back on the queue. Releasing it first
 * only makes that happen sooner. So every way this bridge stops or closes has to reach the channel close, whatever
 * the steps in front of it throw, and these throw from each of those steps against a mocked {@link Channel}.
 * <p>
 * Each test catches what the bridge throws before asserting, since the {@code Error} still propagates once the
 * teardown is done. Without that the test would fail on the {@code Error} reaching it rather than on the close.
 */
class RabbitMqDomainEventBridgeTeardownTest {

    private static final long HELD_DELIVERY_TAG = 42L;

    /**
     * {@code DEFERRED} holds the tag in the deque a pacing release drains, {@code NOT_DELIVERABLE} under
     * {@code REDELIVER} in the one a failure release drains, so this covers both releases.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "NOT_DELIVERABLE"})
    void an_error_releasing_a_held_delivery_on_a_permanent_stop_still_closes_the_channel(RoutingOutcome held) throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new StackOverflowError("release")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        RabbitMqDomainEventBridge<Object> bridge = bridgeOver(channel, DeliveryFailurePolicy.REDELIVER);
        bridge.route(held, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(() -> bridge.route(RoutingOutcome.REFUSED, HELD_DELIVERY_TAG + 1, new BasicProperties(), new byte[0]));

        verify(channel).close();
        assertThat(thrown).isInstanceOf(StackOverflowError.class);
    }

    /**
     * The control for the test above. A {@code RuntimeException} out of the same release was always caught, so this
     * passes with or without the {@code finally} and shows the harness reaches the close.
     */
    @Test
    void a_runtime_exception_releasing_a_held_delivery_on_a_permanent_stop_closes_the_channel() throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new IOException("release")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        RabbitMqDomainEventBridge<Object> bridge = bridgeOver(channel, DeliveryFailurePolicy.REDELIVER);
        bridge.route(RoutingOutcome.DEFERRED, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(() -> bridge.route(RoutingOutcome.REFUSED, HELD_DELIVERY_TAG + 1, new BasicProperties(), new byte[0]));

        verify(channel).close();
        assertThat(thrown).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "NOT_DELIVERABLE"})
    void an_error_releasing_a_held_delivery_on_close_still_closes_the_channel(RoutingOutcome held) throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new StackOverflowError("release")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        RabbitMqDomainEventBridge<Object> bridge = bridgeOver(channel, DeliveryFailurePolicy.REDELIVER);
        bridge.route(held, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(bridge::close);

        verify(channel).close();
        assertThat(thrown).isInstanceOf(StackOverflowError.class);
    }

    /**
     * The failure action is a mock, since closing it is what closes the parking publisher under {@code PARK}.
     */
    @Test
    void an_error_closing_the_consume_channel_still_closes_the_parking_publisher() throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new StackOverflowError("close")).when(channel).close();
        RabbitMqDeliveryFailureAction failureAction = mock(RabbitMqDeliveryFailureAction.class);
        RabbitMqDomainEventBridge<Object> bridge = bridgeOver(channel, failureAction);

        Throwable thrown = catchThrowable(bridge::close);

        verify(failureAction).close();
        assertThat(thrown).isInstanceOf(StackOverflowError.class);
    }

    /**
     * Built through the builder rather than the constructor, since only the lifecycle poll registers a consumer and
     * there is nothing to cancel without one.
     */
    @SuppressWarnings("unchecked")
    @Test
    void an_error_cancelling_the_consumer_on_close_still_closes_the_channel() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn("consumer-tag-1");
        doThrow(new StackOverflowError("cancel")).when(channel).basicCancel("consumer-tag-1");
        DomainEventFeed<Object> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        when(feed.isReadyForLiveDelivery()).thenReturn(true);
        RabbitMqDomainEventBridge<Object> bridge = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(20))
                .build();
        verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

        Throwable thrown = catchThrowable(bridge::close);

        verify(channel).close();
        assertThat(thrown).isInstanceOf(StackOverflowError.class);
    }

    private static RabbitMqDomainEventBridge<Object> bridgeOver(Channel channel, DeliveryFailurePolicy policy) {
        return bridgeOver(channel, new RabbitMqDeliveryFailureAction(channel, policy, null, null,
                LoggerFactory.getLogger(RabbitMqDomainEventBridgeTeardownTest.class)));
    }

    private static RabbitMqDomainEventBridge<Object> bridgeOver(Channel channel, RabbitMqDeliveryFailureAction failureAction) {
        return new RabbitMqDomainEventBridge<>(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, Duration.ofSeconds(1));
    }
}
