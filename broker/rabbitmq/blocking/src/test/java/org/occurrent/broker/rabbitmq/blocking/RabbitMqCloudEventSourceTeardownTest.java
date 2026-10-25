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
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Closing the consume channel is what puts a delivery this source is holding back on the queue. Releasing it first
 * only makes that happen sooner. So every way this source stops or closes has to reach the channel close, whatever
 * the steps in front of it throw, and these throw from each of those steps against a mocked {@link Channel}.
 * <p>
 * Each test catches what the source throws before asserting, since the {@code Error} still propagates once the
 * teardown is done. Without that the test would fail on the {@code Error} reaching it rather than on the close.
 */
class RabbitMqCloudEventSourceTeardownTest {

    private static final long HELD_DELIVERY_TAG = 42L;
    private static final long HANDLED_DELIVERY_TAG = 7L;

    /**
     * {@code DEFERRED} holds the tag in the deque a pacing release drains, {@code NOT_DELIVERABLE} under
     * {@code REDELIVER} in the one a failure release drains, so this covers both releases.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "NOT_DELIVERABLE"})
    void an_error_releasing_a_held_delivery_on_a_permanent_stop_still_closes_the_channel(RoutingOutcome held) throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new StackOverflowError("release")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        RabbitMqCloudEventSource source = sourceOver(channel, DeliveryFailurePolicy.REDELIVER);
        source.route(held, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(() -> source.route(RoutingOutcome.REFUSED, HELD_DELIVERY_TAG + 1, new BasicProperties(), new byte[0]));

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
        RabbitMqCloudEventSource source = sourceOver(channel, DeliveryFailurePolicy.REDELIVER);
        source.route(RoutingOutcome.DEFERRED, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(() -> source.route(RoutingOutcome.REFUSED, HELD_DELIVERY_TAG + 1, new BasicProperties(), new byte[0]));

        verify(channel).close();
        assertThat(thrown).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "NOT_DELIVERABLE"})
    void an_error_releasing_a_held_delivery_on_close_still_closes_the_channel(RoutingOutcome held) throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new StackOverflowError("release")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        RabbitMqCloudEventSource source = sourceOver(channel, DeliveryFailurePolicy.REDELIVER);
        source.route(held, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(source::close);

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
        RabbitMqCloudEventSource source = sourceOver(channel, failureAction);

        Throwable thrown = catchThrowable(source::close);

        verify(failureAction).close();
        assertThat(thrown).isInstanceOf(StackOverflowError.class);
    }

    /**
     * A close timeout of 300 years passes the builder's check but overflows a {@code long} of nanoseconds, which used
     * to make {@code close()} throw before anything else in it had run.
     */
    @Test
    void a_close_timeout_too_large_for_nanoseconds_still_closes_the_channel() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceTeardownTest.class));
        RabbitMqCloudEventSource source = new RabbitMqCloudEventSource(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null, Duration.ofDays(365L * 300));

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isNull();
        verify(channel, times(1)).close();
    }

    @ParameterizedTest
    @MethodSource("closeTimeouts")
    void close_with_any_close_timeout_closes_the_channel_and_the_failure_action_once_and_throws_nothing(Duration closeTimeout) throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqDeliveryFailureAction failureAction = mock(RabbitMqDeliveryFailureAction.class);
        RabbitMqCloudEventSource source = new RabbitMqCloudEventSource(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null, closeTimeout);

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isNull();
        verify(channel, times(1)).close();
        verify(failureAction, times(1)).close();
    }

    @ParameterizedTest
    @MethodSource("waitingCloseTimeouts")
    void close_with_any_close_timeout_that_waits_cancels_the_consumer_before_closing_the_channel(Duration closeTimeout) throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn("consumer-tag-1");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> {
        });
        RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection, model, "queue")
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(20))
                .closeTimeout(closeTimeout)
                .build();
        verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isNull();
        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel).basicCancel("consumer-tag-1");
        inOrder.verify(channel).close();
    }

    /**
     * The handler is still running when {@code close()} starts. A deadline that had wrapped into the past would
     * interrupt it at once and fence the acknowledgement it makes afterwards. A timeout this long has to read as a
     * wait with no practical end instead, so the handler is left alone and its delivery is acknowledged.
     */
    @ParameterizedTest
    @MethodSource("hugeCloseTimeouts")
    void close_with_a_huge_close_timeout_waits_for_a_running_handler_without_interrupting_it_and_lets_it_acknowledge(Duration closeTimeout) throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        AtomicReference<DeliverCallback> deliverCallback = new AtomicReference<>();
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenAnswer(invocation -> {
            deliverCallback.set(invocation.getArgument(2));
            return "consumer-tag-1";
        });
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        AtomicBoolean handlerInterrupted = new AtomicBoolean();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> {
            handlerEntered.countDown();
            try {
                releaseHandler.await();
            } catch (InterruptedException e) {
                handlerInterrupted.set(true);
                Thread.currentThread().interrupt();
            }
        });
        RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection, model, "queue")
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(20))
                .closeTimeout(closeTimeout)
                .build();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        try {
            verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
            deliverCallback.get().handle("consumer-tag-1", delivery(HANDLED_DELIVERY_TAG));
            assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> closeFailure.set(catchThrowable(source::close)));
            Thread.sleep(300);
            assertThat(closing).as("close() waits while the handler is still running").isNotDone();
            assertThat(handlerInterrupted).as("close() leaves the running handler alone").isFalse();
            verify(channel, never()).close();
            releaseHandler.countDown();

            closing.get(5, TimeUnit.SECONDS);
            assertThat(closeFailure.get()).isNull();
            assertThat(handlerInterrupted).isFalse();
            verify(channel).basicAck(HANDLED_DELIVERY_TAG, false);
            verify(channel).close();
        } finally {
            releaseHandler.countDown();
        }
    }

    /**
     * Built through the builder rather than the constructor, since only the lifecycle poll registers a consumer and
     * there is nothing to cancel without one.
     */
    @Test
    void an_error_cancelling_the_consumer_on_close_still_closes_the_channel() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn("consumer-tag-1");
        doThrow(new StackOverflowError("cancel")).when(channel).basicCancel("consumer-tag-1");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> {
        });
        RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection, model, "queue")
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(20))
                .build();
        verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

        Throwable thrown = catchThrowable(source::close);

        verify(channel).close();
        assertThat(thrown).isInstanceOf(StackOverflowError.class);
    }

    /**
     * The release is what fails first, then the channel close fails with something that is not a
     * {@link com.rabbitmq.client.ShutdownSignalException}, which close() does not swallow.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "NOT_DELIVERABLE"})
    void an_exception_closing_the_channel_does_not_replace_the_failure_already_leaving_close(RoutingOutcome held) throws Exception {
        Channel channel = mock(Channel.class);
        StackOverflowError primary = new StackOverflowError("release");
        IllegalStateException closeFailure = new IllegalStateException("close");
        doThrow(primary).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        doThrow(closeFailure).when(channel).close();
        RabbitMqDeliveryFailureAction failureAction = spy(redeliveringAction(channel));
        RabbitMqCloudEventSource source = sourceOver(channel, failureAction);
        source.route(held, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isSameAs(primary);
        assertThat(thrown.getSuppressed()).containsExactly(closeFailure);
        verify(channel).close();
        verify(failureAction).close();
    }

    @Test
    void with_no_failure_already_leaving_close_the_channel_close_failure_leaves_it_with_the_failure_action_failure_suppressed() throws Exception {
        Channel channel = mock(Channel.class);
        IllegalStateException channelFailure = new IllegalStateException("channel");
        IllegalArgumentException actionFailure = new IllegalArgumentException("action");
        doThrow(channelFailure).when(channel).close();
        RabbitMqDeliveryFailureAction failureAction = mock(RabbitMqDeliveryFailureAction.class);
        doThrow(actionFailure).when(failureAction).close();
        RabbitMqCloudEventSource source = sourceOver(channel, failureAction);

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isSameAs(channelFailure);
        assertThat(thrown.getSuppressed()).containsExactly(actionFailure);
        verify(channel).close();
        verify(failureAction).close();
    }

    @Test
    void the_channel_close_throwing_the_very_failure_already_leaving_close_does_not_suppress_it_on_itself() throws Exception {
        Channel channel = mock(Channel.class);
        StackOverflowError primary = new StackOverflowError("shared");
        doThrow(primary).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        doThrow(primary).when(channel).close();
        RabbitMqDeliveryFailureAction failureAction = spy(redeliveringAction(channel));
        RabbitMqCloudEventSource source = sourceOver(channel, failureAction);
        source.route(RoutingOutcome.DEFERRED, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isSameAs(primary);
        assertThat(thrown.getSuppressed()).isEmpty();
        verify(failureAction).close();
    }

    /**
     * The channel close failing with an {@code Error} of its own is what used to replace the one from the release.
     * Everything the stop does after the close still has to happen, and the lock has to be free for the next thread,
     * which a following {@code close()} on another thread shows by releasing the delivery the failed release put back.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "NOT_DELIVERABLE"})
    void an_error_closing_the_channel_on_a_permanent_stop_does_not_replace_the_error_from_the_release_and_the_stop_completes(RoutingOutcome held) throws Exception {
        Channel channel = mock(Channel.class);
        StackOverflowError primary = new StackOverflowError("release");
        LinkageError closeFailure = new LinkageError("close");
        doThrow(primary).doNothing().when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        doThrow(closeFailure).doNothing().when(channel).close();
        RabbitMqCloudEventSource source = sourceOver(channel, DeliveryFailurePolicy.REDELIVER);
        source.route(held, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(() -> source.route(RoutingOutcome.REFUSED, HELD_DELIVERY_TAG + 1, new BasicProperties(), new byte[0]));

        assertThat(thrown).isSameAs(primary);
        assertThat(thrown.getSuppressed()).containsExactly(closeFailure);
        assertThat(schedulerOf(source).isShutdown()).as("the poll is stopped").isTrue();
        assertThat(workerAcceptsWork(source)).as("the worker accepts work").isFalse();
        Throwable closeOnAnotherThread = CompletableFuture.supplyAsync(() -> catchThrowable(source::close)).get(5, TimeUnit.SECONDS);
        assertThat(closeOnAnotherThread).isNull();
        verify(channel, times(2)).basicNack(eq(HELD_DELIVERY_TAG), anyBoolean(), anyBoolean());
    }

    /**
     * Fails on its first attempt, so the exception that leaves build() is the one start() threw rather than one of the
     * three that the teardown after it throws. PARK, so that the failure action has a parking channel to close.
     */
    @Test
    void a_failed_build_throws_its_own_failure_with_the_teardown_failures_suppressed_on_it() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        Channel parkingChannel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel), Optional.of(parkingChannel));
        IOException qosFailure = new IOException("qos");
        IllegalStateException channelCloseFailure = new IllegalStateException("channel close");
        IllegalArgumentException parkingCloseFailure = new IllegalArgumentException("parking close");
        doThrow(qosFailure).when(channel).basicQos(1);
        doThrow(channelCloseFailure).when(channel).close();
        doThrow(parkingCloseFailure).when(parkingChannel).close();
        RabbitMqCloudEventSource.Builder builder = RabbitMqCloudEventSource.builder(connection, new PushSubscriptionModel(DataFieldReader.refusing()), "queue")
                .declareTopology(false)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of("parking-exchange", "parked"))
                .retryStrategy(RetryStrategy.none());

        Throwable thrown = catchThrowable(builder::build);

        assertThat(thrown).isInstanceOf(RabbitMqSourceException.class).hasCause(qosFailure);
        assertThat(thrown.getSuppressed()).containsExactly(channelCloseFailure, parkingCloseFailure);
        verify(channel).close();
        verify(parkingChannel).close();
    }

    @Test
    void a_failed_build_whose_channel_close_throws_the_very_failure_build_is_throwing_does_not_suppress_it_on_itself() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        IllegalStateException shared = new IllegalStateException("shared");
        doThrow(shared).when(channel).basicQos(1);
        doThrow(shared).when(channel).close();
        RabbitMqCloudEventSource.Builder builder = RabbitMqCloudEventSource.builder(connection, new PushSubscriptionModel(DataFieldReader.refusing()), "queue")
                .declareTopology(false)
                .retryStrategy(RetryStrategy.none());

        Throwable thrown = catchThrowable(builder::build);

        assertThat(thrown).isSameAs(shared);
        assertThat(thrown.getSuppressed()).isEmpty();
        verify(channel).close();
    }

    private static Stream<Duration> hugeCloseTimeouts() {
        return Stream.of(
                Duration.ofSeconds(Long.MAX_VALUE, 999_999_999),
                ChronoUnit.FOREVER.getDuration(),
                Duration.ofNanos(Long.MAX_VALUE),
                Duration.ofNanos(Long.MAX_VALUE).plusNanos(1));
    }

    private static Stream<Duration> waitingCloseTimeouts() {
        return Stream.concat(hugeCloseTimeouts(), Stream.of(Duration.ofNanos(Long.MAX_VALUE / 2 - 1)));
    }

    // Zero is left out of waitingCloseTimeouts() because it does not wait for the consume lock. A poll holding the
    // lock at that moment makes close() skip the consumer cancel and leave it to the channel close.
    private static Stream<Duration> closeTimeouts() {
        return Stream.concat(waitingCloseTimeouts(), Stream.of(Duration.ZERO));
    }

    private static ScheduledExecutorService schedulerOf(RabbitMqCloudEventSource source) throws ReflectiveOperationException {
        Field field = RabbitMqCloudEventSource.class.getDeclaredField("scheduler");
        field.setAccessible(true);
        return (ScheduledExecutorService) field.get(source);
    }

    /**
     * Hands the worker a task that cannot finish until released. A worker that accepted it is not idle until then, and
     * one that has stopped dropped it, which leaves it idle at once.
     */
    private static boolean workerAcceptsWork(RabbitMqCloudEventSource source) throws ReflectiveOperationException {
        Field field = RabbitMqCloudEventSource.class.getDeclaredField("worker");
        field.setAccessible(true);
        RabbitMqDeliveryWorker worker = (RabbitMqDeliveryWorker) field.get(source);
        CountDownLatch release = new CountDownLatch(1);
        try {
            worker.submit(HANDLED_DELIVERY_TAG, () -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            return !worker.isIdle();
        } finally {
            release.countDown();
        }
    }

    private static RabbitMqDeliveryFailureAction redeliveringAction(Channel channel) {
        return new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceTeardownTest.class));
    }

    private static Delivery delivery(long deliveryTag) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .build();
        return new Delivery(new Envelope(deliveryTag, false, "exchange", "routingKey"),
                RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of()), RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private static RabbitMqCloudEventSource sourceOver(Channel channel, DeliveryFailurePolicy policy) {
        return sourceOver(channel, new RabbitMqDeliveryFailureAction(channel, policy, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceTeardownTest.class)));
    }

    private static RabbitMqCloudEventSource sourceOver(Channel channel, RabbitMqDeliveryFailureAction failureAction) {
        return new RabbitMqCloudEventSource(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null, Duration.ofSeconds(1));
    }
}
