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
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link RabbitMqCloudEventSource#releaseHeldDeferredDelivery(Deque, java.util.function.LongConsumer)} in
 * isolation, with no real {@code Channel} or broker behind it. A release whose nack was not sent (an
 * {@code IOException} surfacing as {@link RabbitMqSourceException}, or a {@code ShutdownSignalException}) must never
 * drop the tag it was for, and must not let a later tag in the same pass jump ahead of the one that just failed.
 * Any other failure may have come after the nack was sent and must not put the tag back, since a second nack closes
 * the channel. The poll stops the source for it instead, and closing the channel puts the delivery back on the queue.
 */
class RabbitMqCloudEventSourceReleaseHeldDeferredDeliveryTest {

    @Test
    void a_failed_release_puts_the_tag_back_at_the_front_and_stops_the_rest_of_the_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        AtomicInteger attempts = new AtomicInteger();
        List<Long> released = new CopyOnWriteArrayList<>();

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            attempts.incrementAndGet();
            if (tag == 1L) {
                throw new RabbitMqSourceException("Failed to negatively acknowledge delivery tag " + tag,
                        new IOException("channel hiccup"));
            }
            released.add(tag);
        }));

        assertThat(thrown).isInstanceOf(RabbitMqSourceException.class).hasMessageContaining("delivery tag 1");
        // Only the one attempt that failed: the pass stops there rather than moving on to tag 2, which would
        // reorder a later tag ahead of one still waiting to be retried.
        assertThat(attempts.get()).isEqualTo(1);
        assertThat(released).isEmpty();
        // Tag 1 is back at the front, ahead of tag 2, which this pass never got to.
        assertThat(held).containsExactly(1L, 2L);
    }

    /**
     * A nack that was never sent leaves the broker waiting on the tag, and at the default {@code prefetchCount} of
     * one it sends the consumer nothing further until the tag is answered. {@code ShutdownSignalException} is what
     * the channel throws when it is already closing, before anything went out.
     */
    @Test
    void a_release_that_fails_with_a_shutdown_signal_puts_the_tag_back_at_the_front() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        List<Long> released = new CopyOnWriteArrayList<>();
        ShutdownSignalException shutdown = new ShutdownSignalException(true, false, null, null);

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            if (tag == 1L) {
                throw shutdown;
            }
            released.add(tag);
        }));

        assertThat(thrown).isSameAs(shutdown);
        assertThat(released).isEmpty();
        assertThat(held).containsExactly(1L, 2L);
    }

    /**
     * Anything else may come from an application's {@code TrafficListener} before the nack is written or from its
     * {@code MetricsCollector} after. Nacking the same tag a second time makes the broker close the channel, so the
     * failure is rethrown and the tag is not put back, which leaves only the tag this pass never got to.
     */
    @Test
    void a_release_that_fails_with_an_error_that_may_have_followed_the_nack_does_not_put_the_tag_back() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        List<Long> released = new CopyOnWriteArrayList<>();
        StackOverflowError error = new StackOverflowError("a metrics collector that recursed");

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            if (tag == 1L) {
                throw error;
            }
            released.add(tag);
        }));

        assertThat(thrown).isSameAs(error);
        assertThat(released).isEmpty();
        assertThat(held).containsExactly(2L);
    }

    @Test
    void a_release_that_fails_with_a_runtime_exception_that_may_have_followed_the_nack_does_not_put_the_tag_back() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        IllegalStateException failure = new IllegalStateException("a traffic listener or a metrics collector that threw");

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            if (tag == 1L) {
                throw failure;
            }
        }));

        assertThat(thrown).isSameAs(failure);
        assertThat(held).containsExactly(2L);
    }

    /**
     * At the default {@code prefetchCount} of one the broker sends nothing further until the tag is answered, so a
     * poll that dropped it and carried on would leave the source stuck until its channel closed.
     */
    @Test
    void a_poll_whose_release_may_have_sent_the_nack_stops_the_source_and_closes_its_channel_with_no_second_nack() throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new IllegalStateException("thrown by a TrafficListener")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> { });
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceReleaseHeldDeferredDeliveryTest.class));
        RabbitMqCloudEventSource source = new RabbitMqCloudEventSource(model, channel, "queue", 1, Duration.ofSeconds(1), failureAction,
                subscriptionId -> true, Duration.ofSeconds(1));
        source.route(RoutingOutcome.DEFERRED, 1L, new BasicProperties(), new byte[0]);

        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);

        verify(channel).close();
        verify(channel, times(1)).basicNack(1L, false, true);
    }

    /**
     * Closing the channel puts every unacknowledged delivery back on the queue, but the client can still dispatch one
     * it read before the close. The handler of such a delivery would write its effects, fail to acknowledge on the
     * closed channel, and see the redelivered copy again. The worker stops accepting work before the channel is
     * closed, so it never starts that handler. The close below dispatches the delivery and then waits until the
     * worker has either refused it or finished handling it, so the outcome is settled before the assertion.
     */
    @Test
    void a_delivery_dispatched_after_a_poll_stopped_the_source_and_closed_its_channel_is_not_handled() throws Exception {
        Channel channel = mock(Channel.class);
        AtomicReference<DeliverCallback> deliverCallback = new AtomicReference<>();
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenAnswer(invocation -> {
            deliverCallback.set(invocation.getArgument(2));
            return "consumer-tag";
        });
        doThrow(new IllegalStateException("thrown by a TrafficListener")).when(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());
        List<String> handled = new CopyOnWriteArrayList<>();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent.getId()));
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceReleaseHeldDeferredDeliveryTest.class));
        RabbitMqCloudEventSource source = new RabbitMqCloudEventSource(model, channel, "queue", 1, Duration.ofSeconds(1), failureAction,
                subscriptionId -> true, Duration.ofSeconds(1));
        RabbitMqDeliveryWorker worker = worker(source);
        doAnswer(invocation -> {
            deliverCallback.get().handle("consumer-tag", delivery(2L));
            await().atMost(Duration.ofSeconds(5)).until(() -> !handled.isEmpty() || worker.isIdle());
            return null;
        }).when(channel).close();
        source.route(RoutingOutcome.DEFERRED, 1L, new BasicProperties(), new byte[0]);

        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);

        verify(channel).close();
        assertThat(handled).isEmpty();
    }

    private static RabbitMqDeliveryWorker worker(RabbitMqCloudEventSource source) throws ReflectiveOperationException {
        Field worker = RabbitMqCloudEventSource.class.getDeclaredField("worker");
        worker.setAccessible(true);
        return (RabbitMqDeliveryWorker) worker.get(source);
    }

    private static Delivery delivery(long deliveryTag) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-" + deliveryTag)
                .withSource(URI.create("urn:test"))
                .withType("t")
                .build();
        return new Delivery(new Envelope(deliveryTag, false, "exchange", "routingKey"),
                RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of()), RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    @Test
    void a_tag_that_failed_once_is_released_on_the_next_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L, 2L));
        AtomicInteger attempts = new AtomicInteger();
        List<Long> released = new CopyOnWriteArrayList<>();

        Throwable thrown = catchThrowable(() -> RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            if (attempts.getAndIncrement() == 0) {
                throw new RabbitMqSourceException("Failed to negatively acknowledge delivery tag " + tag,
                        new IOException("channel hiccup"));
            }
            released.add(tag);
        }));
        assertThat(thrown).isInstanceOf(RabbitMqSourceException.class);
        assertThat(held).containsExactly(1L, 2L);

        // The next poll's own call to this same method, against the channel that survived the earlier failure.
        RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            attempts.incrementAndGet();
            released.add(tag);
        });

        assertThat(released).containsExactly(1L, 2L);
        assertThat(held).isEmpty();
    }

    @Test
    void a_tag_added_while_the_pass_is_running_is_not_released_until_the_next_pass() {
        Deque<Long> held = new ArrayDeque<>(List.of(1L));
        List<Long> released = new CopyOnWriteArrayList<>();

        RabbitMqCloudEventSource.releaseHeldDeferredDelivery(held, tag -> {
            released.add(tag);
            // A redelivery this release itself triggers, landing back on the same consumer and reporting
            // DEFERRED again before this pass has finished: the exact race the snapshot-count fix exists for.
            held.addLast(2L);
        });

        assertThat(released).containsExactly(1L);
        // The snapshot at entry was 1 tag, so the loop already ran its one iteration and stopped, leaving the
        // tag appended mid-pass for the next poll to pick up instead of nacking it in the same pass.
        assertThat(held).containsExactly(2L);
    }
}
