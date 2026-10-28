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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqConsumerRecovery;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryWorker;
import org.occurrent.subscription.RoutingOutcome;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * Removing a recovery listener is one shutdown step among the others, so its failure must not skip the steps after
 * it. Skipping the consumer cancel and the wait for the worker lets a projection that is still running acknowledge on a
 * channel that is already closed, and RabbitMQ then delivers the message it handled a second time.
 */
class RabbitMqDomainEventSourceShutdownStepFailureTest {

    private static final String CONSUMER_TAG = "consumer-tag-1";
    private static final long HELD_DELIVERY_TAG = 42L;
    private static final long HANDLED_DELIVERY_TAG = 7L;

    private final Channel channel = mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
    private final RabbitMqDomainEventSource<Object> source = new RabbitMqDomainEventSource<>(null, channel, "queue", 1, Duration.ofSeconds(1),
            new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                    LoggerFactory.getLogger(RabbitMqDomainEventSourceShutdownStepFailureTest.class)),
            Duration.ofSeconds(5));

    @Test
    void close_still_cancels_the_consumer_and_waits_for_a_running_projection_when_removing_the_workers_recovery_listener_throws() throws Exception {
        IllegalStateException removalFailure = new IllegalStateException("channel closed");
        RabbitMqDeliveryWorker worker = field("worker", RabbitMqDeliveryWorker.class);
        worker.discardOnRecovery(channel);
        doThrow(removalFailure).when((Recoverable) channel).removeRecoveryListener(any());
        field("consumerTag", AtomicReference.class).set(CONSUMER_TAG);
        AtomicBoolean projectionFinished = new AtomicBoolean();
        CountDownLatch projectionStarted = new CountDownLatch(1);
        worker.submit(HANDLED_DELIVERY_TAG, () -> {
            projectionStarted.countDown();
            sleepUninterruptibly();
            projectionFinished.set(true);
        });
        assertThat(projectionStarted.await(5, TimeUnit.SECONDS)).isTrue();

        Throwable thrown = catchThrowable(source::close);

        assertThat(projectionFinished).as("the projection had finished when the channel closed").isTrue();
        assertThat(thrown).isSameAs(removalFailure);
        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel).basicCancel(CONSUMER_TAG);
        inOrder.verify(channel).close();
    }

    @Test
    void close_still_cancels_the_consumer_when_detaching_from_connection_recoveries_throws() throws Exception {
        IllegalStateException removalFailure = new IllegalStateException("connection closed");
        attachToConnectionWhoseRemovalThrows(removalFailure);
        field("consumerTag", AtomicReference.class).set(CONSUMER_TAG);

        Throwable thrown = catchThrowable(source::close);

        assertThat(thrown).isSameAs(removalFailure);
        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel).basicCancel(CONSUMER_TAG);
        inOrder.verify(channel).close();
    }

    @Test
    void a_permanent_stop_still_cancels_the_consumer_and_releases_a_held_delivery_when_detaching_from_connection_recoveries_throws() throws Exception {
        IllegalStateException removalFailure = new IllegalStateException("connection closed");
        attachToConnectionWhoseRemovalThrows(removalFailure);
        field("consumerTag", AtomicReference.class).set(CONSUMER_TAG);
        source.route(RoutingOutcome.DEFERRED, HELD_DELIVERY_TAG, new BasicProperties(), new byte[0]);

        Throwable thrown = catchThrowable(() -> source.route(RoutingOutcome.REFUSED, HELD_DELIVERY_TAG + 1, new BasicProperties(), new byte[0]));

        assertThat(thrown).isSameAs(removalFailure);
        InOrder inOrder = inOrder(channel);
        inOrder.verify(channel).basicCancel(CONSUMER_TAG);
        inOrder.verify(channel).basicNack(HELD_DELIVERY_TAG, false, true);
        inOrder.verify(channel).close();
    }

    private void attachToConnectionWhoseRemovalThrows(RuntimeException removalFailure) throws ReflectiveOperationException {
        AutorecoveringConnection connection = mock(AutorecoveringConnection.class);
        doThrow(removalFailure).when(connection).removeRecoveryListener(any());
        field("consumerRecovery", RabbitMqConsumerRecovery.class).attachTo(connection);
    }

    @SuppressWarnings("unchecked")
    private <T> T field(String name, Class<T> type) throws ReflectiveOperationException {
        Field field = RabbitMqDomainEventSource.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(source);
    }

    // Long enough that a close() that does not wait closes the channel well before it ends
    private static void sleepUninterruptibly() {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        boolean interrupted = false;
        while (System.nanoTime() - end < 0) {
            try {
                Thread.sleep(Math.max(1, TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime())));
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
