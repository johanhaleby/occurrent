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
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.RecoveryAwareChannelN;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.subscription.RoutingOutcome;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Drives the bridge's own recovery listener by hand over a mocked channel, since a real broker gives no way to hold a
 * projection blocked while deciding exactly which deliveries arrive before and after a recovery starts. Real
 * recoveries are covered for the CloudEvent-level bridge in {@code RabbitMqCloudEventBridgeConnectionRecoveryTest}.
 */
class RabbitMqDomainEventBridgeRecoveryDiscardTest {

    /**
     * A channel that is not the client's own recovering channel says nothing about how it numbers deliveries after a
     * recovery, so the bridge drops nothing for it rather than risk dropping a fresh delivery.
     */
    @SuppressWarnings("unchecked")
    @Test
    void a_recovery_of_a_channel_that_does_not_expose_its_delivery_tag_offset_drops_nothing() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        AtomicReference<DeliverCallback> deliverCallback = new AtomicReference<>();
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenAnswer(invocation -> {
                    deliverCallback.set(invocation.getArgument(2));
                    return "consumer-tag";
                });

        DomainEventFeed<String> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        when(feed.isReadyForLiveDelivery()).thenReturn(true);
        CountDownLatch firstCallEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        List<String> handled = new CopyOnWriteArrayList<>();
        when(feed.acceptCloudEvent(any())).thenAnswer(invocation -> {
            CloudEvent cloudEvent = invocation.getArgument(0);
            handled.add(cloudEvent.getId());
            if (handled.size() == 1) {
                firstCallEntered.countDown();
                releaseFirstCall.await(10, TimeUnit.SECONDS);
            }
            return RoutingOutcome.DELIVERED;
        });

        RabbitMqDomainEventBridge<String> bridge = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(20))
                .build();
        try {
            ArgumentCaptor<RecoveryListener> recoveryListener = ArgumentCaptor.forClass(RecoveryListener.class);
            verify((Recoverable) channel).addRecoveryListener(recoveryListener.capture());
            verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

            deliverCallback.get().handle("consumer-tag", delivery(1, "blocked"));
            assertThat(firstCallEntered.await(5, TimeUnit.SECONDS)).isTrue();
            deliverCallback.get().handle("consumer-tag", delivery(2, "queued-before-the-recovery"));

            recoveryListener.getValue().handleRecoveryStarted((Recoverable) channel);
            // Numbered from 1 again, the way a channel with no offset of its own might number them.
            deliverCallback.get().handle("consumer-tag", delivery(1, "from-the-recovered-channel"));
            releaseFirstCall.countDown();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled)
                    .containsExactly("blocked", "queued-before-the-recovery", "from-the-recovered-channel"));
        } finally {
            releaseFirstCall.countDown();
            bridge.close();
        }
    }

    /**
     * The client can still run a callback from the dead channel after the recovery has started. The replacement
     * channel's offset says which tags the dead channel issued, so that late delivery is dropped too.
     */
    @SuppressWarnings("unchecked")
    @Test
    void a_delivery_from_the_dead_channel_that_reaches_the_bridge_after_the_recovery_started_is_dropped() throws Exception {
        Connection connection = mock(Connection.class);
        AutorecoveringChannel channel = mock(AutorecoveringChannel.class);
        RecoveryAwareChannelN replacement = mock(RecoveryAwareChannelN.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.getDelegate()).thenReturn(replacement);
        // The dead channel issued tags 1 to 3, so the replacement numbers its deliveries from 4.
        when(replacement.getActiveDeliveryTagOffset()).thenReturn(3L);
        AtomicReference<DeliverCallback> deliverCallback = new AtomicReference<>();
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenAnswer(invocation -> {
                    deliverCallback.set(invocation.getArgument(2));
                    return "consumer-tag";
                });

        DomainEventFeed<String> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        when(feed.isReadyForLiveDelivery()).thenReturn(true);
        CountDownLatch firstCallEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        List<String> handled = new CopyOnWriteArrayList<>();
        when(feed.acceptCloudEvent(any())).thenAnswer(invocation -> {
            CloudEvent cloudEvent = invocation.getArgument(0);
            handled.add(cloudEvent.getId());
            if (handled.size() == 1) {
                firstCallEntered.countDown();
                releaseFirstCall.await(10, TimeUnit.SECONDS);
            }
            return RoutingOutcome.DELIVERED;
        });

        RabbitMqDomainEventBridge<String> bridge = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(20))
                .build();
        try {
            ArgumentCaptor<RecoveryListener> recoveryListener = ArgumentCaptor.forClass(RecoveryListener.class);
            verify(channel).addRecoveryListener(recoveryListener.capture());
            verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

            deliverCallback.get().handle("consumer-tag", delivery(1, "blocked"));
            assertThat(firstCallEntered.await(5, TimeUnit.SECONDS)).isTrue();
            deliverCallback.get().handle("consumer-tag", delivery(2, "queued-on-the-dead-channel"));

            recoveryListener.getValue().handleRecoveryStarted(channel);
            deliverCallback.get().handle("consumer-tag", delivery(3, "late-from-the-dead-channel"));
            deliverCallback.get().handle("consumer-tag", delivery(4, "from-the-recovered-channel"));
            releaseFirstCall.countDown();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(2));
            Thread.sleep(200);
            assertThat(handled).containsExactly("blocked", "from-the-recovered-channel");
        } finally {
            releaseFirstCall.countDown();
            bridge.close();
        }
    }

    private static Delivery delivery(long deliveryTag, String id) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("t")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        return new Delivery(new Envelope(deliveryTag, false, "exchange", "routingKey"), properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }
}
