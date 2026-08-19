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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.subscription.UnreadableLiveFilterException;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Proves the specific ordering {@code reconcileConsumption()} and {@code stopPermanently()} both read and write
 * {@code permanentlyStopped} under, by reproducing, deterministically enough for a regression test, a poll tick
 * that is mid-decision while a concurrent permanent stop tries to cancel the consumer. A real broker gives no way
 * to force this exact interleaving on demand, so this drives the bridge against a mocked {@link Connection} and
 * {@link Channel}, and forces the interleaving directly with latches around a stubbed
 * {@link DomainEventFeed#hasProjection()}, which {@code reconcileConsumption()} only calls after acquiring
 * {@code consumeLock}.
 * <p>
 * That placement is what this test can actually tell apart from a regression. Since the blocked tick already holds
 * {@code consumeLock} for the whole time {@link DomainEventFeed#hasProjection()} is stuck, a concurrent
 * {@code stopPermanently()} cannot even start deciding the consumer's fate until the tick's own decision finishes
 * and releases the lock, so whichever of the two wins the lock next fully decides that fate before the other reads
 * anything. This does not prove that checking {@code permanentlyStopped} on its own, ahead of {@code consumeLock},
 * would stay safe if a later change kept {@link DomainEventFeed#hasProjection()} itself inside the lock, since the
 * blocked call this test hooks would no longer be the one holding the lock either. That variant never shipped, and
 * the fix keeps the flag check and the {@link DomainEventFeed#hasProjection()} read in the same locked scope
 * together rather than relying on a test to catch them drifting apart.
 */
class RabbitMqDomainEventBridgeStopPermanentlyRaceTest {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);

    @SuppressWarnings("unchecked")
    @Test
    void a_poll_tick_racing_a_permanent_stop_does_not_restart_the_consumer() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));

        AtomicReference<DeliverCallback> deliverCallback = new AtomicReference<>();
        AtomicInteger consumeCalls = new AtomicInteger();
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenAnswer(invocation -> {
                    deliverCallback.set(invocation.getArgument(2));
                    return "consumer-tag-" + consumeCalls.incrementAndGet();
                });

        DomainEventFeed<String> feed = mock(DomainEventFeed.class);
        // The first reconcile tick answers immediately, establishing the consumer. The second tick is where this
        // test forces the race: it blocks here, mid-reconcile, the same point a concurrent permanent stop has to
        // contend with.
        AtomicInteger hasProjectionCalls = new AtomicInteger();
        CountDownLatch secondTickBlocked = new CountDownLatch(1);
        CountDownLatch releaseSecondTick = new CountDownLatch(1);
        when(feed.hasProjection()).thenAnswer(invocation -> {
            if (hasProjectionCalls.incrementAndGet() == 2) {
                secondTickBlocked.countDown();
                releaseSecondTick.await(5, TimeUnit.SECONDS);
            }
            return true;
        });
        // The coarse poll now also gates on isReadyForLiveDelivery(); stubbed true throughout, since this test's
        // race is about the hasProjection()/permanent-stop interleaving, not about readiness.
        when(feed.isReadyForLiveDelivery()).thenReturn(true);
        when(feed.acceptCloudEvent(any())).thenThrow(new UnreadableLiveFilterException("unreadable", new UnsupportedOperationException()));

        RabbitMqDomainEventBridge<String> bridge = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build();
        try {
            verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
            assertThat(secondTickBlocked.await(2, TimeUnit.SECONDS)).as("second reconcile tick should be blocked in hasProjection()").isTrue();

            // Simulate a delivery arriving right now, on what would be the AMQP consumer thread, triggering the
            // permanent stop from inside handleDelivery while the second tick is still blocked. Run on its own
            // thread: in the fixed code stopPermanently() contends for the same lock the blocked tick holds, so
            // calling it inline here would block this test thread too, before it ever reached the release below.
            Thread delivery = new Thread(() -> {
                try {
                    deliverCallback.get().handle("consumer-tag-1", undecodableFilterDelivery());
                } catch (Exception ignored) {
                    // Nothing to assert on here; the outcome this test checks is what the channel mock recorded.
                }
            });
            delivery.start();

            // Gives the permanent stop a head start. In the buggy ordering (the permanent-stop flag read before
            // contending for the lock) this is enough time for it to run to completion uncontended, since the
            // blocked tick never touches the lock until it is released below, so by the time this releases the
            // tick, consumerTag is already null and the bug is reproduced deterministically rather than by luck of
            // scheduling. The fixed ordering is unaffected either way, since there stopPermanently is blocked on
            // the same lock the tick holds regardless of how long this waits.
            Thread.sleep(500);
            releaseSecondTick.countDown();

            verify(channel, timeout(2000)).basicCancel("consumer-tag-1");
            delivery.join(2000);

            // Gives a poll tick still queued behind the one this test blocked, or the scheduler's next one, a
            // chance to run and (wrongly) restart the consumer if the fix did not actually close the race, before
            // asserting on the total count below.
            Thread.sleep(POLL_INTERVAL.toMillis() * 10);

            // The invariant under test: only the very first tick may ever have called basicConsume. Whatever the
            // blocked tick decided to do with the shouldConsume it read, and whatever any tick after it decided,
            // none may restart the consumer once the permanent stop has cancelled it.
            verify(channel, times(1)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
        } finally {
            bridge.close();
        }
    }

    private static Delivery undecodableFilterDelivery() {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        byte[] body = RabbitMqCloudEventMapper.toBody(cloudEvent);
        return new Delivery(new Envelope(1L, false, "exchange", "routingKey"), properties, body);
    }
}
