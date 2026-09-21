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

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The coarse lifecycle poll is the only thing that starts this bridge's consumer, cancels it when the registration
 * goes away, and releases a held delivery. Anything escaping the poll cancels the scheduled task for good, so the
 * bridge would do none of those again, and a held delivery would sit unacknowledged with nothing left to release it,
 * all without a word. This drives the bridge against a mocked {@link Connection} and {@link Channel}, since the
 * failure under test is a throw out of {@link DomainEventFeed#isReadyForLiveDelivery()}, which a real feed has no
 * reason to do.
 * <p>
 * An {@code Error} rather than a checked exception, unlike the CloudEvent bridge's own twin of this test. What the
 * domain poll calls is {@link DomainEventFeed}, which is Occurrent's own and declares no checked exception, so an
 * {@code Error} is the reachable half here. A {@code StackOverflowError} out of a recursive projection registration
 * is the shape of it.
 */
class RabbitMqDomainEventBridgePollFailureTest {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);

    @SuppressWarnings("unchecked")
    @Test
    void an_error_from_the_feed_does_not_end_the_poll_that_starts_this_bridges_consumer() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn("consumer-tag-1");

        DomainEventFeed<String> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        AtomicInteger readinessCalls = new AtomicInteger();
        when(feed.isReadyForLiveDelivery()).thenAnswer(invocation -> {
            if (readinessCalls.incrementAndGet() == 1) {
                throw new StackOverflowError("a projection registration that recursed");
            }
            return true;
        });

        RabbitMqDomainEventBridge<String> bridge = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build();
        try {
            // A poll the first tick's Error took down with it never gets here, however long this waits.
            verify(channel, timeout(2000)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
        } finally {
            bridge.close();
        }
    }
}
