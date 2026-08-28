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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.RoutingOutcome.Disposition;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * {@link RabbitMqDomainEventBridge#route(RoutingOutcome, long, BasicProperties, byte[])} against a mocked
 * {@link Channel}, one call per {@link RoutingOutcome}, with no broker behind it. The domain-level twin of
 * {@code RabbitMqCloudEventBridgeOutcomeRoutingTest}, written because the two bridges used to sort the outcomes
 * separately and could disagree without either one failing a test.
 * <p>
 * {@code DomainEventFeed#acceptCloudEvent} reports three of the six today. The other three are checked here
 * anyway, since what decides them is {@link RoutingOutcome#disposition()} rather than which of them a feed
 * happens to report.
 */
class RabbitMqDomainEventBridgeOutcomeRoutingTest {

    private static final long DELIVERY_TAG = 42L;

    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DELIVERED", "FILTERED"})
    void an_outcome_a_caller_may_acknowledge_is_acknowledged_immediately(RoutingOutcome outcome) throws Exception {
        Channel channel = mock(Channel.class);

        Disposition disposition = bridgeOver(channel).route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.ACKNOWLEDGE);
        verify(channel).basicAck(DELIVERY_TAG, false);
    }

    /**
     * The event-loss case. An acknowledgement on any of these four tells RabbitMQ the message was consumed when
     * nothing consumed it, and the queue is then the only copy that existed.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "UNAVAILABLE", "NOT_DELIVERABLE", "REFUSED"})
    void an_outcome_a_caller_may_not_acknowledge_is_never_acknowledged(RoutingOutcome outcome) throws Exception {
        Channel channel = mock(Channel.class);

        bridgeOver(channel).route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(outcome.mayAcknowledge()).isFalse();
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    /**
     * Held for the poll to release rather than negatively acknowledged on the spot, which is what paces a
     * redelivery to one per poll interval instead of as fast as the broker can round-trip it.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "UNAVAILABLE"})
    void a_held_outcome_is_neither_acknowledged_nor_negatively_acknowledged(RoutingOutcome outcome) throws Exception {
        Channel channel = mock(Channel.class);

        Disposition disposition = bridgeOver(channel).route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.HOLD);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void not_deliverable_is_decided_by_the_configured_delivery_failure_policy() throws Exception {
        Channel channel = mock(Channel.class);

        Disposition disposition = bridgeOver(channel)
                .route(RoutingOutcome.NOT_DELIVERABLE, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.FAIL);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    /**
     * A permanent refusal stops the bridge outright. Closing the consume channel is what requeues every tag this
     * bridge was still holding, so the messages stay visible on the queue for whoever fixes the registration.
     */
    @Test
    void refused_stops_the_bridge_and_closes_its_consume_channel() throws Exception {
        Channel channel = mock(Channel.class);

        Disposition disposition = bridgeOver(channel)
                .route(RoutingOutcome.REFUSED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.STOP);
        verify(channel).close();
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    // The feed is left out because route(..) is handed an outcome the feed has already reported and never reads it.
    private static RabbitMqDomainEventBridge<Object> bridgeOver(Channel channel) {
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel,
                DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqDomainEventBridgeOutcomeRoutingTest.class));
        return new RabbitMqDomainEventBridge<>(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction);
    }
}
