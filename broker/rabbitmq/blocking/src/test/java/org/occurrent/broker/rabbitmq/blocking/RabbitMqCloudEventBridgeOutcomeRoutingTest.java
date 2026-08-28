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
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.RoutingOutcome.Disposition;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * {@link RabbitMqCloudEventBridge#route(RoutingOutcome, long, BasicProperties, byte[])} against a mocked
 * {@link Channel}, one call per {@link RoutingOutcome}, with no broker behind it. The RabbitMQ half of what
 * {@code RoutingOutcomeTest} states for the outcomes themselves, so a mapping that compiles but sends an outcome
 * to the wrong branch fails here rather than on a queue somewhere.
 */
class RabbitMqCloudEventBridgeOutcomeRoutingTest {

    private static final long DELIVERY_TAG = 42L;

    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DELIVERED", "FILTERED"})
    void an_outcome_a_caller_may_acknowledge_is_acknowledged_immediately(RoutingOutcome outcome) throws Exception {
        Channel channel = mock(Channel.class);

        Disposition disposition = bridgeOver(channel, DeliveryFailurePolicy.REDELIVER)
                .route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.ACKNOWLEDGE);
        verify(channel).basicAck(DELIVERY_TAG, false);
    }

    /**
     * The event-loss case. An acknowledgement on any of these three tells RabbitMQ the message was consumed when
     * nothing consumed it, and the queue is then the only copy that existed. Checked under both delivery failure
     * policies, since {@link DeliveryFailurePolicy#PARK} does acknowledge a parked message once its parking
     * publish is confirmed, and none of these three reaches parking.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "UNAVAILABLE", "REFUSED"})
    void an_outcome_a_caller_may_not_acknowledge_is_never_acknowledged_under_either_policy(RoutingOutcome outcome) throws Exception {
        Channel redelivering = mock(Channel.class);
        Channel parking = mock(Channel.class);

        bridgeOver(redelivering, DeliveryFailurePolicy.REDELIVER).route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);
        bridgeOverParking(parking, mock(RabbitMqConfirmPublisher.class)).route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(outcome.mayAcknowledge()).isFalse();
        verify(redelivering, never()).basicAck(anyLong(), anyBoolean());
        verify(parking, never()).basicAck(anyLong(), anyBoolean());
    }

    /**
     * {@code DEFERRED} and {@code UNAVAILABLE} are held for the poll to release rather than sent through the
     * configured policy, so {@link DeliveryFailurePolicy#PARK} does not park them either. Parking exists to move a
     * failed message out of the retry loop, and neither of these is a failure.
     */
    @ParameterizedTest
    @EnumSource(value = RoutingOutcome.class, names = {"DEFERRED", "UNAVAILABLE"})
    void a_held_outcome_bypasses_the_delivery_failure_policy_including_parking(RoutingOutcome outcome) throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);

        Disposition disposition = bridgeOverParking(channel, parkingPublisher)
                .route(outcome, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.HOLD);
        verify(parkingPublisher, never()).publish(anyString(), anyString(), any(), any());
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    /**
     * {@code NOT_DELIVERABLE} is the one outcome the configured policy decides, so it is what does reach parking.
     * Paired with the test above, which is what shows the bypass is about the outcome rather than about parking
     * being unreachable from here.
     */
    @Test
    void not_deliverable_is_parked_when_the_policy_is_PARK() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        BasicProperties properties = new BasicProperties();

        Disposition disposition = bridgeOverParking(channel, parkingPublisher)
                .route(RoutingOutcome.NOT_DELIVERABLE, DELIVERY_TAG, properties, new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.FAIL);
        verify(parkingPublisher).publish("exchange", "routingKey", properties, new byte[0]);
    }

    /**
     * A permanent refusal stops the bridge outright. Closing the consume channel is what requeues every tag this
     * bridge was still holding, so the messages stay visible on the queue for whoever fixes the registration.
     */
    @Test
    void refused_stops_the_bridge_and_closes_its_consume_channel() throws Exception {
        Channel channel = mock(Channel.class);

        Disposition disposition = bridgeOver(channel, DeliveryFailurePolicy.REDELIVER)
                .route(RoutingOutcome.REFUSED, DELIVERY_TAG, new BasicProperties(), new byte[0]);

        assertThat(disposition).isEqualTo(Disposition.STOP);
        verify(channel).close();
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    private static RabbitMqCloudEventBridge bridgeOver(Channel channel, DeliveryFailurePolicy policy) {
        return bridgeOver(channel, new RabbitMqDeliveryFailureAction(channel, policy, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventBridgeOutcomeRoutingTest.class)));
    }

    private static RabbitMqCloudEventBridge bridgeOverParking(Channel channel, RabbitMqConfirmPublisher parkingPublisher) {
        return bridgeOver(channel, new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.PARK, parkingPublisher,
                RabbitMqDestination.of("exchange", "routingKey"),
                LoggerFactory.getLogger(RabbitMqCloudEventBridgeOutcomeRoutingTest.class)));
    }

    // The model, the outcome channel and the readiness source are left out because route(..) is handed an outcome
    // that has already been reported and reads none of the three.
    private static RabbitMqCloudEventBridge bridgeOver(Channel channel, RabbitMqDeliveryFailureAction failureAction) {
        return new RabbitMqCloudEventBridge(null, null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null);
    }
}
