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
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.RoutingOutcome.Disposition;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Deque;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link RabbitMqCloudEventBridge#route(RoutingOutcome, long, BasicProperties, byte[])} against a mocked
 * {@link Channel}, one call per {@link RoutingOutcome}, with no broker behind it. The RabbitMQ half of what
 * {@code RoutingOutcomeTest} states for the outcomes themselves, so a mapping that compiles but sends an outcome
 * to the wrong branch fails here rather than on a queue somewhere. The tests that go through {@code handleDelivery}
 * put a mocked {@link PushSubscriptionModel} in front, so what the bridge does follows from the outcome
 * {@code acceptRedeliverable(..)} returns and from nothing else.
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

    @Test
    void a_returned_deferred_is_held_unacknowledged_and_bypasses_the_failure_policy() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqCloudEventBridge bridge = parkingBridgeOver(modelReturning(RoutingOutcome.DEFERRED), channel, parkingPublisher);

        handleDelivery(bridge, delivery());

        assertThat(heldDeferredDeliveryTags(bridge)).containsExactly(DELIVERY_TAG);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        verify(parkingPublisher, never()).publish(anyString(), anyString(), any(), any());
    }

    @Test
    void a_returned_not_deliverable_goes_to_the_failure_policy() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqCloudEventBridge bridge = parkingBridgeOver(modelReturning(RoutingOutcome.NOT_DELIVERABLE), channel, parkingPublisher);

        handleDelivery(bridge, delivery());

        verify(parkingPublisher).publish(anyString(), anyString(), any(), any());
        assertThat(heldDeferredDeliveryTags(bridge)).isEmpty();
        verify(channel, never()).close();
    }

    @Test
    void a_returned_refused_stops_the_bridge_without_the_failure_policy() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqCloudEventBridge bridge = parkingBridgeOver(modelReturning(RoutingOutcome.REFUSED), channel, parkingPublisher);

        handleDelivery(bridge, delivery());

        verify(channel).close();
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(parkingPublisher, never()).publish(anyString(), anyString(), any(), any());
    }

    @Test
    void a_filter_or_handler_failure_thrown_by_the_model_goes_to_the_failure_policy() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        PushSubscriptionModel model = mock(PushSubscriptionModel.class);
        when(model.acceptRedeliverable(any(CloudEvent.class))).thenThrow(new IllegalStateException("handler failed"));
        RabbitMqCloudEventBridge bridge = parkingBridgeOver(model, channel, parkingPublisher);

        handleDelivery(bridge, delivery());

        verify(parkingPublisher).publish(anyString(), anyString(), any(), any());
        verify(channel, never()).close();
    }

    private static PushSubscriptionModel modelReturning(RoutingOutcome outcome) {
        PushSubscriptionModel model = mock(PushSubscriptionModel.class);
        when(model.acceptRedeliverable(any(CloudEvent.class))).thenReturn(outcome);
        return model;
    }

    private static RabbitMqCloudEventBridge parkingBridgeOver(PushSubscriptionModel model, Channel channel,
                                                              RabbitMqConfirmPublisher parkingPublisher) {
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.PARK,
                parkingPublisher, RabbitMqDestination.of("exchange", "routingKey"),
                LoggerFactory.getLogger(RabbitMqCloudEventBridgeOutcomeRoutingTest.class));
        return new RabbitMqCloudEventBridge(model, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null, Duration.ofSeconds(1));
    }

    private static Delivery delivery() {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("com.acme.OrderPlaced")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        return new Delivery(new Envelope(DELIVERY_TAG, false, "exchange", "routingKey"), properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private static void handleDelivery(RabbitMqCloudEventBridge bridge, Delivery delivery) throws Exception {
        Method method = RabbitMqCloudEventBridge.class.getDeclaredMethod("handleDelivery", Delivery.class);
        method.setAccessible(true);
        try {
            method.invoke(bridge, delivery);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof Exception exception) {
                throw exception;
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private static Deque<Long> heldDeferredDeliveryTags(RabbitMqCloudEventBridge bridge) throws ReflectiveOperationException {
        Field field = RabbitMqCloudEventBridge.class.getDeclaredField("heldDeferredDeliveryTags");
        field.setAccessible(true);
        return (Deque<Long>) field.get(bridge);
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

    // The model and the readiness source are left out because route(..) is handed an outcome that has already been
    // reported and reads neither.
    private static RabbitMqCloudEventBridge bridgeOver(Channel channel, RabbitMqDeliveryFailureAction failureAction) {
        return new RabbitMqCloudEventBridge(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null, Duration.ofSeconds(1));
    }
}
