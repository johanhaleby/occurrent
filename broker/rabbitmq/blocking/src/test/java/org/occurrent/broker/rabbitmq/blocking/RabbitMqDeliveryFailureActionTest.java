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
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Unit tests for {@link RabbitMqDeliveryFailureAction} against mocked RabbitMQ client types and a mocked
 * {@link RabbitMqConfirmPublisher}, for behaviour a real broker has no way to force on demand. Confirm-publish
 * correctness itself, retirement on timeout and the confirmSelect-failure cleanup, lives in
 * {@link RabbitMqConfirmPublisherTest} now that both parking paths share one implementation of it.
 */
class RabbitMqDeliveryFailureActionTest {

    /**
     * {@link DeliveryFailurePolicy#REDELIVER} never publishes anywhere, so {@code create(...)} must not open a
     * parking channel at all, even when a {@code parkingDestination} happens to be supplied alongside it. Opening
     * one anyway would waste a channel nothing will ever use, and could fail startup for a resource the bridge
     * never needed.
     */
    @Test
    void create_opens_no_parking_resources_when_the_policy_is_REDELIVER_even_with_a_parkingDestination_given() {
        Connection connection = mock(Connection.class);
        Channel consumeChannel = mock(Channel.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");

        RabbitMqDeliveryFailureAction.create(connection, consumeChannel, DeliveryFailurePolicy.REDELIVER,
                parkingDestination, LoggerFactory.getLogger(getClass()));

        verifyNoInteractions(connection);
    }

    /**
     * A failed parking publish, for any reason the shared {@link RabbitMqConfirmPublisher} reports as a
     * {@link RuntimeException}, redelivers the original instead of losing it or letting the exception escape
     * {@code apply}.
     */
    @Test
    void apply_redelivers_when_the_parking_publisher_throws() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");
        doThrow(new RabbitMqPublishException("publish failed"))
                .when(parkingPublisher).publish("exchange", "routingKey", new BasicProperties(), new byte[0]);
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK,
                parkingPublisher, parkingDestination, LoggerFactory.getLogger(getClass()));

        action.apply(42L, new BasicProperties(), new byte[0]);

        verify(consumeChannel).basicNack(42L, false, true);
        verify(consumeChannel, never()).basicAck(anyLong(), anyBoolean());
    }

    /**
     * {@link DeliveryFailurePolicy#PARK} republishes the delivery's own raw {@code properties} unchanged, a
     * caller-supplied {@code correlationId} included, rather than rebuilding them from a decoded {@code CloudEvent}
     * and losing every AMQP field outside the CloudEvents mapping.
     */
    @Test
    void apply_parks_the_original_properties_unchanged_preserving_metadata_outside_the_cloudEvents_mapping() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");
        BasicProperties originalProperties = new BasicProperties.Builder().correlationId("caller-correlation-id").build();
        byte[] originalBody = "payload".getBytes();
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK,
                parkingPublisher, parkingDestination, LoggerFactory.getLogger(getClass()));

        action.apply(42L, originalProperties, originalBody);

        verify(parkingPublisher).publish("exchange", "routingKey", originalProperties, originalBody);
    }

    /**
     * {@code parkingDestination}'s own configured headers reach the parked message alongside the delivery's
     * original AMQP fields, rather than being dropped in favor of the raw properties alone.
     */
    @Test
    void apply_merges_the_parking_destinations_own_headers_onto_the_original_properties() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey")
                .withHeaders(Map.of("parked-reason", "handler-failure"));
        BasicProperties originalProperties = new BasicProperties.Builder()
                .correlationId("caller-correlation-id")
                .headers(Map.of("tenant", "acme"))
                .build();
        byte[] originalBody = "payload".getBytes();
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK,
                parkingPublisher, parkingDestination, LoggerFactory.getLogger(getClass()));

        action.apply(42L, originalProperties, originalBody);

        ArgumentCaptor<BasicProperties> parkedPropertiesCaptor = ArgumentCaptor.forClass(BasicProperties.class);
        verify(parkingPublisher).publish(eq("exchange"), eq("routingKey"), parkedPropertiesCaptor.capture(), eq(originalBody));
        BasicProperties parkedProperties = parkedPropertiesCaptor.getValue();
        assertThat(parkedProperties.getCorrelationId()).isEqualTo("caller-correlation-id");
        assertThat(parkedProperties.getHeaders())
                .containsEntry("tenant", "acme")
                .containsEntry("parked-reason", "handler-failure");
    }
}
