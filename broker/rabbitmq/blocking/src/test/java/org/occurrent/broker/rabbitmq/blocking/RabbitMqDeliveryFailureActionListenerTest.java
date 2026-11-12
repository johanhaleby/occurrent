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
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * What {@link RabbitMqDeliveryFailureAction} tells its listener, against a mocked {@link Channel} and a mocked
 * {@link RabbitMqConfirmPublisher}. The park that falls back to a redelivery is a failure a real broker has no way to
 * force on demand.
 */
class RabbitMqDeliveryFailureActionListenerTest {

    private static final RabbitMqDestination PARKING_DESTINATION = RabbitMqDestination.of("exchange", "routingKey");
    private static final String QUEUE = "queue";

    private final Channel channel = mock(Channel.class);
    private final RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
    private final RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();

    @Test
    void a_park_whose_publish_fails_tells_the_listener_redelivered_and_not_parked() throws Exception {
        doThrow(new RabbitMqPublishException("publish failed")).when(parkingPublisher).publish("exchange", "routingKey", new BasicProperties(), new byte[0]);

        parking().apply(42L, new BasicProperties(), new byte[0]);

        assertThat(listener.calls()).containsExactly(new RecordingRabbitMqSourceListener.Call("redelivered", QUEUE, null));
    }

    @Test
    void a_park_that_succeeds_tells_the_listener_parked_only() {
        parking().apply(42L, new BasicProperties(), new byte[0]);

        assertThat(listener.calls()).containsExactly(new RecordingRabbitMqSourceListener.Call("parked", QUEUE, null));
    }

    @Test
    void a_park_whose_acknowledgement_fails_tells_the_listener_neither_parked_nor_redelivered() throws Exception {
        doThrow(new IOException("ack failed")).when(channel).basicAck(42L, false);

        assertThatThrownBy(() -> parking().apply(42L, new BasicProperties(), new byte[0])).isInstanceOf(RabbitMqSourceException.class);

        assertThat(listener.calls()).isEmpty();
    }

    @Test
    void redeliverFailure_tells_the_listener_redelivered_only_after_basicNack_returned() {
        redelivering().redeliverFailure(42L);

        assertThat(listener.calls()).containsExactly(new RecordingRabbitMqSourceListener.Call("redelivered", QUEUE, null));
    }

    @Test
    void redeliverFailure_tells_the_listener_nothing_when_basicNack_throws() throws Exception {
        doThrow(new IOException("nack failed")).when(channel).basicNack(42L, false, true);

        assertThatThrownBy(() -> redelivering().redeliverFailure(42L)).isInstanceOf(RabbitMqSourceException.class);

        assertThat(listener.calls()).isEmpty();
    }

    @Test
    void redeliver_for_a_held_outcome_tells_the_listener_nothing_since_it_is_not_a_failure() {
        redelivering().redeliver(42L);

        assertThat(listener.calls()).isEmpty();
    }

    private RabbitMqDeliveryFailureAction parking() {
        return new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.PARK, parkingPublisher, PARKING_DESTINATION,
                LoggerFactory.getLogger(getClass()), calls());
    }

    private RabbitMqDeliveryFailureAction redelivering() {
        return new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null, LoggerFactory.getLogger(getClass()), calls());
    }

    private RabbitMqSourceListenerCalls calls() {
        return new RabbitMqSourceListenerCalls(listener, QUEUE, LoggerFactory.getLogger(getClass()));
    }
}
