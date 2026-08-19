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
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RabbitMqDeliveryFailureAction} against mocked RabbitMQ client types, for the two failures
 * a real broker has no way to force on demand: a partially built {@link #create} unwinding what it already opened,
 * and a raw parking publish failing with an unchecked {@link ShutdownSignalException} rather than a checked
 * {@link java.io.IOException}.
 */
class RabbitMqDeliveryFailureActionTest {

    @Test
    void create_closes_the_parking_sink_it_already_built_when_the_raw_parking_channel_fails_to_open() throws Exception {
        Connection connection = mock(Connection.class);
        Channel sinkChannel = mock(Channel.class);
        Channel consumeChannel = mock(Channel.class);
        // The parking sink's own channel opens first and succeeds, the raw parking channel is the one that fails.
        when(connection.openChannel()).thenReturn(Optional.of(sinkChannel)).thenReturn(Optional.empty());
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");

        assertThatThrownBy(() -> RabbitMqDeliveryFailureAction.create(connection, consumeChannel, DeliveryFailurePolicy.PARK,
                parkingDestination, LoggerFactory.getLogger(getClass())))
                .isInstanceOf(RabbitMqBridgeException.class);

        verify(sinkChannel).close();
    }

    /**
     * The regression case ADR 133's bridges caught for {@link java.io.IOException} but not for the unchecked
     * {@link ShutdownSignalException} the broker closing this channel over a missing parking exchange actually
     * raises. Proven directly against {@link RabbitMqDeliveryFailureAction#applyToUndecodable}, since forcing that
     * exact broker behaviour on demand needs a real, deliberately broken exchange, which the mock stands in for.
     */
    @Test
    void applyToUndecodable_redelivers_instead_of_propagating_when_the_raw_parking_publish_throws_a_shutdownSignalException() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        Channel rawParkChannel = mock(Channel.class);
        RabbitMqCloudEventSink parkingSink = mock(RabbitMqCloudEventSink.class);
        doThrow(new ShutdownSignalException(true, false, null, null))
                .when(rawParkChannel).basicPublish(any(), any(), anyBoolean(), any(), any());
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK, parkingSink,
                rawParkChannel, parkingDestination, LoggerFactory.getLogger(getClass()));

        action.applyToUndecodable(42L, new BasicProperties(), new byte[0]);

        verify(consumeChannel).basicNack(42L, false, true);
        verify(consumeChannel, never()).basicAck(anyLong(), anyBoolean());
    }
}
