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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link RabbitMqConfirmPublisher} is the one confirmed-publish implementation both {@link RabbitMqCloudEventSink}
 * and the consume-side parking path in {@link RabbitMqDeliveryFailureAction} share. Its confirm-wait timeout,
 * interruption and channel-retirement behaviour are already exercised through the sink's own public API in
 * {@code RabbitMqCloudEventSinkChannelRetirementTest}, unchanged by the extraction since this class is exactly what
 * that test already ran against. This class covers the two things that test does not: a failure opening the
 * channel after it was already created, and a plain, direct publish of already-built properties and a body, the
 * shape the parking path calls with.
 */
class RabbitMqConfirmPublisherTest {

    /**
     * {@code connection.openChannel()} can succeed and {@code channel.confirmSelect()} can still fail. Before this,
     * the channel that call already opened was never closed, since only the case where {@code openChannel()} itself
     * fails was handled.
     */
    @Test
    void a_confirmSelect_failure_closes_the_channel_that_was_already_opened() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        doThrow(new IOException("confirmSelect failed")).when(channel).confirmSelect();

        assertThatThrownBy(() -> new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5)))
                .isInstanceOf(RabbitMqPublishException.class);

        verify(channel).close();
    }

    @Test
    void publish_sends_the_given_properties_and_body_unchanged_to_the_given_exchange_and_routingKey() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.waitForConfirms(anyLong())).thenReturn(true);
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5));
        byte[] body = "raw body".getBytes(StandardCharsets.UTF_8);
        BasicProperties properties = new BasicProperties.Builder().contentType("text/plain").build();

        publisher.publish("exchange", "routingKey", properties, body);

        verify(channel).basicPublish(eq("exchange"), eq("routingKey"), eq(true), any(BasicProperties.class), eq(body));
    }
}
