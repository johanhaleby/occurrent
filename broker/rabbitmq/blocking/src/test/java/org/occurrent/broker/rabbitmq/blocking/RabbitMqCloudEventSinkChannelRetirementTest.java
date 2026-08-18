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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A confirm wait that ends without a definite answer, by timeout or by interruption, abandons its publish on the
 * channel rather than resolving it, so these two recovery paths are exercised directly against a mocked
 * {@link Channel} and {@link Connection} instead of a real broker, which has no way to force either outcome on
 * demand.
 */
class RabbitMqCloudEventSinkChannelRetirementTest {

    private static final String EXCHANGE = "test-exchange";

    private final RabbitMqTopicExchangeDestinationResolver resolver =
            new RabbitMqTopicExchangeDestinationResolver(EXCHANGE, ReflectionCloudEventTypeMapper.qualified());

    @Test
    void a_confirm_wait_timeout_retires_the_channel_and_publishes_on_a_confirm_mode_replacement() throws Exception {
        Connection connection = mock(Connection.class);
        Channel timedOutChannel = mock(Channel.class);
        Channel replacementChannel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(timedOutChannel), Optional.of(replacementChannel));
        when(timedOutChannel.waitForConfirms(anyLong())).thenThrow(new TimeoutException("no confirm in time"));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        assertThatThrownBy(() -> sink.publish(orderPlaced())).isInstanceOf(RabbitMqPublishTimeoutException.class);

        verify(timedOutChannel).close();
        verify(replacementChannel).confirmSelect();
        verify(replacementChannel).addReturnListener(any(ReturnCallback.class));
        verify(replacementChannel, never()).close();
    }

    @Test
    void an_interrupted_confirm_wait_retires_the_channel_and_restores_the_interrupt_status() throws Exception {
        Connection connection = mock(Connection.class);
        Channel interruptedChannel = mock(Channel.class);
        Channel replacementChannel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(interruptedChannel), Optional.of(replacementChannel));
        when(interruptedChannel.waitForConfirms(anyLong())).thenThrow(new InterruptedException("interrupted"));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        try {
            assertThatThrownBy(() -> sink.publish(orderPlaced()))
                    .isInstanceOf(RabbitMqPublishException.class)
                    .isNotInstanceOf(RabbitMqPublishTimeoutException.class);
            assertThat(Thread.interrupted()).as("interrupt status should be restored on the calling thread").isTrue();
        } finally {
            Thread.interrupted();
        }

        verify(interruptedChannel).close();
        verify(replacementChannel).confirmSelect();
    }

    @Test
    void a_replacement_channel_failure_is_suppressed_on_the_timeout_it_actually_happened_on() throws Exception {
        Connection connection = mock(Connection.class);
        Channel timedOutChannel = mock(Channel.class);
        when(connection.openChannel())
                .thenReturn(Optional.of(timedOutChannel))
                .thenThrow(new ShutdownSignalException(true, true, null, null));
        when(timedOutChannel.waitForConfirms(anyLong())).thenThrow(new TimeoutException("no confirm in time"));

        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection, resolver).build();

        assertThatThrownBy(() -> sink.publish(orderPlaced()))
                .isInstanceOf(RabbitMqPublishTimeoutException.class)
                .satisfies(exception -> assertThat(exception.getSuppressed())
                        .as("the failed replacement should not replace the timeout the caller actually asked about")
                        .hasSize(1)
                        .allMatch(RabbitMqPublishException.class::isInstance));
    }

    private static CloudEvent orderPlaced() {
        return CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .build();
    }

    private static final class OrderPlaced {
    }
}
