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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A source on a channel that isn't {@code Recoverable} warns when it's built, and a close of that channel doesn't stop
 * it.
 */
class RabbitMqCloudEventSourceNonRecoverableChannelTest {

    private static final String QUEUE = "queue-of-the-non-recoverable-channel-test";

    private final ch.qos.logback.classic.Logger sourceLog = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqCloudEventSource.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();

    @BeforeEach
    void captureLog() {
        logged.start();
        sourceLog.addAppender(logged);
        sourceLog.setLevel(Level.WARN);
    }

    @AfterEach
    void releaseLog() {
        sourceLog.detachAppender(logged);
        sourceLog.setLevel(null);
    }

    @ParameterizedTest
    @MethodSource("closesThatStopARecoverableChannel")
    void a_close_of_a_channel_that_is_not_recoverable_does_not_stop_an_idle_source_on_the_next_poll(ShutdownSignalException cause) throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqCloudEventSource source = sourceOn(channel);

        channelShutDown(channel, cause);
        poll(source);

        verify(channel, never()).close();
        verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void building_a_source_on_a_channel_that_is_not_recoverable_warns_with_the_queue() throws Exception {
        RabbitMqCloudEventSource source = built(mock(Channel.class));
        try {
            assertThat(nonRecoverableWarnings()).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("\"" + QUEUE + "\"");
            });
        } finally {
            source.close();
        }
    }

    @Test
    void building_a_source_on_a_recoverable_channel_does_not_warn_about_it() throws Exception {
        RabbitMqCloudEventSource source = built(mock(Channel.class, withSettings().extraInterfaces(Recoverable.class)));
        try {
            assertThat(nonRecoverableWarnings()).isEmpty();
        } finally {
            source.close();
        }
    }

    private static Stream<ShutdownSignalException> closesThatStopARecoverableChannel() {
        return Stream.of(
                new ShutdownSignalException(false, false, null, mock(Channel.class)),
                new ShutdownSignalException(true, true, null, mock(Connection.class))
        );
    }

    private List<ILoggingEvent> nonRecoverableWarnings() {
        return logged.list.stream()
                .filter(event -> event.getFormattedMessage().contains("isn't Recoverable"))
                .toList();
    }

    private static RabbitMqCloudEventSource built(Channel channel) throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn("consumer-tag-1");
        return RabbitMqCloudEventSource.builder(connection, model(), QUEUE)
                .declareTopology(false)
                .pollInterval(Duration.ofMinutes(1))
                .build();
    }

    private static RabbitMqCloudEventSource sourceOn(Channel channel) {
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceNonRecoverableChannelTest.class));
        return new RabbitMqCloudEventSource(model(), channel, QUEUE, 1, Duration.ofMinutes(1), failureAction, subscriptionId -> true, Duration.ofSeconds(1));
    }

    private static PushSubscriptionModel model() {
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> { });
        return model;
    }

    // Calls every shutdown listener the source added to the channel, the way the client does when the channel closes
    private static void channelShutDown(Channel channel, ShutdownSignalException cause) {
        ArgumentCaptor<ShutdownListener> listeners = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel, atLeastOnce()).addShutdownListener(listeners.capture());
        listeners.getAllValues().forEach(listener -> listener.shutdownCompleted(cause));
    }

    private static void poll(RabbitMqCloudEventSource source) throws ReflectiveOperationException {
        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }
}
