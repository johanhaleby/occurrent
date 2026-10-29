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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * A source on a {@code Recoverable} channel logs the stop that follows a close of that channel. It warns when the
 * application closed the channel or its connection, and logs an error when the broker closed the channel, since
 * nothing reopens it and the application did not ask for the stop.
 */
class RabbitMqCloudEventSourceChannelCloseLogTest {

    private static final String QUEUE = "queue-of-the-channel-close-log-test";

    private final ch.qos.logback.classic.Logger sourceLog = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqCloudEventSource.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Channel channel = mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
    private final RabbitMqCloudEventSource source = sourceOn(channel);

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
    @MethodSource("closesByTheApplication")
    void a_close_by_the_application_stops_the_source_with_a_warning_that_names_the_queue(ShutdownSignalException close) throws Exception {
        channelShutDown(close);

        poll();

        assertThat(stopLines()).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("\"" + QUEUE + "\"", "was closed by the application");
        });
    }

    @Test
    void a_close_by_the_broker_stops_the_source_with_an_error_that_names_the_queue() throws Exception {
        channelShutDown(new ShutdownSignalException(false, false, null, channel));

        poll();

        assertThat(stopLines()).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.ERROR);
            assertThat(event.getFormattedMessage()).contains("\"" + QUEUE + "\"", "is not going to reopen it");
        });
    }

    private static Stream<Arguments> closesByTheApplication() {
        return Stream.of(
                argumentSet("a connection close by the application", new ShutdownSignalException(true, true, null, mock(Connection.class))),
                argumentSet("a channel close by the application", new ShutdownSignalException(false, true, null, mock(Channel.class)))
        );
    }

    private List<ILoggingEvent> stopLines() {
        return logged.list.stream()
                .filter(event -> event.getFormattedMessage().contains("Stopping this source"))
                .toList();
    }

    private static RabbitMqCloudEventSource sourceOn(Channel channel) {
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> { });
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceChannelCloseLogTest.class));
        return new RabbitMqCloudEventSource(model, channel, QUEUE, 1, Duration.ofMinutes(1), failureAction, subscriptionId -> true, Duration.ofSeconds(1));
    }

    // Calls every shutdown listener the source added to the channel, the way the client does when the channel closes
    private void channelShutDown(ShutdownSignalException cause) {
        ArgumentCaptor<ShutdownListener> listeners = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel, atLeastOnce()).addShutdownListener(listeners.capture());
        listeners.getAllValues().forEach(listener -> listener.shutdownCompleted(cause));
    }

    private void poll() throws ReflectiveOperationException {
        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }
}
