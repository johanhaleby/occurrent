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
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * A failure that reaches the worker after this source has already stopped is not a second reason to stop it.
 * {@code close()} interrupts a handler that is still running, and a handler that surfaces that interrupt as an
 * {@code InterruptedException} would otherwise be logged at error as this source failing, and stop a source that is
 * already stopped. The failure is routed through the worker's own entry point against a mocked {@link Channel}.
 */
class RabbitMqCloudEventSourceStoppedSourceDeliveryFailureTest {

    private static final long DELIVERY_TAG = 7L;

    private final Channel channel = mock(Channel.class);
    private final AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
    private final ch.qos.logback.classic.Logger sourceLog = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqCloudEventSource.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private RabbitMqCloudEventSource source;

    @BeforeEach
    void buildSource() {
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> sneakyThrow(handlerFailure.get()));
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceStoppedSourceDeliveryFailureTest.class));
        source = new RabbitMqCloudEventSource(model, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null, Duration.ofSeconds(1));
        logged.start();
        sourceLog.addAppender(logged);
    }

    @AfterEach
    void detachLogAppender() {
        sourceLog.detachAppender(logged);
        Thread.interrupted();
    }

    @Test
    void a_handler_error_before_the_source_stopped_stops_it_and_logs_an_error() throws Exception {
        handlerFailure.set(new StackOverflowError("a handler that recursed"));

        handleDeliveryOrStop();

        verify(channel).close();
        assertThat(errorsLogged()).hasSize(1);
    }

    @Test
    void a_handler_error_after_the_source_stopped_does_not_stop_it_again_and_logs_no_error() throws Exception {
        handlerFailure.set(new StackOverflowError("a handler that recursed"));
        stopPermanently();
        verify(channel).close();

        handleDeliveryOrStop();

        verify(channel).close();
        assertThat(errorsLogged()).isEmpty();
    }

    @Test
    void an_interrupted_exception_from_a_handler_after_the_source_stopped_restores_the_interrupt_and_logs_no_error() throws Exception {
        handlerFailure.set(new InterruptedException("close() interrupted the handler"));
        stopPermanently();

        handleDeliveryOrStop();

        assertThat(Thread.currentThread().isInterrupted()).as("the interrupt reached the thread again").isTrue();
        verify(channel).close();
        assertThat(errorsLogged()).isEmpty();
    }

    @Test
    void a_handler_error_after_the_source_stopped_leaves_the_interrupt_flag_alone() throws Exception {
        handlerFailure.set(new StackOverflowError("a handler that recursed"));
        stopPermanently();

        handleDeliveryOrStop();

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    private void stopPermanently() {
        source.route(RoutingOutcome.REFUSED, DELIVERY_TAG + 1, new BasicProperties(), new byte[0]);
        // The refusal logs its own error, which is not what is asserted on below.
        logged.list.clear();
    }

    private void handleDeliveryOrStop() throws ReflectiveOperationException {
        Method method = RabbitMqCloudEventSource.class.getDeclaredMethod("handleDeliveryOrStop", Delivery.class, long.class);
        method.setAccessible(true);
        method.invoke(source, delivery(), DELIVERY_TAG);
    }

    private List<ILoggingEvent> errorsLogged() {
        return logged.list.stream().filter(event -> event.getLevel().isGreaterOrEqual(Level.ERROR)).toList();
    }

    private static Delivery delivery() {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType("t")
                .build();
        return new Delivery(new Envelope(DELIVERY_TAG, false, "exchange", "routingKey"),
                RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of()), RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }
}
