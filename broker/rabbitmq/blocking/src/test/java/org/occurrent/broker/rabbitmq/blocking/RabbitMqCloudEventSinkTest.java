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
import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RabbitMqCloudEventSinkTest extends RabbitMqTestSupport {

    private final ch.qos.logback.classic.Logger sinkLog = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqCloudEventSink.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();

    @BeforeEach
    void captureLog() {
        logged.start();
        sinkLog.addAppender(logged);
        sinkLog.setLevel(Level.WARN);
    }

    @AfterEach
    void releaseLog() {
        sinkLog.detachAppender(logged);
        sinkLog.setLevel(null);
    }

    /**
     * The invariant that matters: {@code publish} only returns once the broker has both confirmed the message and
     * routed it, so by the time it returns the message is already visible to a plain {@code basicGet} on a queue
     * bound to it, with no polling needed.
     */
    @Test
    void publish_waits_for_the_brokers_confirmation_and_the_message_is_already_on_the_bound_queue_when_it_returns() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType(OrderPlaced.class.getName())
                    .withDataContentType("application/json")
                    .withData("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                    .withExtension("streamid", "stream-1")
                    .build();

            sink.publish(cloudEvent);

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("message should already be on the queue once publish() returns").isNotNull();
            assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":42}");
            assertThat(response.getProps().getContentType()).isEqualTo("application/json");
            assertThat(response.getProps().getHeaders().get("cloudEvents_streamid")).hasToString("stream-1");
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
            assertThat(response.getProps().getHeaders().get("cloudEvents_type")).hasToString(OrderPlaced.class.getName());
        }
    }

    /**
     * The other half of the same invariant. A confirm alone is not proof of delivery, so a publish the broker could
     * not route (nothing bound to the routing key) has to fail even though RabbitMQ confirms it before discarding
     * it. Uses the builder's default {@link RetryStrategy}, which excludes
     * {@link RabbitMqUnroutableEventException} from its retries, since it is a configuration bug rather than a
     * transient failure, so this also proves the publish fails promptly instead of retrying into the same
     * permanent failure forever.
     */
    @Test
    void publish_throws_when_the_broker_returns_the_message_as_unroutable() throws Exception {
        // No queue is bound to this routing key on this exchange.
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-2")
                    .withSource(URI.create("urn:test"))
                    .withType(NobodyListens.class.getName())
                    .build();

            assertThatThrownBy(() -> sink.publish(cloudEvent)).isInstanceOf(RabbitMqUnroutableEventException.class);
        }
    }

    @Test
    void publish_returns_normally_for_an_unroutable_event_under_LOG_AND_CONTINUE_and_the_next_routable_event_reaches_its_queue() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).onUnroutable(RabbitMqUnroutablePolicy.LOG_AND_CONTINUE).build()) {
            CloudEvent unroutable = CloudEventBuilder.v1().withId("id-unroutable").withSource(URI.create("urn:test")).withType(NobodyListens.class.getName()).build();
            CloudEvent routable = CloudEventBuilder.v1().withId("id-routable").withSource(URI.create("urn:test")).withType(OrderPlaced.class.getName()).build();

            sink.publish(unroutable);
            sink.publish(routable);

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("the event after the unroutable one should be on the queue once publish() returns").isNotNull();
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-routable");
        }
    }

    /**
     * Counts attempts through the resolver, which {@code publish} calls once per attempt. A strategy that retries
     * every exception and has three attempts would call it three times if the unroutable return still reached the
     * retry, so exactly one call proves the policy accepts the drop inside the attempt.
     */
    @Test
    void publish_makes_exactly_one_attempt_for_an_unroutable_event_under_LOG_AND_CONTINUE_even_when_the_retry_strategy_retries_every_exception() throws Exception {
        RabbitMqTopicExchangeDestinationResolver resolver = spy(new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified()));
        RetryStrategy retryEverything = RetryStrategy.fixed(10).maxAttempts(3);
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver)
                .retryStrategy(retryEverything).onUnroutable(RabbitMqUnroutablePolicy.LOG_AND_CONTINUE).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1().withId("id-3").withSource(URI.create("urn:test")).withType(NobodyListens.class.getName()).build();

            sink.publish(cloudEvent);

            verify(resolver, times(1)).destinationFor(any());
        }
    }

    /**
     * The routing key is deliberately different from the event type, so a warning that dropped either one would
     * fail this test instead of being covered by the other.
     */
    @Test
    void publish_logs_a_warning_naming_the_event_id_the_event_type_the_exchange_and_the_routing_key_for_an_unroutable_event_under_LOG_AND_CONTINUE() throws Exception {
        RabbitMqTopicExchangeDestinationResolver resolver = spy(new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified()));
        doReturn(RabbitMqDestination.of(exchange, "routing.key.nobody.binds")).when(resolver).destinationFor(any());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).onUnroutable(RabbitMqUnroutablePolicy.LOG_AND_CONTINUE).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1().withId("id-4").withSource(URI.create("urn:test")).withType(NobodyListens.class.getName()).build();

            sink.publish(cloudEvent);

            assertThat(logged.list).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains(
                        "id \"id-4\"",
                        "type \"" + NobodyListens.class.getName() + "\"",
                        "exchange \"" + exchange + "\"",
                        "routing key \"routing.key.nobody.binds\"");
            });
        }
    }

    /**
     * Only a return is accepted under {@link RabbitMqUnroutablePolicy#LOG_AND_CONTINUE}. A publish on a closed sink
     * is a failure that is not a return, so it has to keep throwing, and nothing is logged as if it were dropped.
     */
    @Test
    void publish_still_throws_a_RabbitMqPublishException_that_is_not_an_unroutable_return_under_LOG_AND_CONTINUE_when_the_failure_is_not_a_return() throws Exception {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).onUnroutable(RabbitMqUnroutablePolicy.LOG_AND_CONTINUE).build();
        sink.close();
        CloudEvent cloudEvent = CloudEventBuilder.v1().withId("id-6").withSource(URI.create("urn:test")).withType(OrderPlaced.class.getName()).build();

        assertThatThrownBy(() -> sink.publish(cloudEvent))
                .isInstanceOf(RabbitMqPublishException.class)
                .isNotInstanceOf(RabbitMqUnroutableEventException.class);
        assertThat(logged.list).isEmpty();
    }

    @Test
    void publish_throws_when_the_broker_returns_the_message_as_unroutable_under_an_explicit_FAIL_policy() throws Exception {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).onUnroutable(RabbitMqUnroutablePolicy.FAIL).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1().withId("id-5").withSource(URI.create("urn:test")).withType(NobodyListens.class.getName()).build();

            assertThatThrownBy(() -> sink.publish(cloudEvent)).isInstanceOf(RabbitMqUnroutableEventException.class);
        }
    }

    @Test
    void onUnroutable_refuses_null() {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RabbitMqCloudEventSink.Builder builder = RabbitMqCloudEventSink.builder(connection(), resolver);

        assertThatThrownBy(() -> builder.onUnroutable(null)).isInstanceOf(NullPointerException.class);
    }

    /**
     * A duration that truncates to zero milliseconds is read by the RabbitMQ client as "wait indefinitely" rather
     * than "don't wait", which would silently defeat the bounded wait this sink documents.
     */
    @Test
    void acknowledgementTimeout_refuses_a_duration_that_truncates_to_zero_milliseconds() {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RabbitMqCloudEventSink.Builder builder = RabbitMqCloudEventSink.builder(connection(), resolver);

        assertThatThrownBy(() -> builder.acknowledgementTimeout(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.acknowledgementTimeout(Duration.ofNanos(500))).isInstanceOf(IllegalArgumentException.class);
    }

    private static final class OrderPlaced {
    }

    private static final class NobodyListens {
    }
}
