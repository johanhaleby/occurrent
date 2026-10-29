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
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.ChannelN;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.withSettings;

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
     * one anyway would waste a channel nothing will ever use, and could fail startup for a resource the source
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
     * A {@code basicAck} failure on a confirmed park propagates rather than being swallowed, and the log line
     * claiming the original was acknowledged never runs, since {@code ack(...)} throws before reaching it.
     */
    @Test
    void apply_never_logs_the_parked_and_acknowledged_line_when_basicAck_itself_fails() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");
        doThrow(new IOException("ack failed")).when(consumeChannel).basicAck(42L, false);
        Logger log = mock(Logger.class);
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK,
                parkingPublisher, parkingDestination, log);

        assertThatThrownBy(() -> action.apply(42L, new BasicProperties(), new byte[0]))
                .isInstanceOf(RabbitMqSourceException.class);

        verifyNoInteractions(log);
    }

    /**
     * A confirmed park says what was done. A copy went to the parking destination and the acknowledgement for the
     * original was sent. It does not claim anything about what the broker did with that acknowledgement.
     */
    @Test
    void apply_logs_that_a_copy_was_parked_and_the_acknowledgement_sent_without_claiming_nothing_consumed_it() {
        // Given
        Channel consumeChannel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey");
        Logger log = LoggerFactory.getLogger("RabbitMqDeliveryFailureActionTest.park");
        ListAppender<ILoggingEvent> logged = captureLogOf(log);
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK,
                parkingPublisher, parkingDestination, log);

        // When
        action.apply(42L, new BasicProperties(), new byte[0]);

        // Then
        assertThat(logged.list).singleElement().satisfies(event -> assertAll(
                () -> assertThat(event.getLevel()).isEqualTo(Level.WARN),
                () -> assertThat(event.getFormattedMessage())
                        .contains("42", "\"exchange\"", "\"routingKey\"", "sent the acknowledgement for the original")
                        .doesNotContain("nothing consumed")));
    }

    /**
     * A failed delivery that is put back says that a negative acknowledgement with requeue was sent for it. It does
     * not claim the delivery was redelivered or that nothing consumed it, since the broker decides that.
     */
    @Test
    void redeliverFailure_logs_that_a_negative_acknowledgement_with_requeue_was_sent_without_claiming_a_redelivery() {
        // Given
        Channel consumeChannel = mock(Channel.class);
        Logger log = LoggerFactory.getLogger("RabbitMqDeliveryFailureActionTest.redeliver");
        ListAppender<ILoggingEvent> logged = captureLogOf(log);
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.REDELIVER,
                null, null, log);

        // When
        action.redeliverFailure(42L);

        // Then
        assertThat(logged.list).singleElement().satisfies(event -> assertAll(
                () -> assertThat(event.getLevel()).isEqualTo(Level.WARN),
                () -> assertThat(event.getFormattedMessage())
                        .contains("42", "negative acknowledgement with requeue")
                        .doesNotContain("Redelivered", "nothing consumed")));
    }

    /**
     * A {@code basicNack} failure on {@link RabbitMqDeliveryFailureAction#redeliverFailure(long)} propagates rather
     * than being swallowed, and the failure warning never runs, since {@code redeliver(...)} throws before reaching
     * it.
     */
    @Test
    void redeliverFailure_never_logs_the_failure_warning_when_basicNack_itself_fails() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        doThrow(new IOException("nack failed")).when(consumeChannel).basicNack(42L, false, true);
        Logger log = mock(Logger.class);
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.REDELIVER,
                null, null, log);

        assertThatThrownBy(() -> action.redeliverFailure(42L))
                .isInstanceOf(RabbitMqSourceException.class);

        verifyNoInteractions(log);
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

    /**
     * On a colliding key, {@code parkingDestination}'s own header value wins over the delivery's original one,
     * matching {@link RabbitMqDeliveryFailureAction}'s documented merge order. Disjoint keys alone cannot tell a
     * correct merge from one whose precedence is silently reversed.
     */
    @Test
    void apply_lets_the_parking_destinations_header_win_over_the_originals_on_a_colliding_key() throws Exception {
        Channel consumeChannel = mock(Channel.class);
        RabbitMqConfirmPublisher parkingPublisher = mock(RabbitMqConfirmPublisher.class);
        RabbitMqDestination parkingDestination = RabbitMqDestination.of("exchange", "routingKey")
                .withHeaders(Map.of("parked-reason", "destination-value"));
        BasicProperties originalProperties = new BasicProperties.Builder()
                .headers(Map.of("parked-reason", "original-value"))
                .build();
        byte[] originalBody = "payload".getBytes();
        RabbitMqDeliveryFailureAction action = new RabbitMqDeliveryFailureAction(consumeChannel, DeliveryFailurePolicy.PARK,
                parkingPublisher, parkingDestination, LoggerFactory.getLogger(getClass()));

        action.apply(42L, originalProperties, originalBody);

        ArgumentCaptor<BasicProperties> parkedPropertiesCaptor = ArgumentCaptor.forClass(BasicProperties.class);
        verify(parkingPublisher).publish(eq("exchange"), eq("routingKey"), parkedPropertiesCaptor.capture(), eq(originalBody));
        assertThat(parkedPropertiesCaptor.getValue().getHeaders())
                .containsEntry("parked-reason", "destination-value");
    }

    /**
     * Only a failure the client recovers the connection from lets a source wait for the delivery to arrive again
     * instead of stopping. For each other case here that wait would be for a channel that never comes back.
     */
    @Test
    void isLostToConnectionRecovery_is_true_only_for_a_connection_failure_the_client_recovers_from() {
        RabbitMqDeliveryFailureAction recovering = new RabbitMqDeliveryFailureAction(mock(Channel.class, withSettings().extraInterfaces(Recoverable.class)),
                DeliveryFailurePolicy.REDELIVER, null, null, LoggerFactory.getLogger(getClass()));
        RabbitMqDeliveryFailureAction notRecovering = new RabbitMqDeliveryFailureAction(mock(Channel.class),
                DeliveryFailurePolicy.REDELIVER, null, null, LoggerFactory.getLogger(getClass()));
        ShutdownSignalException connectionDropped = new ShutdownSignalException(true, false, null, null);

        assertThat(recovering.isLostToConnectionRecovery(new AlreadyClosedException(connectionDropped))).isTrue();
        assertThat(recovering.isLostToConnectionRecovery(connectionDropped)).isTrue();
        assertThat(recovering.isLostToConnectionRecovery(new RabbitMqSourceException("ack failed", new IOException("broken pipe")))).isTrue();

        assertThat(recovering.isLostToConnectionRecovery(new ShutdownSignalException(true, true, null, null))).as("connection closed by the application").isFalse();
        assertThat(recovering.isLostToConnectionRecovery(new AlreadyClosedException(new ShutdownSignalException(false, false, null, null)))).as("channel closed by the broker").isFalse();
        assertThat(recovering.isLostToConnectionRecovery(new RabbitMqSourceException("no cause"))).isFalse();
        assertThat(recovering.isLostToConnectionRecovery(new IllegalStateException("unrelated"))).isFalse();
        assertThat(recovering.isLostToConnectionRecovery(new OutOfMemoryError())).isFalse();
        assertThat(notRecovering.isLostToConnectionRecovery(new AlreadyClosedException(connectionDropped))).as("connection without automatic recovery").isFalse();
    }

    /**
     * A close the client recovers from on a {@code Recoverable} channel is reopened, so it ends nothing. On a wrapper
     * around the client's own channel a recovery underneath may reopen it without telling the source, so that close
     * isn't final there either. The client's own channel has no recovery underneath, so every close is final.
     */
    @ParameterizedTest
    @MethodSource("closesByChannelKind")
    void isClosedForGood_is_false_only_for_a_close_a_recovery_may_reopen_the_channel_after(Channel channel, ShutdownSignalException close, boolean closedForGood) {
        assertThat(actionOn(channel).isClosedForGood(close)).isEqualTo(closedForGood);
    }

    @Test
    void mayHideRecovery_is_true_only_for_a_channel_that_is_neither_recoverable_nor_the_clients_own() {
        assertThat(actionOn(mock(Channel.class, withSettings().extraInterfaces(Recoverable.class))).mayHideRecovery()).as("Recoverable channel").isFalse();
        assertThat(actionOn(mock(ChannelN.class)).mayHideRecovery()).as("the client's own channel").isFalse();
        assertThat(actionOn(mock(Channel.class)).mayHideRecovery()).as("wrapper channel").isTrue();
    }

    private static Stream<Arguments> closesByChannelKind() {
        ShutdownSignalException channelClosedByBroker = new ShutdownSignalException(false, false, null, mock(Channel.class));
        ShutdownSignalException channelClosedByApplication = new ShutdownSignalException(false, true, null, mock(Channel.class));
        ShutdownSignalException connectionDropped = new ShutdownSignalException(true, false, null, mock(Connection.class));
        ShutdownSignalException connectionClosedByApplication = new ShutdownSignalException(true, true, null, mock(Connection.class));
        return Stream.of(
                argumentSet("Recoverable, channel closed by the broker", recoverable(), channelClosedByBroker, true),
                argumentSet("Recoverable, channel closed by the application", recoverable(), channelClosedByApplication, true),
                argumentSet("Recoverable, connection closed by the application", recoverable(), connectionClosedByApplication, true),
                argumentSet("Recoverable, connection dropped", recoverable(), connectionDropped, false),
                argumentSet("client's own, channel closed by the broker", mock(ChannelN.class), channelClosedByBroker, true),
                argumentSet("client's own, channel closed by the application", mock(ChannelN.class), channelClosedByApplication, true),
                argumentSet("client's own, connection closed by the application", mock(ChannelN.class), connectionClosedByApplication, true),
                argumentSet("client's own, connection dropped", mock(ChannelN.class), connectionDropped, true),
                argumentSet("wrapper, channel closed by the broker", mock(Channel.class), channelClosedByBroker, true),
                argumentSet("wrapper, channel closed by the application", mock(Channel.class), channelClosedByApplication, true),
                argumentSet("wrapper, connection closed by the application", mock(Channel.class), connectionClosedByApplication, true),
                argumentSet("wrapper, connection dropped", mock(Channel.class), connectionDropped, false)
        );
    }

    private static Channel recoverable() {
        return mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
    }

    private RabbitMqDeliveryFailureAction actionOn(Channel channel) {
        return new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null, LoggerFactory.getLogger(getClass()));
    }

    private static ListAppender<ILoggingEvent> captureLogOf(Logger log) {
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        ((ch.qos.logback.classic.Logger) log).addAppender(logged);
        return logged;
    }
}
