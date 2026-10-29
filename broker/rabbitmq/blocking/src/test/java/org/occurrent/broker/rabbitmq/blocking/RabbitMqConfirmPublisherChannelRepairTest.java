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

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A publish never uses a channel it has observed closed, and a reopen that failed on one publish is repaired by the
 * next one that can open a channel. {@code RabbitMqCloudEventSinkChannelRetirementTest} covers the retirement itself
 * and a reopen that succeeds. This covers a reopen that fails, which leaves the closed channel in place, over every
 * way a publish can end in a retirement and every way the reopen can fail. A {@link Channel} mock here refuses to
 * publish once closed, the way a real one does, so a publish that reaches a closed channel fails visibly instead of
 * passing by accident.
 */
class RabbitMqConfirmPublisherChannelRepairTest {

    private static final String EXCHANGE = "exchange";
    private static final String ROUTING_KEY = "routingKey";
    private static final byte[] BODY = "body".getBytes(StandardCharsets.UTF_8);
    private static final BasicProperties PROPERTIES = new BasicProperties.Builder().contentType("text/plain").build();

    /**
     * The ways a publish can end in the channel being retired. A shutdown leaves the channel closed already, a
     * timeout or an interruption leaves it open until the retirement closes it.
     */
    enum PublishFailure {
        CONFIRM_TIMEOUT(RabbitMqPublishTimeoutException.class, false, () -> new TimeoutException("no confirm in time")),
        CHANNEL_SHUTDOWN(RabbitMqPublishException.class, true, () -> new ShutdownSignalException(false, false, null, null)),
        CONNECTION_SHUTDOWN(RabbitMqPublishException.class, true, () -> new ShutdownSignalException(true, false, null, null)),
        INTERRUPTED_CONFIRM_WAIT(RabbitMqPublishException.class, false, () -> new InterruptedException("interrupted"));

        final Class<? extends RabbitMqPublishException> expected;
        final boolean leavesChannelClosed;
        final Supplier<Throwable> thrownByConfirmWait;

        PublishFailure(Class<? extends RabbitMqPublishException> expected, boolean leavesChannelClosed, Supplier<Throwable> thrownByConfirmWait) {
            this.expected = expected;
            this.leavesChannelClosed = leavesChannelClosed;
            this.thrownByConfirmWait = thrownByConfirmWait;
        }
    }

    /**
     * The ways opening the replacement channel can fail while the connection cannot hand one out.
     */
    enum ReopenFailure {
        OPEN_CHANNEL_THROWS_SHUTDOWN(invocation -> {
            throw new ShutdownSignalException(true, false, null, null);
        }),
        NO_CHANNEL_NUMBER_AVAILABLE(invocation -> Optional.empty()),
        CONFIRM_SELECT_FAILS(invocation -> {
            Channel unusable = mock(Channel.class);
            doThrow(new IOException("confirmSelect failed")).when(unusable).confirmSelect();
            return Optional.of(unusable);
        });

        final Answer<Optional<Channel>> answer;

        ReopenFailure(Answer<Optional<Channel>> answer) {
            this.answer = answer;
        }
    }

    static Stream<Arguments> everyPublishFailureWithEveryReopenFailure() {
        return Stream.of(PublishFailure.values())
                .flatMap(publishFailure -> Stream.of(ReopenFailure.values()).map(reopenFailure -> Arguments.of(publishFailure, reopenFailure)));
    }

    @ParameterizedTest(name = "{0} then reopen {1}")
    @MethodSource("everyPublishFailureWithEveryReopenFailure")
    void a_publish_after_a_failed_reopen_replaces_the_closed_channel_and_never_publishes_on_it(PublishFailure publishFailure, ReopenFailure reopenFailure) throws Exception {
        Connection connection = mock(Connection.class);
        FakeChannel retired = new FakeChannel();
        FakeChannel replacement = new FakeChannel();
        when(connection.openChannel())
                .thenReturn(Optional.of(retired.channel))
                .thenAnswer(reopenFailure.answer)
                .thenReturn(Optional.of(replacement.channel));
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5));
        retired.failConfirmWaitWith(publishFailure);

        assertThatThrownBy(() -> publishRestoringInterrupt(publisher))
                .isInstanceOf(publishFailure.expected)
                .satisfies(exception -> assertThat(exception.getSuppressed())
                        .as("the failed reopen is attached to the failure the caller asked about")
                        .hasSize(1)
                        .allMatch(RabbitMqPublishException.class::isInstance));
        retired.awaitClosed();
        assertThat(retired.channel.isOpen()).as("the retired channel is closed when the next publish starts").isFalse();

        assertThatCode(() -> publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY)).doesNotThrowAnyException();

        verify(replacement.channel).confirmSelect();
        verify(replacement.channel).basicPublish(eq(EXCHANGE), eq(ROUTING_KEY), eq(true), any(BasicProperties.class), eq(BODY));
        verify(retired.channel, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class), any(byte[].class));
        verify(connection, times(3)).openChannel();
    }

    @ParameterizedTest(name = "{0} then reopen {1}")
    @MethodSource("everyPublishFailureWithEveryReopenFailure")
    void a_publish_made_while_the_reopen_keeps_failing_fails_without_touching_the_closed_channel_and_a_later_one_recovers(PublishFailure publishFailure, ReopenFailure reopenFailure) throws Exception {
        Connection connection = mock(Connection.class);
        FakeChannel retired = new FakeChannel();
        FakeChannel replacement = new FakeChannel();
        when(connection.openChannel())
                .thenReturn(Optional.of(retired.channel))
                .thenAnswer(reopenFailure.answer)
                .thenAnswer(reopenFailure.answer)
                .thenAnswer(reopenFailure.answer)
                .thenReturn(Optional.of(replacement.channel));
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5));
        retired.failConfirmWaitWith(publishFailure);
        assertThatThrownBy(() -> publishRestoringInterrupt(publisher)).isInstanceOf(publishFailure.expected);
        retired.awaitClosed();

        for (int stillFailing = 1; stillFailing <= 2; stillFailing++) {
            assertThatThrownBy(() -> publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY))
                    .as("publish %d while the reopen still fails", stillFailing)
                    .isInstanceOf(RabbitMqPublishException.class)
                    .isNotInstanceOf(RabbitMqPublishTimeoutException.class);
            verify(retired.channel, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class), any(byte[].class));
        }

        assertThatCode(() -> publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY)).doesNotThrowAnyException();

        verify(replacement.channel).basicPublish(eq(EXCHANGE), eq(ROUTING_KEY), eq(true), any(BasicProperties.class), eq(BODY));
        verify(retired.channel, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class), any(byte[].class));
        verify(connection, times(5)).openChannel();
    }

    @Test
    void publishing_on_a_channel_that_stays_open_opens_exactly_one_channel_in_total() throws Exception {
        Connection connection = mock(Connection.class);
        FakeChannel only = new FakeChannel();
        when(connection.openChannel()).thenReturn(Optional.of(only.channel));
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5));

        publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY);
        publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY);
        publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY);

        verify(connection, times(1)).openChannel();
        verify(only.channel, times(3)).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class), any(byte[].class));
    }

    /**
     * A negative acknowledgement resolves its publish, so it leaves the channel open and in place. Retiring on it
     * would open a channel per nack, so the open-channel check must not widen into a retirement of its own.
     */
    @Test
    void a_negative_acknowledgement_does_not_retire_a_channel_that_is_still_open() throws Exception {
        Connection connection = mock(Connection.class);
        FakeChannel only = new FakeChannel();
        when(connection.openChannel()).thenReturn(Optional.of(only.channel));
        doAnswer(invocation -> false).doAnswer(invocation -> true).when(only.channel).waitForConfirms(anyLong());
        RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, Duration.ofSeconds(5));

        assertThatThrownBy(() -> publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY)).isInstanceOf(RabbitMqPublishException.class);
        assertThatCode(() -> publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY)).doesNotThrowAnyException();

        verify(connection, times(1)).openChannel();
        verify(only.channel, times(2)).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class), any(byte[].class));
    }

    // An interrupted confirm wait leaves the interrupt flag set on this thread, which would fail the latch await
    // that follows it.
    private static void publishRestoringInterrupt(RabbitMqConfirmPublisher publisher) {
        try {
            publisher.publish(EXCHANGE, ROUTING_KEY, PROPERTIES, BODY);
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * A {@link Channel} mock that is open until it is closed or a configured failure shuts it, and that throws
     * {@link AlreadyClosedException} from {@code basicPublish} once it is not, like a real one.
     */
    private static final class FakeChannel {
        final Channel channel = mock(Channel.class);
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final CountDownLatch closed = new CountDownLatch(1);

        FakeChannel() throws Exception {
            when(channel.isOpen()).thenAnswer(invocation -> open.get());
            doAnswer(invocation -> {
                if (!open.get()) {
                    throw new AlreadyClosedException(new ShutdownSignalException(false, false, null, null));
                }
                return null;
            }).when(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class), any(byte[].class));
            doAnswer(invocation -> true).when(channel).waitForConfirms(anyLong());
            // The retirement closes the channel on its own thread, so a test awaits this rather than assuming it.
            doAnswer(invocation -> {
                open.set(false);
                closed.countDown();
                return null;
            }).when(channel).close();
        }

        void failConfirmWaitWith(PublishFailure failure) throws Exception {
            doAnswer(invocation -> {
                if (failure.leavesChannelClosed) {
                    open.set(false);
                }
                throw failure.thrownByConfirmWait.get();
            }).when(channel).waitForConfirms(anyLong());
        }

        void awaitClosed() throws InterruptedException {
            assertThat(closed.await(5, TimeUnit.SECONDS)).as("the retired channel is closed").isTrue();
        }
    }
}
