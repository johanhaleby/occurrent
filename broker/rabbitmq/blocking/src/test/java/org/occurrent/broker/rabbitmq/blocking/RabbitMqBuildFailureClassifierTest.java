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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link RabbitMqBuildFailureClassifier#isTransient(Throwable)}, the classification
 * {@code build()}'s default retry shares across both bridges and the Spring Boot starter. Constructs each
 * {@link ShutdownSignalException} directly against a mocked {@link AMQP.Connection.Close}/{@link AMQP.Channel.Close}
 * rather than against a real broker, which has no way to force a chosen reply code on demand.
 */
class RabbitMqBuildFailureClassifierTest {

    @Test
    void a_plain_ioexception_wrapped_in_a_bridge_exception_is_transient() {
        RabbitMqBridgeException exception = new RabbitMqBridgeException("failed", new IOException("connection reset"));

        assertThat(RabbitMqBuildFailureClassifier.isTransient(exception)).isTrue();
    }

    @Test
    void a_shutdown_signal_exception_with_no_amqp_reply_is_transient() {
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, null, "connection");

        assertThat(RabbitMqBuildFailureClassifier.isTransient(shutdown)).isTrue();
    }

    @Test
    void a_connection_forced_close_is_transient() {
        AMQP.Connection.Close close = mock(AMQP.Connection.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.CONNECTION_FORCED);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "connection");

        assertThat(RabbitMqBuildFailureClassifier.isTransient(shutdown)).isTrue();
    }

    @Test
    void a_not_found_channel_close_wrapped_in_a_bridge_exception_is_not_transient() {
        AMQP.Channel.Close close = mock(AMQP.Channel.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.NOT_FOUND);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "channel");
        RabbitMqBridgeException exception = new RabbitMqBridgeException("failed to declare topology", shutdown);

        assertThat(RabbitMqBuildFailureClassifier.isTransient(exception)).isFalse();
    }

    @Test
    void a_precondition_failed_channel_close_is_not_transient() {
        AMQP.Channel.Close close = mock(AMQP.Channel.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.PRECONDITION_FAILED);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "channel");

        assertThat(RabbitMqBuildFailureClassifier.isTransient(shutdown)).isFalse();
    }

    @Test
    void an_access_refused_connection_close_is_not_transient() {
        AMQP.Connection.Close close = mock(AMQP.Connection.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.ACCESS_REFUSED);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "connection");

        assertThat(RabbitMqBuildFailureClassifier.isTransient(shutdown)).isFalse();
    }

    @Test
    void an_invalid_path_connection_close_is_not_transient() {
        AMQP.Connection.Close close = mock(AMQP.Connection.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.INVALID_PATH);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "connection");

        assertThat(RabbitMqBuildFailureClassifier.isTransient(shutdown)).isFalse();
    }

    @Test
    void a_not_allowed_channel_close_is_not_transient() {
        AMQP.Channel.Close close = mock(AMQP.Channel.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.NOT_ALLOWED);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "channel");

        assertThat(RabbitMqBuildFailureClassifier.isTransient(shutdown)).isFalse();
    }

    @Test
    void a_publish_exception_wrapping_a_plain_ioexception_is_transient() {
        RabbitMqPublishException exception = new RabbitMqPublishException(
                "Failed to create a confirm-mode RabbitMQ channel", new IOException("connection refused"));

        assertThat(RabbitMqBuildFailureClassifier.isTransient(exception)).isTrue();
    }

    @Test
    void a_publish_exception_wrapping_a_permanent_amqp_close_is_not_transient() {
        AMQP.Channel.Close close = mock(AMQP.Channel.Close.class);
        when(close.getReplyCode()).thenReturn(AMQP.NOT_FOUND);
        ShutdownSignalException shutdown = new ShutdownSignalException(false, true, close, "channel");
        RabbitMqPublishException exception = new RabbitMqPublishException(
                "Failed to create a confirm-mode RabbitMQ channel", shutdown);

        assertThat(RabbitMqBuildFailureClassifier.isTransient(exception)).isFalse();
    }

    @Test
    void a_bug_shaped_runtime_exception_is_never_transient() {
        assertThat(RabbitMqBuildFailureClassifier.isTransient(new IllegalStateException("misconfigured"))).isFalse();
        assertThat(RabbitMqBuildFailureClassifier.isTransient(new NullPointerException())).isFalse();
    }
}
