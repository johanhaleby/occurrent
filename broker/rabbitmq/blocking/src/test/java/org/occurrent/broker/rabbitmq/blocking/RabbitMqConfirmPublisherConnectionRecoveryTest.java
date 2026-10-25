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
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * A {@link RabbitMqConfirmPublisher} on a connection with automatic recovery publishes again once that connection
 * has recovered, also when a publish was made during the outage. That publish fails and retires the channel, and the
 * reopen fails too since the connection is down. Without a repair on the next publish the publisher stays on the
 * channel it retired, which the client does not recover since it was closed on purpose, and every later publish
 * fails for the rest of the process even though the broker is back.
 * <p>
 * The connection is dropped from the broker side with {@code rabbitmqctl close_all_connections}. The network
 * recovery interval is long enough that the publish during the outage always lands before the client has
 * reconnected, and the test fails outright, rather than passing, when that publish unexpectedly succeeds or the
 * exec reports a non-zero exit code.
 */
@Testcontainers
class RabbitMqConfirmPublisherConnectionRecoveryTest {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion());

    @Test
    void a_publish_made_during_a_connection_outage_does_not_strand_the_publisher_on_a_closed_channel_after_the_connection_recovers() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setNetworkRecoveryInterval(5000);
        Connection recovering = connectionFactory.newConnection();
        try {
            try (Channel setup = recovering.createChannel()) {
                setup.queueDeclare(queue, false, false, false, null);
            }
            CountDownLatch shutDown = new CountDownLatch(1);
            CountDownLatch recovered = new CountDownLatch(1);
            recovering.addShutdownListener(cause -> shutDown.countDown());
            ((Recoverable) recovering).addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    recovered.countDown();
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                }
            });
            RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(recovering, Duration.ofSeconds(5));
            publisher.publish("", queue, new BasicProperties(), "before the outage".getBytes(StandardCharsets.UTF_8));

            forceCloseAllConnectionsOrFail();
            assertThat(shutDown.await(10, TimeUnit.SECONDS)).as("the connection should report the forced shutdown").isTrue();
            await().atMost(Duration.ofSeconds(5)).until(() -> !recovering.isOpen());

            assertThatThrownBy(() -> publisher.publish("", queue, new BasicProperties(), "during the outage".getBytes(StandardCharsets.UTF_8)))
                    .as("a publish during the outage fails, which is what retires the channel and fails its reopen")
                    .isInstanceOf(RabbitMqPublishException.class);

            assertThat(recovered.await(30, TimeUnit.SECONDS)).as("the client should reconnect by itself").isTrue();
            await().atMost(Duration.ofSeconds(5)).until(recovering::isOpen);

            assertThatCode(() -> publisher.publish("", queue, new BasicProperties(), "after the recovery".getBytes(StandardCharsets.UTF_8)))
                    .as("the first publish after the connection recovered")
                    .doesNotThrowAnyException();

            publisher.close();
            assertThat(drain(queue)).containsExactly("before the outage", "after the recovery");
        } finally {
            if (recovering.isOpen()) {
                recovering.close();
            }
        }
    }

    // Read on a plain connection of its own so the assertion does not lean on the recovered one.
    private static List<String> drain(String queue) throws Exception {
        ConnectionFactory plain = new ConnectionFactory();
        plain.setUri(rabbitMQContainer.getAmqpUrl());
        plain.setAutomaticRecoveryEnabled(false);
        try (Connection connection = plain.newConnection(); Channel channel = connection.createChannel()) {
            List<String> bodies = new ArrayList<>();
            GetResponse response;
            while ((response = channel.basicGet(queue, true)) != null) {
                bodies.add(new String(response.getBody(), StandardCharsets.UTF_8));
            }
            return bodies;
        }
    }

    /**
     * Fails the test outright, rather than skipping the outage it exists for, when the exec reports a non-zero exit
     * code.
     */
    private static void forceCloseAllConnectionsOrFail() throws Exception {
        Container.ExecResult closeResult = rabbitMQContainer.execInContainer(
                "rabbitmqctl", "close_all_connections", "forced-by-confirm-publisher-recovery-test");
        assertThat(closeResult.getExitCode())
                .as("rabbitmqctl close_all_connections must succeed for this test to force the outage it exists "
                        + "to exercise; stdout: %s, stderr: %s", closeResult.getStdout(), closeResult.getStderr())
                .isZero();
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }
}
