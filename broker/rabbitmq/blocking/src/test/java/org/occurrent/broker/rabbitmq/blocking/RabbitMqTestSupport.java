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
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

/**
 * A single-node RabbitMQ container, no fixed host port, plus a fresh connection and a scratch topic exchange for
 * each test method, torn down when the test ends. Shared by the sink tests in this package and in {@code .domain}
 * rather than duplicated in each of them, which is why this and its members are {@code public}/{@code protected}
 * rather than package-private.
 */
@Testcontainers
public abstract class RabbitMqTestSupport {

    @Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion()).withReuse(true);

    private Connection connection;

    protected Channel adminChannel;
    protected String exchange;

    @BeforeEach
    protected void openConnectionAndScratchExchange() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        connection = connectionFactory.newConnection();
        adminChannel = connection.createChannel();
        exchange = "test-exchange-" + UUID.randomUUID();
        // Not durable, auto-delete: this exchange only ever needs to outlive the one test method that declares it.
        adminChannel.exchangeDeclare(exchange, "topic", false, true, null);
    }

    @AfterEach
    protected void closeConnection() throws Exception {
        connection.close();
    }

    protected Connection connection() {
        return connection;
    }

    /**
     * The {@code test.rabbitmq.version} system property Surefire is configured to pass, the same way
     * {@code test.mongo.version} already works for the MongoDB containers. Falls back to a literal for an IDE run,
     * where nothing sets it.
     */
    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }
}
