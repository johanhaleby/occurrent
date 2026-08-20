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

package org.occurrent.example.broker.rabbitmq;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * A Mongo event store plus a RabbitMQ broker, both real, both Testcontainers, shared by the CloudEvent-level and
 * domain-level example tests. Each test method gets its own scratch database (Mongo) and its own scratch exchange
 * and queue names (RabbitMQ), so tests running against the same reused containers never see each other's events.
 * The one connection to each opened here is deliberately kept open for the whole test method, including across a
 * simulated restart. That restart is consumer-side only. It tears down and rebuilds the bridge and the push model,
 * never the forwarder's own subscription, the database connection or the broker connection themselves.
 */
@Testcontainers
abstract class AbstractBrokerExampleTest {

    protected static final String EVENTS_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion()).withReuse(true);

    protected MongoClient mongoClient;
    protected String databaseName;
    protected Connection rabbitConnection;
    /** For declaring the exchange up front and, in a test, reading a queue's message count. Not used to publish or consume. */
    protected Channel adminChannel;
    protected String exchange;
    protected String queue;

    @BeforeEach
    void openMongoAndRabbit() throws Exception {
        String scratch = UUID.randomUUID().toString().replace("-", "");
        // getReplicaSetUrl(databaseName), not the no-arg overload plus "." + scratch. A dot after the URL names a
        // collection inside the container's one shared database, not a new database, so every test would read and
        // write the same database and see each other's events.
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(scratch));
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        rabbitConnection = connectionFactory.newConnection();
        adminChannel = rabbitConnection.createChannel();

        exchange = "broker-example-" + scratch;
        queue = "broker-example-" + scratch;
        // Neither RabbitMqCloudEventSink nor either bridge declares the exchange itself, only queues and bindings
        // against one that already exists, so the application declares it, the same as Bootstrap.java does for the
        // number-guessing-game example's own basicPublish.
        adminChannel.exchangeDeclare(exchange, "topic", true);
    }

    // adminChannel is a channel on rabbitConnection, so separate @AfterEach methods for the two would work only by
    // accident of whichever order JUnit happens to run them in. Closing the connection first leaves the channel
    // already closed, and closing it again then throws AlreadyClosedException instead of the resource actually
    // leaking. One method, each close in its own try, is what keeps "every resource gets a close attempt" true
    // without depending on an order this class does not control, the same shape the bootstraps' own close()
    // methods use for production code.
    @AfterEach
    void closeMongoAndRabbit() {
        // Null-guarded, since a failure partway through openMongoAndRabbit leaves whichever field it had not
        // reached yet still null. Closing unconditionally would add a NullPointerException here on top of that
        // real failure instead of just leaving this method with nothing to do for the field that never got set.
        RuntimeException failure = null;
        if (adminChannel != null) {
            try {
                adminChannel.close();
            } catch (Exception e) {
                failure = collectFailure(failure, e);
            }
        }
        if (rabbitConnection != null) {
            try {
                rabbitConnection.close();
            } catch (Exception e) {
                failure = collectFailure(failure, e);
            }
        }
        if (mongoClient != null) {
            try {
                mongoClient.close();
            } catch (RuntimeException e) {
                failure = collectFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException collectFailure(@Nullable RuntimeException first, Exception e) {
        RuntimeException wrapped = e instanceof RuntimeException re ? re : new RuntimeException("Failed to close test infrastructure", e);
        if (first == null) {
            return wrapped;
        }
        first.addSuppressed(wrapped);
        return first;
    }

    protected MongoEventStore newEventStore() {
        return new MongoEventStore(mongoClient, databaseName, EVENTS_COLLECTION, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
    }

    /**
     * {@code eventId} is the id every {@code OrderEvent} already carries, so the CloudEvent this converter writes
     * and the domain event a {@code DomainEventFeed} de-dups by agree on the same identity.
     */
    protected CloudEventConverter<OrderEvent> newConverter() {
        return new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:example:broker"))
                .typeMapper(newTypeMapper())
                .idMapper(OrderEvent::eventId)
                .build();
    }

    protected CloudEventTypeMapper<OrderEvent> newTypeMapper() {
        return ReflectionCloudEventTypeMapper.simple(OrderEvent.class);
    }

    /**
     * A single topic exchange, shared by every publisher and every bridge in one test, with the routing key derived
     * from {@code OrderEvent}'s cloud event type. Give it the same {@link CloudEventTypeMapper} instance
     * {@link #newConverter()} used, so a publisher and a consumer agree on where an event goes by reading one
     * mapping.
     */
    protected RabbitMqTopicExchangeDestinationResolver newResolver(CloudEventTypeMapper<OrderEvent> typeMapper) {
        return new RabbitMqTopicExchangeDestinationResolver(exchange, typeMapper);
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
