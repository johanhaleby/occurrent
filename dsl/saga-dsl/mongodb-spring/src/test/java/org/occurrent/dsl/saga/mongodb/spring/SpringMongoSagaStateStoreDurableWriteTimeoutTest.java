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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. A client {@code timeoutMS} of 0 means no timeout, so a write outside a transaction that waits for a
 * majority that is gone would wait forever and hold the subscription thread. The store puts a bound of at most 10
 * seconds back, which the driver sends as {@code maxTimeMS} on the command, and a command listener reads it off the
 * wire.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreDurableWriteTimeoutTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-durable-write-timeout";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    record Payment(long amount) {
    }

    // The command document of each insert sent to the instance collection
    private final List<BsonDocument> inserts = new CopyOnWriteArrayList<>();
    private MongoClient client;

    @AfterEach
    void close() {
        client.close();
    }

    private SpringMongoSagaStateStore<Payment> storeOnAClientWithATimeoutOf(long timeoutMillis) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-timeout-" + UUID.randomUUID().toString().substring(0, 8)));
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .timeout(timeoutMillis, MILLISECONDS)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        BsonDocument command = event.getCommand();
                        if (event.getCommandName().equals("insert") && COLLECTION.equals(command.getString("insert").getValue())) {
                            inserts.add(command);
                        }
                    }
                }).build();
        client = MongoClients.create(settings);
        return new SpringMongoSagaStateStore<>(new MongoTemplate(client, requireNonNull(connectionString.getDatabase())), COLLECTION, Payment.class);
    }

    @Test
    void a_client_timeout_of_zero_still_sends_the_insert_of_an_instance_with_a_max_time_of_at_most_ten_seconds() {
        SpringMongoSagaStateStore<Payment> store = storeOnAClientWithATimeoutOf(0);
        SagaEnvelope<Payment> envelope = new SagaEnvelope<>("order-1", new Payment(12L), SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);

        assertThat(SagaEnvelopes.of(store, null).compareAndSave("order-1", envelope, 0)).isTrue();

        assertThat(inserts).singleElement().satisfies(insert -> {
            assertThat(insert.containsKey("maxTimeMS")).as("the insert carries maxTimeMS").isTrue();
            assertThat(insert.get("maxTimeMS").asNumber().longValue()).as("maxTimeMS").isGreaterThan(0).isLessThanOrEqualTo(10_000);
        });
    }
}
