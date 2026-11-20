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
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. A client {@code timeoutMS} of 0 means no timeout, so a skipped-input write that waits for a majority
 * that is gone would wait forever and hold the subscription thread. The store puts a bound of at most 10 seconds back,
 * which the driver sends as {@code maxTimeMS} on the {@code update} command, and a command listener reads it off the
 * wire.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreSkippedInputWriteTimeoutTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-skipped-input-write-timeout";
    private static final String SKIPPED_INPUTS_COLLECTION = COLLECTION + "_skipped_inputs";

    record Payment(long amount) {
    }

    // The command document of each update sent to the skipped-inputs collection
    private final List<BsonDocument> skippedInputUpdates = new CopyOnWriteArrayList<>();

    private SpringMongoSagaStateStore<Payment> storeOnAClientWithATimeoutOf(long timeoutMillis) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-timeout-" + UUID.randomUUID().toString().substring(0, 8)));
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .timeout(timeoutMillis, MILLISECONDS)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        BsonDocument command = event.getCommand();
                        if (event.getCommandName().equals("update") && SKIPPED_INPUTS_COLLECTION.equals(command.getString("update").getValue())) {
                            skippedInputUpdates.add(command);
                        }
                    }
                }).build();
        return new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(settings), requireNonNull(connectionString.getDatabase())), COLLECTION, Payment.class);
    }

    @Test
    void a_client_timeout_of_zero_still_sends_the_skipped_input_write_with_a_max_time_of_at_most_ten_seconds() {
        SpringMongoSagaStateStore<Payment> store = storeOnAClientWithATimeoutOf(0);

        store.recordSkippedInput("order-1", "q-1", new SkippedInput(0, "order-1@7"));

        assertThat(skippedInputUpdates).singleElement().satisfies(update -> {
            assertThat(update.containsKey("maxTimeMS")).as("the update carries maxTimeMS").isTrue();
            assertThat(update.get("maxTimeMS").asNumber().longValue()).as("maxTimeMS").isGreaterThan(0).isLessThanOrEqualTo(10_000);
        });
    }
}
