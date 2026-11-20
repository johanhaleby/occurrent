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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
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
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. The store commits the transaction it starts at write concern {@code majority} with {@code j: true} and
 * a {@code wtimeout} of 10 seconds. A client with a {@code timeoutMS} makes the driver drop the {@code wtimeout}, and a
 * {@code timeoutMS} of 0 waits forever. The wire shows that a commit under a {@code timeoutMS} of 0 has neither a
 * {@code wtimeout} nor a {@code maxTimeMS}, so it has no time limit, and that a client {@code timeoutMS} above 0 bounds
 * it. The commit is read off the wire, since a single-node replica set acknowledges it at once whatever it asks for.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreTransactionTimeoutTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-transaction-timeout";
    private static final String QUARANTINE_ID = "q-1";
    private static final long TEN_SECONDS = 10_000;

    record Payment(long amount) {
    }

    // The commit of the transaction the store starts, which the caller has no transaction around
    private BsonDocument commitOfAStoreTransactionOnAClient(Consumer<MongoClientSettings.Builder> clientSettings) {
        List<BsonDocument> commits = new CopyOnWriteArrayList<>();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-timeout-" + UUID.randomUUID().toString().substring(0, 8)));
        MongoClientSettings.Builder settings = MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.W1)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("commitTransaction")) {
                            commits.add(BsonDocument.parse(event.getCommand().toJson()));
                        }
                    }
                });
        clientSettings.accept(settings);
        try (MongoClient client = MongoClients.create(settings.build())) {
            MongoTemplate template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
            template.setWriteConcern(WriteConcern.W1);
            SpringMongoSagaStateStore<Payment> store = new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class);
            SagaEnvelopes<Payment> envelopes = SagaEnvelopes.of(store, null);
            envelopes.compareAndSave("order-1", envelope(1), 0);
            commits.clear();

            assertThat(envelopes.compareAndSaveSkipped(store, envelope(2), 1, QUARANTINE_ID, new SkippedInput(0, "order-1@8"))).isTrue();
        }
        assertThat(commits).as("commitTransaction commands the store sent").hasSize(1);
        return commits.get(0);
    }

    private static SagaEnvelope<Payment> envelope(long version) {
        SagaFailure failure = new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
        return new SagaEnvelope<>("order-1", new Payment(12L), SagaStatus.QUARANTINED, version, List.of(new TimerEntry("payment", NOW.toEpochMilli())), Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static long maxTimeMs(BsonDocument commit) {
        return commit.containsKey("maxTimeMS") ? commit.get("maxTimeMS").asNumber().longValue() : -1;
    }

    @Test
    void a_client_timeoutMS_of_0_leaves_the_commit_of_a_transaction_without_a_time_limit() {
        BsonDocument commit = commitOfAStoreTransactionOnAClient(settings -> settings.timeout(0, MILLISECONDS));

        BsonDocument writeConcern = commit.getDocument("writeConcern", new BsonDocument());
        assertAll(
                () -> assertThat(commit.containsKey("maxTimeMS")).as("maxTimeMS in %s", commit).isFalse(),
                () -> assertThat(writeConcern.containsKey("wtimeout")).as("wtimeout in %s", commit).isFalse()
        );
    }

    @Test
    void a_client_without_timeoutMS_commits_at_majority_with_journal_and_a_wtimeout_of_10_seconds() {
        BsonDocument commit = commitOfAStoreTransactionOnAClient(settings -> {
        });

        BsonDocument writeConcern = commit.getDocument("writeConcern", new BsonDocument());
        assertAll(
                () -> assertThat(writeConcern.get("w")).as("w of %s", commit).isEqualTo(new org.bson.BsonString("majority")),
                () -> assertThat(writeConcern.getBoolean("j", org.bson.BsonBoolean.FALSE).getValue()).as("j of %s", commit).isTrue(),
                () -> assertThat(writeConcern.get("wtimeout")).as("wtimeout of %s", commit).isNotNull().extracting(v -> v.asNumber().longValue()).isEqualTo(TEN_SECONDS)
        );
    }

    @Test
    void a_client_timeoutMS_of_2_seconds_is_not_raised_to_10_seconds() {
        BsonDocument commit = commitOfAStoreTransactionOnAClient(settings -> settings.timeout(2_000, MILLISECONDS));

        assertThat(maxTimeMs(commit)).as("maxTimeMS of %s", commit).isBetween(1L, 2_000L);
    }
}
