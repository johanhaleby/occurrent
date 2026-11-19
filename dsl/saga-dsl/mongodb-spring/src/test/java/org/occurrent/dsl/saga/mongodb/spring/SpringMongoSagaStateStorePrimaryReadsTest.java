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
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. A release turns an instance active once its read of the skipped-input records finds none, so a read
 * from a secondary that lags can find none while a record is written. Every read of the store is from the primary
 * whatever read preference its {@code MongoOperations} has.
 * <p>
 * The container is a single-node replica set that a test reaches by a direct connection, and a direct connection
 * ignores the read preference when it selects a server, so a read with a secondary preference does not fail here.
 * What differs is the {@code $readPreference} the driver puts on the {@code find} command, which a command listener
 * reads off the wire. The template here prefers a secondary, so a read that honoured it sends {@code secondary}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStorePrimaryReadsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-primary-reads";
    private static final String QUARANTINE_ID = "q-1";

    record Payment(long amount) {
    }

    private SpringMongoSagaStateStore<Payment> writer;
    private SpringMongoSagaStateStore<Payment> secondaryOnly;
    private SagaEnvelopes<Payment> writerEnvelopes;
    private SagaEnvelopes<Payment> secondaryOnlyEnvelopes;
    private MongoTemplate secondaryOnlyTemplate;
    // The read preference each find command was sent with, as the server was told
    private final List<String> readPreferencesOfFinds = new CopyOnWriteArrayList<>();

    @BeforeEach
    void createTheStores() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-primary-" + UUID.randomUUID().toString().substring(0, 8)));
        String database = requireNonNull(connectionString.getDatabase());
        writer = new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(connectionString), database), COLLECTION, Payment.class);
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("find")) {
                            BsonDocument preference = event.getCommand().getDocument("$readPreference", new BsonDocument());
                            readPreferencesOfFinds.add(preference.containsKey("mode") ? preference.getString("mode").getValue() : "none");
                        }
                    }
                }).build();
        secondaryOnlyTemplate = new MongoTemplate(MongoClients.create(settings), database);
        secondaryOnlyTemplate.setReadPreference(ReadPreference.secondary());
        secondaryOnly = new SpringMongoSagaStateStore<>(secondaryOnlyTemplate, COLLECTION, Payment.class);
        writerEnvelopes = SagaEnvelopes.of(writer, null);
        secondaryOnlyEnvelopes = SagaEnvelopes.of(secondaryOnly, null);
    }

    private static SagaEnvelope<Payment> envelope(String sagaId, SagaStatus status, List<TimerEntry> timers, SagaFailure failure) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), status, 1, timers, Map.of("order-1", 6L), null,
                NOW.minusSeconds(600), NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static SagaEnvelope<Payment> atVersion(SagaEnvelope<Payment> e, long version) {
        return new SagaEnvelope<>(e.sagaId(), e.state(), e.status(), version, e.timers(), e.streamWatermarks(), null, e.createdAt(), e.updatedAt(), e.completedAt(), e.currentStep(), e.started(), e.failure(), e.streamMarks(), e.quarantineSkipCount());
    }

    private static SagaFailure releasingFailure() {
        return new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
    }

    private List<String> readPreferencesSentBy(Runnable read) {
        readPreferencesOfFinds.clear();
        read.run();
        return List.copyOf(readPreferencesOfFinds);
    }

    @Test
    void the_template_used_here_sends_a_secondary_read_preference_so_the_tests_below_prove_something() {
        writerEnvelopes.compareAndSave("control", envelope("control", SagaStatus.ACTIVE, List.of(), null), 0);

        List<String> sent = readPreferencesSentBy(() -> secondaryOnlyTemplate.findOne(Query.query(where("_id").is("control")), Document.class, COLLECTION));

        assertThat(sent).containsExactly("secondary");
    }

    @Test
    void find_reads_from_the_primary_whatever_the_read_preference_of_the_template() {
        writerEnvelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(), null), 0);

        AtomicReference<Optional<SagaEnvelope<Payment>>> read = new AtomicReference<>();

        List<String> sent = readPreferencesSentBy(() -> read.set(secondaryOnlyEnvelopes.find("order-1")));

        assertAll(
                () -> assertThat(sent).as("read preference of each find").isNotEmpty().doesNotContain("secondary"),
                () -> assertThat(read.get()).get().extracting(SagaEnvelope::state).isEqualTo(new Payment(12L))
        );
    }

    @Test
    void find_without_state_reads_from_the_primary_whatever_the_read_preference_of_the_template() {
        writerEnvelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(), null), 0);

        AtomicReference<Optional<SagaEnvelope<Payment>>> read = new AtomicReference<>();

        List<String> sent = readPreferencesSentBy(() -> read.set(secondaryOnlyEnvelopes.findWithoutState("order-1")));

        assertAll(
                () -> assertThat(sent).as("read preference of each find").isNotEmpty().doesNotContain("secondary"),
                () -> assertThat(read.get()).get().extracting(SagaEnvelope::sagaId).isEqualTo("order-1")
        );
    }

    @Test
    void find_with_due_timers_reads_from_the_primary_whatever_the_read_preference_of_the_template() {
        writerEnvelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.ACTIVE, List.of(new TimerEntry("payment", NOW.toEpochMilli())), null), 0);

        AtomicReference<List<SagaEnvelope<Payment>>> read = new AtomicReference<>();

        List<String> sent = readPreferencesSentBy(() -> read.set(secondaryOnlyEnvelopes.findWithDueTimers(NOW.plusSeconds(1), 10)));

        assertAll(
                () -> assertThat(sent).as("read preference of each find").isNotEmpty().doesNotContain("secondary"),
                () -> assertThat(read.get()).extracting(SagaEnvelope::sagaId).containsExactly("order-1")
        );
    }

    @Test
    void find_by_status_reads_from_the_primary_whatever_the_read_preference_of_the_template() {
        writerEnvelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.QUARANTINED, List.of(), releasingFailure()), 0);

        AtomicReference<List<SagaEnvelope<Payment>>> read = new AtomicReference<>();

        List<String> sent = readPreferencesSentBy(() -> read.set(secondaryOnlyEnvelopes.findByStatus(SagaStatus.QUARANTINED, NOW.plusSeconds(1), 10)));

        assertAll(
                () -> assertThat(sent).as("read preference of each find").isNotEmpty().doesNotContain("secondary"),
                () -> assertThat(read.get()).extracting(SagaEnvelope::sagaId).containsExactly("order-1")
        );
    }

    @Test
    void find_releasing_saga_ids_reads_from_the_primary_whatever_the_read_preference_of_the_template() {
        writerEnvelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.QUARANTINED, List.of(), releasingFailure()), 0);

        AtomicReference<List<String>> read = new AtomicReference<>();

        List<String> sent = readPreferencesSentBy(() -> read.set(secondaryOnly.findReleasingSagaIds(null, 10)));

        assertAll(
                () -> assertThat(sent).as("read preference of each find").isNotEmpty().doesNotContain("secondary"),
                () -> assertThat(read.get()).containsExactly("order-1")
        );
    }

    @Test
    void find_skipped_inputs_reads_from_the_primary_whatever_the_read_preference_of_the_template() {
        writerEnvelopes.compareAndSave("order-1", envelope("order-1", SagaStatus.QUARANTINED, List.of(), releasingFailure()), 0);
        writerEnvelopes.compareAndSaveSkipped(writer, atVersion(envelope("order-1", SagaStatus.QUARANTINED, List.of(), releasingFailure()), 2), 1, QUARANTINE_ID, new SkippedInput(0, "order-1@8"));

        AtomicReference<List<SkippedInput>> read = new AtomicReference<>();

        List<String> sent = readPreferencesSentBy(() -> read.set(secondaryOnly.findSkippedInputs("order-1", QUARANTINE_ID, 10)));

        assertAll(
                () -> assertThat(sent).as("read preference of each find").isNotEmpty().doesNotContain("secondary"),
                () -> assertThat(read.get()).containsExactly(new SkippedInput(0, "order-1@8"))
        );
    }
}
