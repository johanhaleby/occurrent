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

// In the package of SagaExecution, which is not public, so the clock of a release is the test's and no wait stands in for time.
// The store is the Spring MongoDB one, so it lives with the tests of that module
package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set. Two things a release meets that no longer decode, against real MongoDB. A state the
 * last replay saved, which is read after that replay deleted the last record, so no record is left to count the failure
 * against and the quarantine ends with the stored state left as it is, checked on the raw document. And a held record
 * whose event no longer decodes, corrupted in the collection of the records, which is counted against the budget of that
 * record by its seq and key, and stays stored whatever the budget says.
 */
@Testcontainers
@DisplayName("A release over data that no longer decodes, on MongoDB")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
class SpringMongoSagaReleaseUndecodableDataTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-undecodable";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record Pack(String orderId, String item) {
    }

    private volatile boolean poisonFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<String> mongo;
    private SagaEnvelopes<String> envelopes;
    private FaultyStore store;
    private SagaExecution<OrderEvent, String, Object> execution;
    private String quarantineId = "";
    private String keyOfA = "";
    private String keyOfB = "";

    // The state is the items packed so far, as one string
    private Saga<OrderEvent, String, Object> saga() {
        return Saga.<OrderEvent, String, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "")
                .react(OrderPlaced.class, (state, e) -> List.of())
                .evolve(ItemAdded.class, (state, e) -> state.isEmpty() ? e.item() : state + "," + e.item())
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .build();
    }

    private CloudEvent append(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
        return eventStore.read(ORDER).eventList().getLast();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<String> instance() {
        return envelopes.findWithoutState(ORDER).orElseThrow();
    }

    // Read off the collection itself, since a record that no longer decodes can't be read through the store
    private List<String> recordKeys(String ofQuarantineId) {
        return template.getCollection(SKIPPED_COLLECTION).find(new Document("sagaId", ORDER).append("quarantineId", ofQuarantineId))
                .sort(Sorts.ascending("seq", "key")).map(record -> record.getString("key")).into(new ArrayList<>());
    }

    private @Nullable Object rawState() {
        return Objects.requireNonNull(template.getCollection(COLLECTION).find(new Document("_id", ORDER)).first()).get(SagaCodec.STATE);
    }

    @BeforeEach
    void quarantinedOnPoisonWithAAndBRecorded() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-undecodable-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(connectionString);
        template = new MongoTemplate(client, Objects.requireNonNull(connectionString.getDatabase()));
        mongo = new SpringMongoSagaStateStore<>(template, COLLECTION, String.class);
        envelopes = SagaEnvelopes.of(mongo, null);
        store = new FaultyStore();
        execution = new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        quarantineId = Objects.requireNonNull(instance().quarantineId());
        List<String> keys = recordKeys(quarantineId);
        assertThat(keys).as("records of the quarantine").hasSize(3);
        keyOfA = keys.get(1);
        keyOfB = keys.get(2);
    }

    @AfterEach
    void close() {
        client.close();
    }

    private void corruptTheEventOf(String key) {
        assertThat(template.getCollection(SKIPPED_COLLECTION).updateOne(new Document("sagaId", ORDER).append("key", key), Updates.set("event", "{ this is not a CloudEvent")).getModifiedCount()).isEqualTo(1L);
    }

    private void pollAfterTheInterval() {
        now.set(now.get().plus(CONFIG.timerPollInterval()).plusMillis(1));
        execution.pollTimers();
    }

    private String evidence(@Nullable ReleaseResult result, @Nullable Throwable thrown) {
        SagaEnvelope<String> current = instance();
        return "result " + result + ", threw " + thrown + ", status " + current.status() + ", releasing " + current.isReleasing() + ", failure "
               + current.failure() + ", records " + recordKeys(Objects.requireNonNullElse(current.quarantineId(), quarantineId)) + ", dispatched " + dispatched;
    }

    // What a release that ended over a state that no longer decodes leaves, read the way MongoDB holds it
    private List<Executable> endedWithTheStateTheLastReplaySaved(@Nullable ReleaseResult result, @Nullable Throwable thrown) {
        SagaEnvelope<String> after = instance();
        String evidence = evidence(result, thrown);
        Object stored = rawState();
        return List.of(
                () -> assertThat(store.stateStoppedDecoding).as("the state stopped decoding after the last replay. " + evidence).isTrue(),
                () -> assertThat(thrown).as("the failed read ends the release and is not thrown. " + evidence).isNull(),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(after.isReleasing()).as(evidence).isFalse(),
                () -> assertThat(after.quarantineId()).as("no quarantine id is left. " + evidence).isNull(),
                () -> assertThat(after.failure()).as("no failure is left charged. " + evidence).isNull(),
                () -> assertThat(after.quarantineSkipCount()).as("no skip count is left. " + evidence).isNull(),
                () -> assertThat(recordKeys(quarantineId)).as("no record is left. " + evidence).isEmpty(),
                () -> assertThat(dispatched).as("every record was applied once. " + evidence)
                        .containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(stored).as("the stored state is the one the last replay saved. " + evidence).isNotNull().isEqualTo(store.stateSavedByTheLastReplay),
                () -> assertThat(stored).as("and it holds every item. " + evidence).isEqualTo("poison,a,b")
        );
    }

    @Test
    void a_release_whose_last_replay_saves_a_state_that_stops_decoding_ends_released_and_leaves_the_stored_state_as_that_replay_saved_it() {
        poisonFails = false;
        store.stopsDecodingAfterTheLastReplay = true;
        AtomicReference<@Nullable ReleaseResult> result = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> result.set(execution.release(ORDER)));

        List<Executable> assertions = new ArrayList<>(endedWithTheStateTheLastReplaySaved(result.get(), thrown));
        assertions.add(() -> assertThat(result.get()).as(evidence(result.get(), thrown)).isEqualTo(ReleaseResult.RELEASED));
        assertAll(assertions);
    }

    @Test
    void a_timer_poll_that_continues_a_release_whose_last_replay_saves_a_state_that_stops_decoding_ends_it_released_in_the_same_poll() {
        AtomicReference<@Nullable ReleaseResult> started = new AtomicReference<>();
        catchThrowable(() -> started.set(execution.release(ORDER)));
        assertThat(started.get()).as(evidence(started.get(), null)).isEqualTo(ReleaseResult.STILL_RELEASING);
        poisonFails = false;
        store.stopsDecodingAfterTheLastReplay = true;

        Throwable thrown = catchThrowable(this::pollAfterTheInterval);

        assertAll(endedWithTheStateTheLastReplaySaved(null, thrown));
    }

    @Test
    void a_held_record_whose_event_no_longer_decodes_is_charged_by_its_key_after_the_record_before_it_is_replayed_and_the_instance_stays_releasing_inside_its_budget() {
        poisonFails = false;
        corruptTheEventOf(keyOfA);
        AtomicReference<@Nullable ReleaseResult> result = new AtomicReference<>();
        Instant chargedAt = now.get();

        Throwable thrown = catchThrowable(() -> result.set(execution.release(ORDER)));

        SagaEnvelope<String> after = instance();
        String evidence = evidence(result.get(), thrown);
        assertAll(
                () -> assertThat(thrown).as("the failed read is recorded and not thrown. " + evidence).isNull(),
                () -> assertThat(result.get()).as(evidence).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(after.isReleasing()).as(evidence).isTrue(),
                () -> assertThat(dispatched).as("the record before it was replayed and nothing else. " + evidence).containsExactly(new Pack(ORDER, "poison")),
                () -> assertThat(after.failure()).as(evidence).isNotNull(),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as("the failure is charged to the record that does not decode. " + evidence).isEqualTo(keyOfA),
                () -> assertThat(Objects.requireNonNull(after.failure()).firstFailedAt()).as("its clock starts when it was first charged. " + evidence).isEqualTo(chargedAt),
                () -> assertThat(recordKeys(quarantineId)).as("the record and the one after it are stored. " + evidence).containsExactly(keyOfA, keyOfB)
        );
    }

    @Test
    void a_second_charge_inside_the_budget_keeps_the_clock_of_the_first_and_one_past_it_quarantines_the_instance_again_on_the_record_with_it_and_the_one_after_it_stored() {
        poisonFails = false;
        corruptTheEventOf(keyOfA);
        Instant firstCharge = now.get();
        execution.release(ORDER);
        now.set(now.get().plus(BUDGET.dividedBy(2)));
        AtomicReference<@Nullable ReleaseResult> inside = new AtomicReference<>();
        Throwable insideThrown = catchThrowable(() -> inside.set(execution.release(ORDER)));
        String insideEvidence = evidence(inside.get(), insideThrown);
        SagaEnvelope<String> insideTheBudget = instance();
        now.set(firstCharge.plus(BUDGET));
        AtomicReference<@Nullable ReleaseResult> past = new AtomicReference<>();
        Throwable pastThrown = catchThrowable(() -> past.set(execution.release(ORDER)));

        SagaEnvelope<String> after = instance();
        String evidence = evidence(past.get(), pastThrown);
        assertAll(
                () -> assertThat(insideThrown).as(insideEvidence).isNull(),
                () -> assertThat(inside.get()).as(insideEvidence).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(Objects.requireNonNull(insideTheBudget.failure()).firstFailedAt()).as("the clock is the first charge's. " + insideEvidence).isEqualTo(firstCharge),
                () -> assertThat(pastThrown).as(evidence).isNull(),
                () -> assertThat(past.get()).as(evidence).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).as(evidence).isFalse(),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as("quarantined again on the record that does not decode. " + evidence).isEqualTo(keyOfA),
                () -> assertThat(Objects.requireNonNull(after.failure()).firstFailedAt()).as(evidence).isEqualTo(firstCharge),
                () -> assertThat(recordKeys(Objects.requireNonNull(after.quarantineId()))).as("the record is never dropped, and nor is the one after it. " + evidence).containsExactly(keyOfA, keyOfB),
                () -> assertThat(dispatched).as(evidence).containsExactly(new Pack(ORDER, "poison"))
        );
    }

    @Test
    void a_timer_poll_charges_a_record_whose_event_no_longer_decodes_and_quarantines_the_instance_again_on_it_past_its_budget() {
        poisonFails = false;
        corruptTheEventOf(keyOfA);
        Instant firstCharge = now.get();
        execution.release(ORDER);
        pollAfterTheInterval();
        SagaEnvelope<String> insideTheBudget = instance();
        String insideEvidence = evidence(null, null);
        now.set(firstCharge.plus(BUDGET));
        Throwable thrown = catchThrowable(execution::pollTimers);

        SagaEnvelope<String> after = instance();
        String evidence = evidence(null, thrown);
        assertAll(
                () -> assertThat(insideTheBudget.isReleasing()).as("inside the budget the poll leaves it releasing. " + insideEvidence).isTrue(),
                () -> assertThat(Objects.requireNonNull(insideTheBudget.failure()).input()).as(insideEvidence).isEqualTo(keyOfA),
                () -> assertThat(thrown).as("the poll does not throw. " + evidence).isNull(),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).as(evidence).isFalse(),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as(evidence).isEqualTo(keyOfA),
                () -> assertThat(recordKeys(Objects.requireNonNull(after.quarantineId()))).as(evidence).containsExactly(keyOfA, keyOfB)
        );
    }

    @Test
    void releasing_an_instance_already_releasing_whose_first_record_does_not_decode_charges_the_failure_instead_of_throwing_it() {
        poisonFails = false;
        corruptTheEventOf(keyOfA);
        execution.release(ORDER);
        AtomicReference<@Nullable ReleaseResult> again = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> again.set(execution.release(ORDER)));

        SagaEnvelope<String> after = instance();
        String evidence = evidence(again.get(), thrown);
        assertAll(
                () -> assertThat(thrown).as("the read failure is charged and not thrown. " + evidence).isNull(),
                () -> assertThat(again.get()).as(evidence).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as(evidence).isEqualTo(keyOfA)
        );
    }

    // The store of the test: MongoDB with two faults, each off until a test asks for it. The state codec refuses to decode
    // once the replay that deleted the last record has been saved, so the read of the instance whole fails while the read
    // without its state and the read of the key of the first record work, and the state that replay saved is read off the
    // collection as it is stored
    private final class FaultyStore implements SagaStateStore<String>, SagaStateStoreSkippedInputs {
        volatile boolean stopsDecodingAfterTheLastReplay = false;
        volatile boolean stateStoppedDecoding = false;
        volatile @Nullable Object stateSavedByTheLastReplay;

        @Override
        public boolean fits(StoredSaga candidate) {
            return mongo.fits(candidate);
        }

        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            return mongo.fitsWithoutState(saga, expectedVersion);
        }

        @Override
        public void requireAtomicTransitions() {
            mongo.requireAtomicTransitions();
        }

        @Override
        public SagaStateCodec<String> stateCodec() {
            SagaStateCodec<String> codec = mongo.stateCodec();
            return new SagaStateCodec<>() {
                @Override
                public Object encode(String state) {
                    return codec.encode(state);
                }

                @Override
                public String decode(Object encoded) {
                    if (stateStoppedDecoding) {
                        throw new IllegalStateException("the state can no longer be decoded");
                    }
                    return codec.decode(encoded);
                }
            };
        }

        @Override
        public Optional<StoredSaga> find(String sagaId) {
            return mongo.find(sagaId);
        }

        @Override
        public Optional<StoredSaga> findWithoutState(String sagaId) {
            return mongo.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return mongo.compareAndSave(saga, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return mongo.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return mongo.findWithDueTimers(now, limit);
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            return mongo.findReleasingSagaIds(after, limit);
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            return mongo.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            boolean saved = mongo.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
            if (saved && stopsDecodingAfterTheLastReplay && recordKeys(quarantineId).isEmpty()) {
                stateSavedByTheLastReplay = rawState();
                stopsDecodingAfterTheLastReplay = false;
                stateStoppedDecoding = true;
            }
            return saved;
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return mongo.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return mongo.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            return mongo.findWithFirstSkippedInput(sagaId);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
            return mongo.findWithFirstSkippedInputKey(sagaId);
        }

        @Override
        public void delete(String sagaId) {
            mongo.delete(sagaId);
        }
    }
}
