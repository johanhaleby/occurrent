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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.StepCondition;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set. A flow saga on the Spring MongoDB store keeps its received events as CloudEvent JSON
 * strings in its stored state, and the store reads them back as CloudEvents whenever it reads the instance whole. When
 * one of them no longer parses, that read fails, and a release must still be able to charge the failure to its record
 * or to end the quarantine. The failure comes from the store's own decoding here, since a malformed JSON string is
 * written into the stored received list of the instance, and nothing is injected in the state codec. What the release
 * checks before each write that leaves the stored state alone reads the stored document as it is, so it has to answer
 * for such an instance too.
 */
@Testcontainers
@DisplayName("A release over a flow saga whose stored received events no longer decode, on MongoDB")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(180)
class SpringMongoSagaReleaseUndecodableStoredEventsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-undecodable-stored-events";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String ORDER = "o1";
    private static final String MALFORMED = "{not json";
    private static final String RECEIVED = SagaCodec.STATE + "." + SagaCodec.FLOW_RECEIVED;
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);

    sealed interface OrderEvent permits OrderPlaced, Approved {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record Approved(String eventId, String orderId) implements OrderEvent {
    }

    record Pack(String orderId, String step) {
    }

    private volatile boolean poisonFails = false;
    // The release replay after which the stored received events are corrupted, 0 for none
    private volatile int corruptAfterReplay = 0;
    private final AtomicInteger replays = new AtomicInteger();
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoSagaStateStore<FlowState<OrderEvent>> mongo;
    private SagaEnvelopes<FlowState<OrderEvent>> envelopes;
    private SagaExecution<OrderEvent, FlowState<OrderEvent>, Pack> execution;
    private String quarantineId = "";
    private String keyOfA = "";
    private String keyOfB = "";
    private CloudEvent eventA;

    // FlowState.class erases its type argument, as in the other flow tests of this module
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<OrderEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    // Four steps, each completed by an Approved, so the three events of a test leave the saga open. The first fails on its input while poisonFails is set
    private Saga<OrderEvent, FlowState<OrderEvent>, Pack> saga() {
        return FlowSaga.<OrderEvent, Pack>builder()
                .startsOn(OrderPlaced.class)
                .correlateAll(OrderEvent::orderId)
                .step("first", step -> step.on(StepCondition.event(Approved.class), Continuation.next(), received -> {
                    if (poisonFails) {
                        throw new IllegalStateException("cannot pack the first step");
                    }
                    return List.of(new Pack(ORDER, "first"));
                }))
                .step("second", step -> step.on(StepCondition.event(Approved.class), Continuation.next(), received -> List.of(new Pack(ORDER, "second"))))
                .step("third", step -> step.on(StepCondition.event(Approved.class), Continuation.next(), received -> List.of(new Pack(ORDER, "third"))))
                .step("fourth", step -> step.on(StepCondition.event(Approved.class), Continuation.end(), received -> List.of(new Pack(ORDER, "fourth"))))
                .build();
    }

    private CloudEvent append(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
        return eventStore.read(ORDER).eventList().getLast();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<FlowState<OrderEvent>> instance() {
        return envelopes.findWithoutState(ORDER).orElseThrow();
    }

    // Read off the collection itself, since the store can't read an instance whose received events don't decode
    private Document raw() {
        return Objects.requireNonNull(template.getCollection(COLLECTION).find(new Document("_id", ORDER)).first());
    }

    // Stored as epoch milliseconds
    private static Instant firstFailedAt(Document stored) {
        return Instant.ofEpochMilli(((Number) Objects.requireNonNull(stored.get(SagaCodec.FAILURE_FIRST_FAILED_AT))).longValue());
    }

    private @Nullable Object rawReceived() {
        return ((Document) Objects.requireNonNull(raw().get(SagaCodec.STATE))).get(SagaCodec.FLOW_RECEIVED);
    }

    private List<String> recordKeys(String ofQuarantineId) {
        return template.getCollection(SKIPPED_COLLECTION).find(new Document("sagaId", ORDER).append("quarantineId", ofQuarantineId))
                .sort(Sorts.ascending("seq", "key")).map(record -> record.getString("key")).into(new ArrayList<>());
    }

    private void corruptTheReceivedEvents() {
        assertThat(template.getCollection(COLLECTION).updateOne(new Document("_id", ORDER), Updates.set(RECEIVED, List.of(MALFORMED))).getModifiedCount()).isEqualTo(1L);
    }

    // The store the execution uses is the Mongo one, which corrupts the stored received events right after the replay
    // of the record corruptAfterReplay that it saved
    @SuppressWarnings("unchecked")
    private SagaStateStore<FlowState<OrderEvent>> corruptingStore() {
        return (SagaStateStore<FlowState<OrderEvent>>) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{SagaStateStore.class, SagaStateStoreSkippedInputs.class}, (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(mongo, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (method.getName().equals("compareAndSaveReplayed") && Boolean.TRUE.equals(result)
                        && replays.incrementAndGet() == corruptAfterReplay) {
                        corruptTheReceivedEvents();
                    }
                    return result;
                });
    }

    @BeforeEach
    void quarantinedOnTheFirstStepWithTwoMoreRecorded() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-undecodable-stored-" + UUID.randomUUID().toString().substring(0, 8)));
        client = MongoClients.create(connectionString);
        template = new MongoTemplate(client, Objects.requireNonNull(connectionString.getDatabase()));
        mongo = new SpringMongoSagaStateStore<>(template, COLLECTION, rawFlowStateType(), converter);
        envelopes = SagaEnvelopes.of(mongo, converter);
        execution = new SagaExecution<>("orders", saga(), corruptingStore(), dispatched::add, converter, CONFIG, now::get);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new Approved(nextEventId(), ORDER));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        eventA = append(new Approved(nextEventId(), ORDER));
        execution.onCloudEvent(eventA);
        execution.onCloudEvent(append(new Approved(nextEventId(), ORDER)));
        quarantineId = Objects.requireNonNull(instance().quarantineId());
        List<String> keys = recordKeys(quarantineId);
        assertThat(keys).as("records of the quarantine").hasSize(3);
        keyOfA = keys.get(1);
        keyOfB = keys.get(2);
        assertThat(rawReceived()).as("control: the received events are stored as JSON strings before anything is corrupted")
                .isInstanceOfSatisfying(List.class, received -> assertThat(received).isNotEmpty().allSatisfy(json -> assertThat(json).isInstanceOf(String.class).isNotEqualTo(MALFORMED)));
    }

    @AfterEach
    void close() {
        client.close();
    }

    private void pollAfterTheInterval() {
        now.set(now.get().plus(CONFIG.timerPollInterval()).plusMillis(1));
        execution.pollTimers();
    }

    private String evidence(@Nullable Throwable thrown) {
        Document stored = raw();
        return "threw " + thrown + ", stored status " + stored.get(SagaCodec.STATUS) + ", failure input " + stored.get(SagaCodec.FAILURE_INPUT)
               + ", releasedAt " + stored.get(SagaCodec.FAILURE_RELEASED_AT) + ", quarantine id " + stored.get(SagaCodec.FAILURE_QUARANTINE_ID)
               + ", records " + recordKeys(quarantineId) + ", dispatched " + dispatched;
    }

    @Test
    void a_release_whose_last_replay_leaves_stored_received_events_that_no_longer_decode_ends_released_and_leaves_them_stored_as_they_were() {
        poisonFails = false;
        corruptAfterReplay = 3;

        Throwable thrown = catchThrowable(() -> execution.release(ORDER));
        Throwable thrownByThePoll = catchThrowable(this::pollAfterTheInterval);

        Document stored = raw();
        String evidence = evidence(thrown) + ", poll threw " + thrownByThePoll;
        assertAll(
                () -> assertThat(replays.get()).as("control: every record was replayed and the received events corrupted after the last. " + evidence).isEqualTo(3),
                () -> assertThat(instance().status()).as("the instance is active again. " + evidence).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(stored.get(SagaCodec.FAILURE_RELEASED_AT)).as("no releasing marker is left. " + evidence).isNull(),
                () -> assertThat(stored.get(SagaCodec.FAILURE_QUARANTINE_ID)).as("no quarantine id is left. " + evidence).isNull(),
                () -> assertThat(recordKeys(quarantineId)).as("no record is left. " + evidence).isEmpty(),
                () -> assertThat(rawReceived()).as("the malformed JSON stays stored as it was. " + evidence).isEqualTo(List.of(MALFORMED)),
                () -> assertThat(dispatched).as("every record was applied once. " + evidence)
                        .containsExactly(new Pack(ORDER, "first"), new Pack(ORDER, "second"), new Pack(ORDER, "third"))
        );
    }

    @Test
    void a_poll_charges_the_record_after_stored_received_events_that_no_longer_decode_and_quarantines_the_instance_again_on_it_past_its_budget_with_every_record_stored() {
        poisonFails = false;
        corruptAfterReplay = 1;
        Instant chargedAt = now.get();
        Throwable thrown = catchThrowable(() -> execution.release(ORDER));
        Document charged = raw();
        String chargedEvidence = evidence(thrown);
        Throwable insideThrown = catchThrowable(this::pollAfterTheInterval);
        Document insideTheBudget = raw();
        String insideEvidence = evidence(insideThrown);
        now.set(chargedAt.plus(BUDGET));
        Throwable pastThrown = catchThrowable(execution::pollTimers);

        Document after = raw();
        String evidence = evidence(pastThrown);
        assertAll(
                () -> assertThat(replays.get()).as("control: the first record was replayed and the received events corrupted after it. " + chargedEvidence).isEqualTo(1),
                () -> assertThat(charged.get(SagaCodec.FAILURE_INPUT)).as("the record after it is charged. " + chargedEvidence).isEqualTo(keyOfA),
                () -> assertThat(firstFailedAt(charged)).as("its clock starts when it was first charged. " + chargedEvidence).isEqualTo(chargedAt),
                () -> assertThat(charged.get(SagaCodec.FAILURE_RELEASED_AT)).as("inside its budget the instance stays releasing. " + chargedEvidence).isNotNull(),
                () -> assertThat(insideTheBudget.get(SagaCodec.FAILURE_INPUT)).as(insideEvidence).isEqualTo(keyOfA),
                () -> assertThat(firstFailedAt(insideTheBudget)).as("a poll inside the budget keeps the clock of the first charge. " + insideEvidence).isEqualTo(chargedAt),
                () -> assertThat(insideTheBudget.get(SagaCodec.FAILURE_RELEASED_AT)).as(insideEvidence).isNotNull(),
                () -> assertThat(instance().status()).as("past its budget the instance is quarantined again. " + evidence).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.get(SagaCodec.FAILURE_RELEASED_AT)).as("and no longer releasing. " + evidence).isNull(),
                () -> assertThat(after.get(SagaCodec.FAILURE_INPUT)).as(evidence).isEqualTo(keyOfA),
                () -> assertThat(firstFailedAt(after)).as(evidence).isEqualTo(chargedAt),
                () -> assertThat(recordKeys(quarantineId)).as("the record is never dropped, and nor is the one after it. " + evidence).containsExactly(keyOfA, keyOfB),
                () -> assertThat(rawReceived()).as("the malformed JSON stays stored as it was. " + evidence).isEqualTo(List.of(MALFORMED)),
                () -> assertThat(dispatched).as(evidence).containsExactly(new Pack(ORDER, "first"))
        );
    }

    // The type and the stream version of an input are known only from its event. A failure record that reads the record
    // alone names the event, so what the instance holds pending under its type is the version of that record. One that
    // fell back to the key alone, as a release did when the read of the record came with the instance's state that
    // doesn't decode, has no type to name, so it holds the version under none
    @Test
    void the_failure_of_a_record_read_while_the_state_does_not_decode_names_the_event_of_that_record_by_its_type_and_not_by_its_key_alone() {
        poisonFails = false;
        corruptAfterReplay = 1;
        long versionOfA = Long.parseLong(String.valueOf(eventA.getExtension("streamversion")));

        Throwable thrown = catchThrowable(() -> execution.release(ORDER));

        StreamMarks marks = Objects.requireNonNull(instance().streamMarks().get(ORDER));
        String evidence = evidence(thrown) + ", pending " + marks.pending();
        assertAll(
                () -> assertThat(raw().get(SagaCodec.FAILURE_INPUT)).as("control: the failure is charged to the record. " + evidence).isEqualTo(keyOfA),
                () -> assertThat(marks.pending().get(eventA.getType())).as("pending under the type of the event. " + evidence).contains(versionOfA),
                () -> assertThat(marks.pending().get("")).as("and not under the empty type of a key alone. " + evidence).isNull()
        );
    }

    // The without-state instance a release writes, with a failure message of the given size
    private StoredSaga writeWithAMessageOf(int characters) {
        StoredSaga stored = mongo.findWithoutState(ORDER).orElseThrow();
        Map<String, Object> fields = new LinkedHashMap<>(stored.encoded().fields());
        fields.put(SagaCodec.FAILURE_MESSAGE, "m".repeat(characters));
        EncodedSaga encoded = new EncodedSaga(fields);
        return new StoredSaga(stored.sagaId(), stored.version(), encoded, SagaCodec.indexOf(encoded));
    }

    @Test
    void asking_whether_a_write_without_the_state_fits_on_a_stored_instance_whose_received_events_no_longer_decode_answers_instead_of_throwing() {
        corruptTheReceivedEvents();
        StoredSaga write = mongo.findWithoutState(ORDER).orElseThrow();
        AtomicReference<@Nullable Boolean> fits = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> fits.set(mongo.fitsWithoutState(write, write.version())));

        assertAll(
                () -> assertThat(catchThrowable(() -> mongo.find(ORDER))).as("control: reading the instance whole fails on the stored received events").isNotNull(),
                () -> assertThat(thrown).as("the check does not decode the stored state").isNull(),
                () -> assertThat(fits.get()).isTrue()
        );
    }

    @Test
    void a_write_without_the_state_does_not_fit_when_the_stored_state_alone_takes_the_document_past_the_limit_of_mongodb() {
        // The state alone is under the limit of 16 MiB, so only measuring it with the fields the write sets can refuse
        assertThat(template.getCollection(COLLECTION).updateOne(new Document("_id", ORDER), Updates.set(RECEIVED, List.of("s".repeat(15_000_000)))).getModifiedCount()).isEqualTo(1L);
        StoredSaga small = writeWithAMessageOf(10);
        StoredSaga large = writeWithAMessageOf(2_000_000);

        assertAll(
                () -> assertThat(mongo.fitsWithoutState(small, small.version())).as("control: the write fits next to that state").isTrue(),
                () -> assertThat(mongo.fitsWithoutState(large, large.version())).as("the fields the write sets alone would fit, so the stored state is measured").isFalse()
        );
    }

    @Test
    void a_write_without_the_state_fits_when_the_instance_is_not_stored_or_is_stored_at_another_version_since_the_compare_and_set_refuses_it() {
        assertThat(template.getCollection(COLLECTION).updateOne(new Document("_id", ORDER), Updates.set(RECEIVED, List.of("s".repeat(15_000_000)))).getModifiedCount()).isEqualTo(1L);
        StoredSaga large = writeWithAMessageOf(2_000_000);
        StoredSaga ofAnotherInstance = new StoredSaga("another", large.version(), large.encoded(), large.index());

        assertAll(
                () -> assertThat(mongo.fitsWithoutState(large, large.version() - 1)).as("stored at another version, though it would not fit at this one").isTrue(),
                () -> assertThat(mongo.fitsWithoutState(large, large.version() + 1)).as("stored at another version, though it would not fit at this one").isTrue(),
                () -> assertThat(mongo.fitsWithoutState(ofAnotherInstance, large.version())).as("not stored").isTrue()
        );
    }
}
