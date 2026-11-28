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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. The downgrade from 0.35.0 to 0.34.0 runs two {@code updateMany} commands on each saga collection once
 * every 0.35.0 node has stopped. The stored state they run over is built by the 0.35.0 store and runner. The commands
 * turn {@code QUARANTINED_RECORDING} into {@code QUARANTINED} and drop the three fields only 0.35.0 writes, turn
 * {@code ACTIVE_MARKED} into {@code ACTIVE}, and leave the records of skipped inputs where they are. A 0.35.0 node that
 * meets such an instance again reads it as one that 0.34.0 quarantined.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreDowngradeTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-downgrade";
    private static final String SKIPPED_COLLECTION = COLLECTION + "_skipped_inputs";
    private static final String STREAM = "orders";
    private static final String RECORDING = "order-recording";
    private static final String RELEASING = "order-releasing";
    private static final String MARKED = "order-marked";

    private static final String REQUARANTINE_FILTER = "{ \"" + SagaCodec.STATUS + "\": \"QUARANTINED_RECORDING\" }";
    private static final String REQUARANTINE_UPDATE = "{ \"$set\": { \"" + SagaCodec.STATUS + "\": \"QUARANTINED\" }, \"$unset\": { \""
            + SagaCodec.FAILURE_QUARANTINE_ID + "\": \"\", \"" + SagaCodec.QUARANTINE_SKIP_COUNT + "\": \"\", \"" + SagaCodec.FAILURE_RELEASED_AT + "\": \"\" } }";
    private static final String ACTIVATE_FILTER = "{ \"" + SagaCodec.STATUS + "\": \"ACTIVE_MARKED\" }";
    private static final String ACTIVATE_UPDATE = "{ \"$set\": { \"" + SagaCodec.STATUS + "\": \"ACTIVE\" } }";

    private static final Set<String> KEYS_THE_UPDATE_TOUCHES = Set.of(SagaCodec.STATUS, SagaCodec.FAILURE_QUARANTINE_ID,
            SagaCodec.QUARANTINE_SKIP_COUNT, SagaCodec.FAILURE_RELEASED_AT);

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item) implements OrderEvent {
    }

    record Pack(String item) {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private MongoOperations mongoOperations;
    private SagaStateStore<String> stateStore;
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private @Nullable SagaSubscription subscription;

    private Document recordingBefore;
    private Document releasingBefore;
    private Document markedBefore;
    private List<Document> skippedBefore;

    @BeforeEach
    void storeThreeInstancesWith_0_35_0_andRunTheDowngrade() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-downgrade-" + UUID.randomUUID()));
        mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);

        SagaSubscription running = run();
        write(new OrderPlaced(RECORDING));
        write(new OrderPlaced(RELEASING));
        write(new OrderPlaced(MARKED));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(Arrays.asList(storedStatus(RECORDING), storedStatus(RELEASING), storedStatus(MARKED)))
                .containsOnly("ACTIVE_MARKED"));

        failing.add("poison-recording");
        failing.add("poison-releasing");
        write(new ItemAdded(RECORDING, "poison-recording"));
        write(new ItemAdded(RELEASING, "poison-releasing"));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(Arrays.asList(storedStatus(RECORDING), storedStatus(RELEASING)))
                .containsOnly("QUARANTINED_RECORDING"));
        write(new ItemAdded(RECORDING, "skipped-1"));
        write(new ItemAdded(RECORDING, "skipped-2"));
        write(new ItemAdded(RELEASING, "skipped-in-release"));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(Arrays.asList(skipCount(RECORDING), skipCount(RELEASING))).as("skip counts of the two quarantined instances").containsExactly(3L, 2L));

        failing.remove("poison-releasing");
        failing.add("skipped-in-release");
        assertThat(running.release(RELEASING)).isEqualTo(SagaSubscription.ReleaseResult.STILL_RELEASING);
        running.close();
        subscriptionModel.cancelSubscription(STREAM);
        subscription = null;

        recordingBefore = stored(RECORDING);
        releasingBefore = stored(RELEASING);
        markedBefore = stored(MARKED);
        skippedBefore = mongoOperations.findAll(Document.class, SKIPPED_COLLECTION);
        assertThat(recordingBefore).containsEntry(SagaCodec.STATUS, "QUARANTINED_RECORDING").containsKeys(SagaCodec.FAILURE_QUARANTINE_ID, SagaCodec.QUARANTINE_SKIP_COUNT);
        assertThat(releasingBefore).containsEntry(SagaCodec.STATUS, "QUARANTINED_RECORDING").containsKeys(SagaCodec.FAILURE_QUARANTINE_ID, SagaCodec.FAILURE_RELEASED_AT);
        assertThat(markedBefore).containsEntry(SagaCodec.STATUS, "ACTIVE_MARKED");
        assertThat(skippedBefore).hasSize(4);

        runTheDowngradeCommands();
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    private void runTheDowngradeCommands() {
        var collection = mongoOperations.getCollection(COLLECTION);
        collection.updateMany(Document.parse(REQUARANTINE_FILTER), Document.parse(REQUARANTINE_UPDATE));
        collection.updateMany(Document.parse(ACTIVATE_FILTER), Document.parse(ACTIVATE_UPDATE));
    }

    private Saga<OrderEvent, String, Pack> saga() {
        return Saga.<OrderEvent, String, Pack>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .evolve(ItemAdded.class, (state, e) -> "packing")
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.item())));
                })
                .build();
    }

    private SagaSubscription run() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run(STREAM, saga(), stateStore, dispatched::add, null,
                        SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        return subscription;
    }

    private void write(OrderEvent event) {
        eventStore.write(event.orderId(), converter.toCloudEvents(List.of(event)));
    }

    private @Nullable Document storedOrNull(String sagaId) {
        return mongoOperations.findOne(Query.query(where("_id").is(sagaId)), Document.class, COLLECTION);
    }

    private Document stored(String sagaId) {
        return requireNonNull(storedOrNull(sagaId));
    }

    private @Nullable String storedStatus(String sagaId) {
        Document document = storedOrNull(sagaId);
        return document == null ? null : document.getString(SagaCodec.STATUS);
    }

    private @Nullable Long skipCount(String sagaId) {
        Document document = storedOrNull(sagaId);
        return document == null ? null : document.getLong(SagaCodec.QUARANTINE_SKIP_COUNT);
    }

    private static Document without(Document document, Set<String> keys) {
        Document copy = new Document(document);
        keys.forEach(copy::remove);
        return copy;
    }

    @Test
    void an_instance_quarantined_with_two_recorded_skips_is_stored_QUARANTINED_with_the_three_0_35_0_fields_gone() {
        Document after = stored(RECORDING);

        assertAll(
                () -> assertThat(after.getString(SagaCodec.STATUS)).isEqualTo("QUARANTINED"),
                () -> assertThat(after).doesNotContainKeys(SagaCodec.FAILURE_QUARANTINE_ID, SagaCodec.QUARANTINE_SKIP_COUNT, SagaCodec.FAILURE_RELEASED_AT)
        );
    }

    @Test
    void an_instance_quarantined_with_two_recorded_skips_keeps_every_other_field_as_it_was() {
        assertThat(without(stored(RECORDING), KEYS_THE_UPDATE_TOUCHES)).isEqualTo(without(recordingBefore, KEYS_THE_UPDATE_TOUCHES));
    }

    @Test
    void an_instance_quarantined_and_mid_release_is_stored_QUARANTINED_with_the_three_0_35_0_fields_gone() {
        Document after = stored(RELEASING);

        assertAll(
                () -> assertThat(after.getString(SagaCodec.STATUS)).isEqualTo("QUARANTINED"),
                () -> assertThat(after).doesNotContainKeys(SagaCodec.FAILURE_QUARANTINE_ID, SagaCodec.QUARANTINE_SKIP_COUNT, SagaCodec.FAILURE_RELEASED_AT)
        );
    }

    @Test
    void an_instance_quarantined_and_mid_release_keeps_every_other_field_as_it_was() {
        assertThat(without(stored(RELEASING), KEYS_THE_UPDATE_TOUCHES)).isEqualTo(without(releasingBefore, KEYS_THE_UPDATE_TOUCHES));
    }

    @Test
    void an_ACTIVE_MARKED_instance_is_stored_ACTIVE_and_nothing_else_of_it_changes() {
        Document after = stored(MARKED);

        assertAll(
                () -> assertThat(after.getString(SagaCodec.STATUS)).isEqualTo("ACTIVE"),
                () -> assertThat(without(after, Set.of(SagaCodec.STATUS))).isEqualTo(without(markedBefore, Set.of(SagaCodec.STATUS)))
        );
    }

    @Test
    void the_records_of_skipped_inputs_are_left_where_they_are_unchanged() {
        assertThat(mongoOperations.findAll(Document.class, SKIPPED_COLLECTION)).containsExactlyInAnyOrderElementsOf(skippedBefore);
    }

    @Test
    void find_returns_an_instance_the_update_left_QUARANTINED_with_a_failure_that_has_no_quarantine_id() {
        SagaEnvelope<String> read = SagaEnvelopes.of(stateStore, null).find(RECORDING).orElseThrow();

        assertAll(
                () -> assertThat(read.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(read.failure()).isNotNull(),
                () -> assertThat(requireNonNull(read.failure()).quarantineId()).isNull(),
                () -> assertThat(read.quarantineId()).isNull(),
                () -> assertThat(read.isReleasing()).isFalse()
        );
    }

    @Test
    void throws_IllegalStateException_when_an_instance_the_update_left_QUARANTINED_is_released() {
        SagaSubscription running = run();

        Throwable thrown = catchThrowable(() -> running.release(RECORDING));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("quarantined by 0.34.0")
                .hasMessageContaining("SagaStateStore.delete(\"" + RECORDING + "\")");
    }

    @Test
    void throws_IllegalStateException_when_a_release_of_the_instance_that_was_mid_release_is_called_after_the_update() {
        SagaSubscription running = run();

        assertThatThrownBy(() -> running.release(RELEASING)).isInstanceOf(IllegalStateException.class).hasMessageContaining("quarantined by 0.34.0");
    }

    @Test
    void an_input_delivered_to_an_instance_the_update_left_QUARANTINED_is_skipped_and_writes_no_record() {
        Document updated = stored(RECORDING);
        run();

        write(new ItemAdded(RECORDING, "late"));
        write(new ItemAdded(MARKED, "witness"));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(dispatched)
                .as("the input after it, which a node delivers in order, has been handled")
                .contains(new Pack("witness")));

        assertAll(
                () -> assertThat(dispatched).doesNotContain(new Pack("late")),
                () -> assertThat(stored(RECORDING)).isEqualTo(updated),
                () -> assertThat(mongoOperations.findAll(Document.class, SKIPPED_COLLECTION)).containsExactlyInAnyOrderElementsOf(skippedBefore)
        );
    }
}
