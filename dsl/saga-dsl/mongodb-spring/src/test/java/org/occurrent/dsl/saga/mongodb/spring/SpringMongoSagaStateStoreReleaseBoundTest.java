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
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A release keeps every version it applies in the stream marks of the instance, and stops at
 * {@link SagaExecutionSupport#MAX_APPLIED_VERSIONS_IN_A_RELEASE} of them by quarantining the instance again. Checks
 * against a MongoDB replica set that an instance at that bound still saves and reads back, which shows the bound fits
 * the document size MongoDB allows, and that a release of it ends QUARANTINED_AGAIN. The marks are seeded through the
 * state store, since that many versions cannot be produced by a real flow.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(300)
class SpringMongoSagaStateStoreReleaseBoundTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final int BOUND = SagaExecutionSupport.MAX_APPLIED_VERSIONS_IN_A_RELEASE;
    private static final String COLLECTION = "saga-release-bound";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final String SEEDED_TYPE = "seeded";

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item, int quantity) implements OrderEvent {
    }

    record Pack(String item, int quantity) {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private SpringMongoSagaStateStore<String> stateStore;
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-bound-" + UUID.randomUUID()));
        stateStore = new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase())), COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
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
                    return List.of(SagaEffect.issue(new Pack(e.item(), e.quantity())));
                })
                .build();
    }

    private void write(OrderEvent event) {
        eventStore.write(STREAM, converter.toCloudEvents(List.of(event)));
    }

    private SagaEnvelope<String> instance() {
        return SagaEnvelopes.of(stateStore, null).find(ORDER).orElseThrow();
    }

    // Adds applied versions of an event type no saga event has, so the instance keeps exactly the count asked for
    private void keepAppliedVersions(long count) {
        SagaEnvelopes<String> envelopes = SagaEnvelopes.of(stateStore, null);
        SagaEnvelope<String> current = instance();
        long extra = count - SagaExecutionSupport.appliedVersions(current);
        List<Long> applied = new ArrayList<>();
        for (long version = 0; version < extra; version++) {
            applied.add(1_000_000L + version);
        }
        StreamMarks existing = current.streamMarks().get(STREAM);
        Map<String, TypeMark> byType = new HashMap<>(existing.byType());
        byType.put(SEEDED_TYPE, new TypeMark(1_000_000L + extra, applied, 0));
        Map<String, StreamMarks> marks = new HashMap<>(current.streamMarks());
        marks.put(STREAM, new StreamMarks(existing.floor(), byType, existing.pending(), existing.legacyFailed(), existing.pendingSince()));
        SagaEnvelope<String> seeded = new SagaEnvelope<>(current.sagaId(), current.state(), current.status(), current.version(), current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), marks, current.quarantineSkipCount());
        assertThat(envelopes.compareAndSave(ORDER, seeded, current.version())).isTrue();
    }

    @Test
    void an_instance_with_as_many_applied_versions_as_a_release_may_keep_saves_and_reads_back_and_a_release_of_it_ends_quarantined_again_with_its_marks_trimmed() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        write(new ItemAdded(ORDER, "book", 2));
        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "pen", 5));
        await().ignoreExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(stateStore.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE)).hasSize(2));
        keepAppliedVersions(BOUND);
        long readBack = SagaExecutionSupport.appliedVersions(instance());
        failing.clear();
        dispatched.clear();

        ReleaseResult result = subscription.release(ORDER);

        SagaEnvelope<String> after = instance();
        assertAll(
                () -> assertThat(readBack).as("the instance at the bound read back whole").isEqualTo(BOUND),
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(dispatched).as("the held event is not applied").isEmpty(),
                () -> assertThat(SagaExecutionSupport.appliedVersions(after)).as("the marks are trimmed").isLessThan(1_000),
                () -> assertThat(stateStore.findSkippedInputs(ORDER, after.quarantineId(), Integer.MAX_VALUE)).as("both held events stay recorded").hasSize(2)
        );
    }
}
