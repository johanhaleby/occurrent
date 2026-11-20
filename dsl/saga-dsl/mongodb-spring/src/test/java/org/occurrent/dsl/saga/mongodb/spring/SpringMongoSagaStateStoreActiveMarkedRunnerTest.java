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

import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
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
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. An input that carries a stream version leaves marks on the instance, so the saga runner leaves an
 * active instance stored as {@code ACTIVE_MARKED}. The runner's timer poller still fires the timers of such an
 * instance, and a release that ends a quarantine leaves the instance {@code ACTIVE_MARKED} again, because it still
 * holds its marks.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreActiveMarkedRunnerTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-active-marked-runner";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item) implements OrderEvent {
    }

    sealed interface OrderCommand permits Pack, Remind {
    }

    record Pack(String item) implements OrderCommand {
    }

    record Remind(String orderId) implements OrderCommand {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private MongoOperations mongoOperations;
    private SagaStateStore<String> stateStore;
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private @Nullable SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-am-" + UUID.randomUUID()));
        mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    private Saga<OrderEvent, String, OrderCommand> saga() {
        return Saga.<OrderEvent, String, OrderCommand>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.startTimeout("reminder", Duration.ofMillis(100))))
                .evolve(ItemAdded.class, (state, e) -> "packing")
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.item())));
                })
                .reactOnTimeout("reminder", (state, timeout) -> List.of(SagaEffect.issue(new Remind(timeout.sagaId()))))
                .build();
    }

    /**
     * {@link InMemorySubscriptionModel} does not implement {@link HistoryRetainingSubscriptions}, and
     * {@code SagaRunner} switches quarantine off for a subscription model it cannot ask. Wrapping it with an
     * always-retains answer is what makes the quarantine and the release reachable.
     */
    private record RetainsEverything(InMemorySubscriptionModel delegate) implements Subscribable, HistoryRetainingSubscriptions {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    private SagaSubscription run(SagaRunnerConfig config) {
        subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(new RetainsEverything(subscriptionModel), converter)
                .run(STREAM, saga(), stateStore, dispatched::add, null, config);
        return subscription;
    }

    private void write(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
    }

    private @Nullable String storedStatus() {
        Document document = mongoOperations.findOne(Query.query(where("_id").is(ORDER)), Document.class, COLLECTION);
        return document == null ? null : document.getString("status");
    }

    private static final long LEGACY_VERSION = 3L;

    // The document a 0.34.0 node stored for an active instance, which has no stream marks
    private static Document activeBy034() {
        return new Document("_id", ORDER).append("status", "ACTIVE").append("version", LEGACY_VERSION)
                .append("state", "placed").append("timers", List.of()).append("started", true)
                .append("createdAt", 1_000L).append("updatedAt", 2_000L).append("streamWatermarks", new Document());
    }

    // What a 0.34.0 node does with an instance it read at LEGACY_VERSION: replaces the whole document if the version still matches
    private @Nullable Document replaceAsOf034() {
        return mongoOperations.findAndReplace(Query.query(where("_id").is(ORDER).and("version").is(LEGACY_VERSION)), activeBy034(), COLLECTION);
    }

    private void withoutRedelivery() {
        subscriptionModel.shutdown();
        subscriptionModel = new InMemorySubscriptionModel(RetryStrategy.none());
        eventStore = new InMemoryEventStore(subscriptionModel);
    }

    @Test
    void the_first_marks_an_event_that_fails_gives_a_0_34_0_instance_move_its_version_so_a_0_34_0_replace_of_the_old_version_matches_nothing() {
        withoutRedelivery();
        mongoOperations.insert(activeBy034(), COLLECTION);
        failing.add("book");
        run(SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)));

        write(new ItemAdded(ORDER, "book"));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(storedStatus())
                .as("the failed event is recorded as pending, which gives the instance its first marks")
                .isEqualTo("ACTIVE_MARKED"));
        assertThat(replaceAsOf034()).as("a 0.34.0 replace of the version the instance had before it got marks matches nothing").isNull();
        assertThat(SagaEnvelopes.of(stateStore, null).find(ORDER).orElseThrow().streamMarks()).containsKey(ORDER);
        assertThat(storedStatus()).isEqualTo("ACTIVE_MARKED");
    }

    @Test
    void the_first_marks_an_event_that_applies_gives_a_0_34_0_instance_move_its_version_so_a_0_34_0_replace_of_the_old_version_matches_nothing() {
        withoutRedelivery();
        mongoOperations.insert(activeBy034(), COLLECTION);
        run(SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)));

        write(new ItemAdded(ORDER, "book"));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(storedStatus())
                .as("the applied event gives the instance its first marks")
                .isEqualTo("ACTIVE_MARKED"));
        assertThat(replaceAsOf034()).as("a 0.34.0 replace of the version the instance had before it got marks matches nothing").isNull();
        assertThat(SagaEnvelopes.of(stateStore, null).find(ORDER).orElseThrow().streamMarks()).containsKey(ORDER);
        assertThat(storedStatus()).isEqualTo("ACTIVE_MARKED");
    }

    @Test
    void an_event_with_a_stream_version_leaves_the_stored_instance_ACTIVE_MARKED() {
        run(SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)));

        write(new OrderPlaced(ORDER));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(storedStatus())
                .as("an ACTIVE instance that applied an input with a stream version holds marks, so it is stored as ACTIVE_MARKED")
                .isEqualTo("ACTIVE_MARKED"));
        assertThat(SagaEnvelopes.of(stateStore, null).find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE);
    }

    @Test
    void a_due_timer_on_an_ACTIVE_MARKED_instance_fires() {
        run(SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50)));

        write(new OrderPlaced(ORDER));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(dispatched)
                .as("the reminder timer of an instance stored as ACTIVE_MARKED is found by the due-timer poll and fires")
                .containsExactly(new Remind(ORDER)));
        assertThat(storedStatus()).isEqualTo("ACTIVE_MARKED");
    }

    @Test
    void a_quarantined_instance_that_a_release_brings_back_is_stored_ACTIVE_MARKED_again_because_it_still_holds_its_marks() {
        SagaSubscription running = run(SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(storedStatus()).isEqualTo("ACTIVE_MARKED"));
        failing.add("book");

        write(new ItemAdded(ORDER, "book"));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(storedStatus())
                .as("the instance is quarantined on the input that keeps failing, and records the inputs it skips")
                .isEqualTo("QUARANTINED_RECORDING"));
        failing.clear();

        running.release(ORDER);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(storedStatus())
                .as("the release replays the input it was quarantined on and ends the quarantine, and the instance still holds its marks")
                .isEqualTo("ACTIVE_MARKED"));
        assertThat(dispatched).contains(new Pack("book"));
    }
}
