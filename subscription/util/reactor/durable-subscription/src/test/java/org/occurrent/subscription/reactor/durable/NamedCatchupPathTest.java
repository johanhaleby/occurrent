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

package org.occurrent.subscription.reactor.durable;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Drives the real replay-then-delegate path ({@code NamedCatchupSupport.subscribeWithCatchup}) of the
 * {@code Durable(Catchup(Mongo))} composition, which the conformance wirings never reach: their default
 * {@code StartAt} resolves to a change-stream token, so they always subscribe straight to live. Every test here
 * subscribes with {@code StartAt.checkpoint(GlobalCheckpoint.of(0))}, the one start position that runs the
 * bulk-replay-then-handover machinery.
 * <p>
 * Waits are bounded at 20 seconds, the bound the TCK suites argue for container-backed models: long enough for a
 * replica-set change stream to open under CI load, short enough that a hang fails the build rather than stalling it.
 * The replay gate latches are 10 seconds because they only wait for in-process work this test itself releases.
 */
@Testcontainers
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class NamedCatchupPathTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String DATABASE = "namedcatchuppath";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;

    private ReactiveMongoTemplate reactiveMongoTemplate;
    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel mongoModel;
    private ReactorDurableSubscriptionModel durableModel;
    private String eventCollectionName;
    private String checkpointCollectionName;
    private String streamId;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void createCompositionChain() {
        eventCollectionName = "events-" + UUID.randomUUID();
        checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
        streamId = UUID.randomUUID().toString();
        reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, DATABASE);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(timeRepresentation)
                .build();
        eventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        mongoModel = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, timeRepresentation);
        durableModel = new ReactorDurableSubscriptionModel(
                new ReactorCatchupSubscriptionModel(mongoModel, eventStore, Filter.all()),
                new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName));
    }

    @AfterEach
    void shutdownAndClean() {
        durableModel.shutdown();
        // Delete documents rather than dropping: dropping kills a live change stream mid-close.
        reactiveMongoTemplate.remove(new Query(), eventCollectionName).block();
        reactiveMongoTemplate.remove(new Query(), checkpointCollectionName).block();
    }

    @Test
    void history_replays_then_live_delivery_continues() {
        publish("e1", "e2", "e3");
        List<String> delivered = new CopyOnWriteArrayList<>();

        SubscriptionHandle subscription = durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.fromRunnable(() -> delivered.add(event.getId())));
        subscription.waitUntilStarted().block(TIMEOUT);

        assertThat(delivered).startsWith("e1", "e2", "e3");
        publish("e4");
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).containsExactly("e1", "e2", "e3", "e4"));
    }

    @Test
    void an_event_committed_during_replay_arrives_exactly_once() throws Exception {
        publish("e1", "e2", "e3");
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch firstReplayedEventReached = new CountDownLatch(1);
        CountDownLatch midReplayEventPublished = new CountDownLatch(1);

        SubscriptionHandle subscription = durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.fromRunnable(() -> {
                    if (delivered.isEmpty()) {
                        firstReplayedEventReached.countDown();
                        // Hold the (serialized) replay on its first event until the mid-replay write has committed,
                        // so "an event committed during the replay" is deterministic rather than a race.
                        awaitOrFail(midReplayEventPublished);
                    }
                    delivered.add(event.getId());
                }));

        awaitOrFail(firstReplayedEventReached);
        publish("mid-replay");
        midReplayEventPublished.countDown();
        subscription.waitUntilStarted().block(TIMEOUT);

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(delivered).contains("e1", "e2", "e3", "mid-replay"));
        // Exactly once: the handover cache dedupes the replay/live overlap, so no id may appear twice.
        assertThat(delivered).doesNotHaveDuplicates();
    }

    @Test
    void a_subscription_created_while_the_model_is_stopped_does_not_replay_until_start() {
        publish("e1", "e2", "e3");
        List<String> delivered = new CopyOnWriteArrayList<>();
        durableModel.stop();

        durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.fromRunnable(() -> delivered.add(event.getId())));

        // The operator stopped the model, so nothing may be delivered, however long we look. Two seconds is a
        // deliberate observation window, not a wait-for-condition: the broken behavior replays immediately, so a
        // regression fails within milliseconds, while the fixed behavior stays empty for the whole window.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(delivered).isEmpty());

        durableModel.start(true);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).containsExactly("e1", "e2", "e3"));
    }

    @Test
    void stopping_mid_replay_aborts_the_replay_and_start_resumes_it() throws Exception {
        publish("e1", "e2", "e3");
        Map<String, Integer> deliveredCounts = new ConcurrentHashMap<>();
        CountDownLatch firstReplayedEventReached = new CountDownLatch(1);
        CountDownLatch modelStopped = new CountDownLatch(1);
        AtomicBoolean deliveredWhileStopped = new AtomicBoolean(false);

        SubscriptionHandle subscription = durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.fromRunnable(() -> {
                    if (deliveredCounts.isEmpty()) {
                        firstReplayedEventReached.countDown();
                        awaitOrFail(modelStopped);
                    }
                    if (!durableModel.isRunning() && !"e1".equals(event.getId())) {
                        deliveredWhileStopped.set(true);
                    }
                    deliveredCounts.merge(event.getId(), 1, Integer::sum);
                }));

        awaitOrFail(firstReplayedEventReached);
        durableModel.stop();
        modelStopped.countDown();

        // The stop aborts the replay: no further event may be delivered while the model is stopped, and the
        // subscription's started signal must not be poisoned (the pre-fix behavior handed over into the stopped
        // wrapped model and errored waitUntilStarted with "already paused").
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(deliveredWhileStopped).isFalse());

        durableModel.start(true);
        subscription.waitUntilStarted().block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(deliveredCounts).containsKeys("e1", "e2", "e3"));
        publish("live-after-restart");
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(deliveredCounts).containsKey("live-after-restart"));
    }

    @Test
    void a_duplicate_subscription_id_on_the_catchup_path_is_refused_synchronously() {
        publish("e1");
        SubscriptionHandle subscription = durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.empty());
        subscription.waitUntilStarted().block(TIMEOUT);

        // The id now lives in the wrapped model. A second catch-up subscribe with the same id must be refused
        // synchronously, like every other subscribe path, not replay history again and fail asynchronously.
        assertThatThrownBy(() -> durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> Mono.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(streamId);
    }

    @Test
    void cancelling_mid_replay_leaves_the_wrapped_model_untouched_and_fails_the_started_signal() throws Exception {
        publish("e1", "e2", "e3");
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch firstReplayedEventReached = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);

        SubscriptionHandle subscription = durableModel.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)),
                event -> Mono.fromRunnable(() -> {
                    delivered.add(event.getId());
                    if (delivered.size() == 1) {
                        firstReplayedEventReached.countDown();
                        awaitOrFail(cancelled);
                    }
                }));

        awaitOrFail(firstReplayedEventReached);
        durableModel.cancelSubscription(streamId);
        cancelled.countDown();

        // The id never reached the wrapped model, so it must not know it; and per the blocking contract
        // (CancelledSubscription answers false), the started signal errors rather than completing, since nothing here
        // ever started, and completing would have claimed otherwise.
        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(streamId)
                .hasMessageContaining("was cancelled before it started");
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(mongoModel.subscriptionIds()).doesNotContain(streamId));
    }

    @Test
    void a_cold_only_composition_refuses_named_subscribe_but_its_life_cycle_is_safe() {
        ColdOnlyCheckpointAwareModel coldOnly = new ColdOnlyCheckpointAwareModel();
        ReactorDurableSubscriptionModel durableOverColdOnly = new ReactorDurableSubscriptionModel(
                new ReactorCatchupSubscriptionModel(coldOnly, eventStore, Filter.all()),
                new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName));

        assertThatThrownBy(() -> durableOverColdOnly.subscribe(streamId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> Mono.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("named subscriptions");

        // Model-wide life-cycle calls are what a Spring context close and a health check invoke; they must be safe
        // no-ops on a composition that never could subscribe by name, not late IllegalStateExceptions.
        assertThat(durableOverColdOnly.isRunning()).isFalse();
        durableOverColdOnly.stop();
        durableOverColdOnly.start(true);
        durableOverColdOnly.shutdown();
    }

    private void publish(String... eventIds) {
        List<CloudEvent> events = java.util.Arrays.stream(eventIds)
                .map(id -> CloudEventBuilder.v1()
                        .withId(id)
                        .withSource(URI.create("urn:test"))
                        .withType("test-event")
                        .withTime(OffsetDateTime.now())
                        .build())
                .toList();
        eventStore.write(streamId, Flux.fromIterable(events)).block();
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            if (!latch.await(10, SECONDS)) {
                throw new AssertionError("Gate latch was not released within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting on a gate latch", e);
        }
    }

    /**
     * A checkpoint-aware model that only offers the plain {@code subscribe(filter, startAt)} primitive: exactly the
     * shape migration guide section 6 is about.
     */
    private static final class ColdOnlyCheckpointAwareModel implements CheckpointAwareSubscriptionModel {
        @Override
        public Flux<CloudEvent> subscribe(org.occurrent.subscription.SubscriptionFilter filter, StartAt startAt) {
            return Flux.never();
        }

        @Override
        public Mono<org.occurrent.subscription.Checkpoint> globalCheckpoint() {
            return Mono.empty();
        }
    }
}
