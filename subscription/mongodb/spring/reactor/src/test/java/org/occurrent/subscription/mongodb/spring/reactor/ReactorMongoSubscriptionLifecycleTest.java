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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Tests the named, lifecycle-managed subscriptions ({@link org.occurrent.subscription.api.reactor.Subscribable},
 * {@link org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle}) that {@link ReactorMongoSubscriptionModel}
 * adds on top of the plain {@link Flux} primitive. Mirrors {@code NativeMongoSubscriptionModelTest}'s
 * {@code LifeCycleTest}.
 */
@Testcontainers
@Timeout(20)
public class ReactorMongoSubscriptionLifecycleTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private ObjectMapper objectMapper;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivelifecycle");
        mongoClient = MongoClients.create(connectionString);
        reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        subscriptionModel = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void named_subscription_delivers_events_to_the_action() {
        // Given: an explicit position from before the write, since waitUntilStarted() only signals that the change
        // stream Flux was subscribed to, not that the server has acknowledged the command and the cursor is
        // positioned, so a write right after it could otherwise land before the cursor is actually watching.
        LocalDateTime now = LocalDateTime.now();
        StartAt beforeWrite = StartAt.checkpoint(subscriptionModel.globalCheckpoint().block());
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), beforeWrite, cloudEvent -> {
            state.add(cloudEvent);
            return Mono.empty();
        }).waitUntilStarted().block(Duration.ofSeconds(10));

        // When
        mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();

        // Then
        await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
    }

    @Test
    void wait_until_started_with_a_timeout_throws_npe_when_timeout_is_null() {
        // Given
        SubscriptionHandle subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> Mono.empty());

        // When
        Throwable throwable = catchThrowable(() -> subscription.waitUntilStarted(null));

        // Then
        assertThat(throwable).isInstanceOf(NullPointerException.class).hasMessageContaining("timeout");
    }

    @Test
    void subscribing_twice_with_the_same_id_throws_iae() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, __ -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(10));

        // When
        Throwable throwable = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, __ -> Mono.empty()));

        // Then
        assertThat(throwable).isExactlyInstanceOf(DuplicateSubscriptionIdException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void subscribing_with_a_start_position_the_model_cannot_parse_is_refused_by_subscribe_itself() {
        // Given: a checkpoint whose string form contains "resumeToken" (steering MongoCommons.applyStartPosition
        // into its legacy string-parsing branch) but isn't valid BSON, so parsing it fails. Before subscribe made
        // this eager check, the same failure only happened later, inside the Flux.defer built by
        // resilientChangeStream/changeStream: shouldRestart sent it round the unbounded retry forever, so
        // waitUntilStarted() never completed and isRunning(id) kept saying yes for a subscription that would
        // never deliver anything.
        String subscriptionId = UUID.randomUUID().toString();
        StartAt unparsableStartAt = StartAt.checkpoint(new StringBasedCheckpoint("not-a-valid-resumeToken-document"));

        // When
        Throwable throwable = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, unparsableStartAt, __ -> Mono.empty()));

        // Then: refused synchronously by subscribe() itself, and nothing is left registered under the id
        assertThat(throwable).isExactlyInstanceOf(JsonParseException.class);
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
    }

    @Test
    void pausing_a_subscription_stops_delivery_and_resuming_continues_without_replay() {
        // Given: an explicit position from before the write, since waitUntilStarted() only signals that the change
        // stream Flux was subscribed to, not that the server has acknowledged the command and the cursor is
        // positioned, so a write right after it could otherwise land before the cursor is actually watching.
        LocalDateTime now = LocalDateTime.now();
        StartAt beforeWrite = StartAt.checkpoint(subscriptionModel.globalCheckpoint().block());
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, beforeWrite, cloudEvent -> {
            state.add(cloudEvent);
            return Mono.empty();
        }).waitUntilStarted().block(Duration.ofSeconds(10));

        mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();
        await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

        // When
        subscriptionModel.pauseSubscription(subscriptionId);
        mongoEventStore.write("1", 1, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2"))).block();

        // Then: nothing delivered while paused
        assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();

        // When: resumed
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted().block(Duration.ofSeconds(10));

        // Then: the event written while paused is delivered exactly once, no replay of the first event
        await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));
        assertThat(state).extracting(CloudEvent::getId).doesNotHaveDuplicates();
    }

    @Test
    void pausing_while_events_are_buffered_ahead_of_a_slow_action_does_not_lose_them_on_resume() {
        // Given: the action blocks indefinitely on its first call, so the change stream underneath can keep
        // reading (and, without the fix, keep advancing the tracked position for) further written events while
        // that first call is still pending and nothing has reached the action a second time yet.
        StartAt beforeWrite = StartAt.checkpoint(subscriptionModel.globalCheckpoint().block());
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        Sinks.Empty<Void> releaseFirstAction = Sinks.empty();
        AtomicInteger actionCallCount = new AtomicInteger();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, beforeWrite, cloudEvent -> {
            if (actionCallCount.getAndIncrement() == 0) {
                return releaseFirstAction.asMono();
            }
            state.add(cloudEvent);
            return Mono.empty();
        }).waitUntilStarted().block(Duration.ofSeconds(10));

        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 5; i++) {
            mongoEventStore.write(UUID.randomUUID().toString(), 0, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(i), "name", "name" + i))).block();
        }

        // When: give the change stream time to read ahead of the still-blocked first action call, then pause,
        // discarding whatever it read, and resume.
        Mono.delay(Duration.of(300, MILLIS)).block();
        subscriptionModel.pauseSubscription(subscriptionId);
        releaseFirstAction.tryEmitEmpty();

        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted().block(Duration.ofSeconds(10));

        // Then: none of the 5 events are lost, even though they were read out of the change stream well before
        // the blocked first action call ever completed.
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(state).hasSize(5));
    }

    @Test
    void cancelling_a_subscription_forgets_it_and_stops_delivery() {
        // Given: an explicit position from before the write, since waitUntilStarted() only signals that the change
        // stream Flux was subscribed to, not that the server has acknowledged the command and the cursor is
        // positioned, so a write right after it could otherwise land before the cursor is actually watching.
        LocalDateTime now = LocalDateTime.now();
        StartAt beforeWrite = StartAt.checkpoint(subscriptionModel.globalCheckpoint().block());
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, beforeWrite, cloudEvent -> {
            state.add(cloudEvent);
            return Mono.empty();
        }).waitUntilStarted().block(Duration.ofSeconds(10));

        mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();
        await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

        // When
        subscriptionModel.cancelSubscription(subscriptionId);

        // Then
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();

        // A new subscription can reuse the same id since the old one is forgotten
        subscriptionModel.subscribe(subscriptionId, __ -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(10));
    }

    @Test
    void a_subscription_whose_action_fails_is_retried_and_stays_running() {
        // Given: the action throws on the first delivery only. The failure is retried with the model's backoff (the
        // reactor counterpart of the blocking models' RetryStrategy around the handler), so one bad delivery must not
        // end the subscription. An explicit position from before the write is used, since waitUntilStarted() only
        // signals that the change stream Flux was subscribed to, not that the server has acknowledged the command and
        // the cursor is positioned, so a write right after it could otherwise land before the cursor is actually
        // watching.
        String subscriptionId = UUID.randomUUID().toString();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch delivered = new CountDownLatch(1);
        StartAt beforeWrite = StartAt.checkpoint(subscriptionModel.globalCheckpoint().block());
        subscriptionModel.subscribe(subscriptionId, beforeWrite, __ -> {
            if (deliveries.incrementAndGet() == 1) {
                throw new RuntimeException("boom, once");
            }
            delivered.countDown();
            return Mono.empty();
        }).waitUntilStarted().block(Duration.ofSeconds(10));

        // When
        mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1"))).block();

        // Then: the event reaches the action on a later attempt, and the subscription is still tracked as running,
        // so its id cannot be taken by a second subscribe
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered.getCount()).isZero());
        assertThat(deliveries.get()).isGreaterThanOrEqualTo(2);
        assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
        assertThatThrownBy(() -> subscriptionModel.subscribe(subscriptionId, __ -> Mono.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pausing_a_subscription_mid_retry_backoff_cancels_the_pending_retry() {
        // Given: a model whose retry backoff is long (1 s) so the pause below always lands inside the backoff window
        // rather than racing the retry it means to cancel. Pause disposes the subscription's pipeline, which is what
        // cancels a scheduled retry, and nothing else pins that: a pause that merely stopped new deliveries would
        // leave the retry timer live and the action would run again while "paused".
        ReactorMongoSubscriptionModel slowRetry = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events",
                TimeRepresentation.RFC_3339_STRING, new ReactorMongoSubscriptionModelConfig().backoff(Duration.ofSeconds(1), Duration.ofSeconds(2)));
        try {
            String subscriptionId = UUID.randomUUID().toString();
            AtomicInteger attempts = new AtomicInteger();
            StartAt beforeWrite = StartAt.checkpoint(slowRetry.globalCheckpoint().block());
            slowRetry.subscribe(subscriptionId, beforeWrite, __ -> {
                attempts.incrementAndGet();
                return Mono.error(new RuntimeException("always failing"));
            }).waitUntilStarted().block(Duration.ofSeconds(10));

            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1"))).block();
            await().atMost(10, SECONDS).untilAsserted(() -> assertThat(attempts.get()).isGreaterThanOrEqualTo(1));

            // When: pause within the 1 s backoff window that opened when the first attempt failed.
            slowRetry.pauseSubscription(subscriptionId);

            // Then: the pending retry never fires. The observation window is 2.5 s, past both the 1 s first retry
            // and the 2 s second, so a retry the pause failed to cancel would be observed. A fixed window rather
            // than a condition wait, because "nothing happens" has no condition to await.
            Mono.delay(Duration.ofMillis(2500)).block();
            assertThat(attempts).hasValue(1);
            assertThat(slowRetry.isPaused(subscriptionId)).isTrue();
        } finally {
            slowRetry.shutdown();
        }
    }

    @Test
    void shutdown_disposes_all_running_and_paused_subscriptions() {
        // Given
        String runningId = UUID.randomUUID().toString();
        String pausedId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(runningId, __ -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(10));
        subscriptionModel.subscribe(pausedId, __ -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(pausedId);

        // When
        subscriptionModel.shutdown();

        // Then
        assertThat(subscriptionModel.isRunning(runningId)).isFalse();
        assertThat(subscriptionModel.isPaused(pausedId)).isFalse();
    }

    @Test
    void a_named_subscription_created_while_the_model_is_stopped_does_not_deliver_events_until_started() {
        // Given
        subscriptionModel.stop();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, cloudEvent -> {
            state.add(cloudEvent);
            return Mono.empty();
        });

        // Then: it's tracked as paused, not running, and an event written while stopped is not delivered
        assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1"))).block();
        await().atMost(Duration.ofSeconds(1)).during(Duration.ofMillis(500)).untilAsserted(() -> assertThat(state).isEmpty());

        // When
        subscriptionModel.start();
        mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2"))).block();

        // Then: exactly one delivery. If the subscription created while stopped had stayed live underneath instead
        // of being disposed, this event would be delivered twice: once on that leaked subscription and once on the
        // one resumeSubscription starts here.
        await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        assertThat(state).extracting(CloudEvent::getId).doesNotHaveDuplicates();
    }

    @Test
    void wait_until_started_does_not_complete_for_a_subscription_created_while_the_model_is_stopped() {
        // Given
        subscriptionModel.stop();
        SubscriptionHandle subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> Mono.empty());

        // Then: it never actually starts while paused, so waitUntilStarted() on this handle never completes,
        // unlike before the fix where it misleadingly completed via doOnSubscribe just before being disposed.
        StepVerifier.create(subscription.waitUntilStarted())
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(500))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    /**
     * The subscription ids this model reports. It keeps running and paused subscriptions in two separate maps, so the
     * answer is their union, and a subscription registered while the model was stopped is only in the paused one.
     */
    @Nested
    class ListingItsSubscriptions {

        @Test
        void knows_nothing_before_anything_subscribes() {
            assertThat(subscriptionModel.subscriptionIds()).isEmpty();
        }

        @Test
        void knows_a_running_subscription() {
            subscriptionModel.subscribe("someSubscription", __ -> Mono.empty());

            assertThat(subscriptionModel.isRunning("someSubscription")).isTrue();
            assertThat(subscriptionModel.subscriptionIds()).containsExactly("someSubscription");
        }

        @Test
        void knows_a_paused_subscription_too() {
            subscriptionModel.subscribe("someSubscription", __ -> Mono.empty());

            subscriptionModel.pauseSubscription("someSubscription");

            assertThat(subscriptionModel.isPaused("someSubscription")).isTrue();
            assertThat(subscriptionModel.subscriptionIds()).containsExactly("someSubscription");
        }

        @Test
        void knows_a_subscription_registered_while_the_model_was_stopped() {
            subscriptionModel.stop();

            subscriptionModel.subscribe("someSubscription", __ -> Mono.empty());

            // Registering on a stopped model records it as paused, so a model answering from the running map alone
            // would report nothing for a subscription that exists and will deliver once started.
            assertThat(subscriptionModel.isPaused("someSubscription")).isTrue();
            assertThat(subscriptionModel.subscriptionIds()).containsExactly("someSubscription");
        }

        @Test
        void forgets_a_cancelled_subscription() {
            subscriptionModel.subscribe("someSubscription", __ -> Mono.empty());

            subscriptionModel.cancelSubscription("someSubscription");

            assertThat(subscriptionModel.subscriptionIds()).isEmpty();
        }

        @Test
        void knows_running_and_paused_subscriptions_together() {
            subscriptionModel.subscribe("running", __ -> Mono.empty());
            subscriptionModel.subscribe("paused", __ -> Mono.empty());

            subscriptionModel.pauseSubscription("paused");

            assertThat(subscriptionModel.subscriptionIds()).containsExactlyInAnyOrder("running", "paused");
        }

        @Test
        void answers_a_copy_rather_than_the_maps_it_keeps() {
            subscriptionModel.subscribe("first", __ -> Mono.empty());
            Set<String> ids = subscriptionModel.subscriptionIds();

            subscriptionModel.subscribe("second", __ -> Mono.empty());

            assertThat(ids).containsExactly("first");
            assertThat(subscriptionModel.subscriptionIds()).containsExactlyInAnyOrder("first", "second");
        }
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}
