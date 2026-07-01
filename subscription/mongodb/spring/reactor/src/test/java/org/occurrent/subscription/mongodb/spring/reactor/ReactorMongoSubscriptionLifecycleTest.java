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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
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
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                    .withReplicaSet()
                    .withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivelifecycle"));

    private MongoClient mongoClient;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;
    private ObjectMapper objectMapper;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivelifecycle");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
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
        StartAt beforeWrite = StartAt.subscriptionPosition(subscriptionModel.globalSubscriptionPosition().block());
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
        Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> Mono.empty());

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
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void pausing_a_subscription_stops_delivery_and_resuming_continues_without_replay() {
        // Given: an explicit position from before the write, since waitUntilStarted() only signals that the change
        // stream Flux was subscribed to, not that the server has acknowledged the command and the cursor is
        // positioned, so a write right after it could otherwise land before the cursor is actually watching.
        LocalDateTime now = LocalDateTime.now();
        StartAt beforeWrite = StartAt.subscriptionPosition(subscriptionModel.globalSubscriptionPosition().block());
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
    void cancelling_a_subscription_forgets_it_and_stops_delivery() {
        // Given: an explicit position from before the write, since waitUntilStarted() only signals that the change
        // stream Flux was subscribed to, not that the server has acknowledged the command and the cursor is
        // positioned, so a write right after it could otherwise land before the cursor is actually watching.
        LocalDateTime now = LocalDateTime.now();
        StartAt beforeWrite = StartAt.subscriptionPosition(subscriptionModel.globalSubscriptionPosition().block());
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
    void a_subscription_that_terminates_with_an_unrecoverable_error_is_removed_from_running_and_can_be_resubscribed() {
        // Given: the action itself throws, which concatMap surfaces as a terminal error the outer retryWhen never
        // sees, since it only covers the change stream, not the action. An explicit position from before the write
        // is used, since waitUntilStarted() only signals that the change stream Flux was subscribed to, not that the
        // server has acknowledged the command and the cursor is positioned, so a write right after it could
        // otherwise land before the cursor is actually watching.
        String subscriptionId = UUID.randomUUID().toString();
        StartAt beforeWrite = StartAt.subscriptionPosition(subscriptionModel.globalSubscriptionPosition().block());
        subscriptionModel.subscribe(subscriptionId, beforeWrite, __ -> {
            throw new RuntimeException("boom");
        }).waitUntilStarted().block(Duration.ofSeconds(10));

        // When
        mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1"))).block();

        // Then: the dead subscription is no longer tracked as running or paused
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse());
        assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();

        // A new subscription can reuse the same id since the dead one is forgotten
        subscriptionModel.subscribe(subscriptionId, __ -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(10));
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
        Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> Mono.empty());

        // Then: it never actually starts while paused, so waitUntilStarted() on this handle never completes,
        // unlike before the fix where it misleadingly completed via doOnSubscribe just before being disposed.
        StepVerifier.create(subscription.waitUntilStarted())
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(500))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
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
