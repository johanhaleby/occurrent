/*
 * Copyright 2021 Johan Haleby
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
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.groups.Tuple.tuple;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Timeout(20)
@DisplayNameGeneration(DisplayNameGenerator.Simple.class)
@Testcontainers
public class ReactorMongoSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true);

    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private CopyOnWriteArrayList<Disposable> disposables;

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));
    private MongoClient mongoClient;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, Objects.requireNonNull(connectionString.getDatabase()));
        // A collection of its own for every test, since a subscription started at the present can also receive what
        // was written shortly before it, which would otherwise include the previous test's events
        String eventCollection = "events-" + UUID.randomUUID();
        subscription = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollection, TimeRepresentation.RFC_3339_STRING);
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection).transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @Test
    void reactive_spring_subscription_calls_listener_for_each_new_event() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        disposeAfterTest(subscription.subscribe().flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent))).subscribe());
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Nested
    @DisplayName("SubscriptionFilter for BsonMongoDBFilterSpecification")
    class MongoBsonFilterSpecificationTest {
        @Test
        void using_bson_query_for_type_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscription.subscribe(MongoFilterSpecification.MongoBsonFilterSpecification.filter().type(Filters::eq, NameDefined.class.getSimpleName()))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();
            Thread.sleep(200);
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name", "name4");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getSimpleName());
        }

        @Test
        void using_bson_query_dsl_composition_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name", "name4");

            subscription.subscribe(MongoFilterSpecification.MongoBsonFilterSpecification.filter().id(Filters::eq, nameDefined2.eventId()).and().type(Filters::eq, NameDefined.class.getSimpleName()))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getSimpleName()));
        }

        @Test
        void using_bson_query_native_mongo_filters_composition_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name", "name4");

            subscription.subscribe(MongoFilterSpecification.MongoBsonFilterSpecification.filter(match(and(eq("fullDocument.id", nameDefined2.eventId()), eq("fullDocument.type", NameDefined.class.getSimpleName())))))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getSimpleName()));
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter for JsonMongoDBFilterSpecification")
    class MongoJsonFilterSpecificationTest {
        @Test
        void using_json_query_for_type_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscription.subscribe(MongoFilterSpecification.MongoJsonFilterSpecification.filter("{ $match : { \"" + MongoFilterSpecification.FULL_DOCUMENT + ".type\" : \"" + NameDefined.class.getSimpleName() + "\" } }"))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name", "name4");

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getSimpleName());
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter using StreamSubscriptionFilter")
    class StreamSubscriptionFilterTest {

        @Test
        void using_occurrent_subscription_filter_for_type() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

            subscription.subscribe(StreamSubscriptionFilter.filter(Filter.type(NameDefined.class.getSimpleName())))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name", "name4");

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getSimpleName());
        }

        @Test
        void using_occurrent_subscription_filter_dsl_composition() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name", "name4");

            Filter filter = Filter.id(nameDefined2.eventId()).and(Filter.type(NameDefined.class.getSimpleName()));
            subscription.subscribe(StreamSubscriptionFilter.filter(filter))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getSimpleName()));
        }
    }

    @Nested
    @DisplayName("Subscribe at a catch-up position")
    class SubscribeAtACatchupPositionTest {

        @AfterEach
        void shutdown_subscription_model() {
            subscription.shutdown();
        }

        @Test
        void subscribing_at_a_global_checkpoint_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(7));
        }

        @Test
        void subscribing_at_a_global_checkpoint_with_a_live_start_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(5, liveStart(), 0, 7));
        }

        @Test
        void subscribing_at_a_global_checkpoint_read_back_as_a_string_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeIsRefusedAndLeavesTheIdFree(new StringBasedCheckpoint(GlobalCheckpoint.of(5, liveStart(), 0, 7).asString()));
        }

        @Test
        void subscribing_at_a_catchup_time_checkpoint_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeIsRefusedAndLeavesTheIdFree(catchupTimeCheckpoint());
        }

        @Test
        void subscribing_at_a_catchup_time_checkpoint_read_back_as_a_string_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeIsRefusedAndLeavesTheIdFree(new StringBasedCheckpoint(catchupTimeCheckpoint().asString()));
        }

        @Test
        void the_flux_subscribed_at_a_global_checkpoint_fails_when_subscribed_to() {
            assertFluxFailsWhenSubscribedTo(GlobalCheckpoint.of(7), false);
        }

        @Test
        void the_flux_subscribed_at_a_global_checkpoint_read_back_as_a_string_fails_when_subscribed_to() {
            assertFluxFailsWhenSubscribedTo(new StringBasedCheckpoint(GlobalCheckpoint.of(5, liveStart(), 0, 7).asString()), false);
        }

        @Test
        void the_flux_subscribed_at_a_catchup_time_checkpoint_fails_when_subscribed_to() {
            assertFluxFailsWhenSubscribedTo(catchupTimeCheckpoint(), false);
        }

        @Test
        void the_flux_that_fails_if_the_start_is_lost_fails_when_subscribed_to_at_a_global_checkpoint() {
            assertFluxFailsWhenSubscribedTo(GlobalCheckpoint.of(7), true);
        }

        @Test
        void the_flux_that_fails_if_the_start_is_lost_fails_when_subscribed_to_at_a_global_checkpoint_read_back_as_a_string() {
            assertFluxFailsWhenSubscribedTo(new StringBasedCheckpoint(GlobalCheckpoint.of(5, liveStart(), 0, 7).asString()), true);
        }

        @Test
        void the_flux_that_fails_if_the_start_is_lost_fails_when_subscribed_to_at_a_catchup_time_checkpoint() {
            assertFluxFailsWhenSubscribedTo(catchupTimeCheckpoint(), true);
        }

        @Test
        void subscribing_at_a_time_string_is_still_accepted() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();

            // When
            Subscription started = subscription.subscribe(subscriptionId, StartAt.checkpoint(new StringBasedCheckpoint("2026-01-01T00:00:00Z")), cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)));
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            NameDefined written = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(written)).block();

            // Then
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(state).extracting(CloudEvent::getId).contains(written.eventId()));
        }

        @Test
        void the_flux_subscribed_at_a_time_string_is_still_accepted() {
            // When
            Flux<CloudEvent> flux = subscription.subscribe(StartAt.checkpoint(new StringBasedCheckpoint("2026-01-01T00:00:00Z")));

            // Then it neither fails nor completes
            StepVerifier.create(flux)
                    .expectSubscription()
                    .expectNoEvent(Duration.ofMillis(500))
                    .thenCancel()
                    .verify(Duration.ofSeconds(5));
        }

        @Test
        void subscribing_at_a_dynamic_position_is_not_checked() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();

            // When
            Throwable throwable = catchThrowable(() -> subscription.subscribe(subscriptionId, StartAt.dynamic(() -> StartAt.checkpoint(GlobalCheckpoint.of(7))), cloudEvent -> Mono.empty()));

            // Then
            assertThat(throwable).isNull();
            assertThat(subscription.isRunning(subscriptionId)).isTrue();
        }

        private void assertNamedSubscribeIsRefusedAndLeavesTheIdFree(Checkpoint checkpoint) {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> refused = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> later = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            NameDefined writtenBefore = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBefore)).block();
            StartAt startAt = StartAt.checkpoint(checkpoint);

            // When
            Throwable throwable = catchThrowable(() -> subscription.subscribe(subscriptionId, startAt, cloudEvent -> Mono.fromRunnable(() -> refused.add(cloudEvent))));

            // Then it is refused by the call itself, naming the checkpoint, and nothing is registered under the id
            assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("\"" + checkpoint.asString() + "\"");
            assertAll(
                    () -> assertThat(((UnsupportedStartAtException) throwable).startAt()).isSameAs(startAt),
                    () -> assertThat(subscription.isRunning(subscriptionId)).isFalse(),
                    () -> assertThat(subscription.isPaused(subscriptionId)).isFalse()
            );

            // And the id can be used again, while the refused handler is never called
            Subscription started = subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> later.add(cloudEvent)));
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "sentinel");
            mongoEventStore.write("1", 1, serialize(sentinel)).block();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(later).extracting(CloudEvent::getId).contains(sentinel.eventId()));
            assertThat(refused).isEmpty();
        }

        private void assertFluxFailsWhenSubscribedTo(Checkpoint checkpoint, boolean failingIfStartLost) {
            // Given
            NameDefined writtenBefore = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBefore)).block();
            StartAt startAt = StartAt.checkpoint(checkpoint);

            // When
            AtomicReference<Flux<CloudEvent>> flux = new AtomicReference<>();
            Throwable thrownByTheCall = catchThrowable(() -> flux.set(failingIfStartLost ? subscription.subscribeFailingIfStartLost(null, startAt) : subscription.subscribe(startAt)));

            // Then the call returns the Flux, which fails with the exception before it delivers anything, naming the checkpoint
            assertThat(thrownByTheCall).isNull();
            StepVerifier.create(flux.get())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                            .hasMessageContaining("\"" + checkpoint.asString() + "\"")
                            .satisfies(refused -> assertThat(((UnsupportedStartAtException) refused).startAt()).isSameAs(startAt)))
                    .verify(Duration.ofSeconds(5));
        }

        private Checkpoint liveStart() {
            return requireNonNull(subscription.globalCheckpoint().block(Duration.ofSeconds(10)));
        }

        private CatchupTimeCheckpoint catchupTimeCheckpoint() {
            return CatchupTimeCheckpoint.of("2026-01-01T00:00:00Z", liveStart(), "2026-01-01T00:00:00Z");
        }
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getSimpleName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }

    private void disposeAfterTest(Disposable disposable) {
        disposables.add(disposable);
    }
}