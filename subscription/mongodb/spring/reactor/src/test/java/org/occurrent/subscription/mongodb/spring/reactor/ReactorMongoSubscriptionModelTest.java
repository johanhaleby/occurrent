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
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
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
        void subscribing_at_a_global_checkpoint_of_zero_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(0));
        }

        @Test
        void subscribing_at_a_dynamic_position_answering_a_global_checkpoint_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeAtADynamicPositionIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(0));
        }

        @Test
        void subscribing_at_a_dynamic_position_answering_a_catchup_time_checkpoint_is_refused_and_leaves_the_id_free() {
            assertNamedSubscribeAtADynamicPositionIsRefusedAndLeavesTheIdFree(catchupTimeCheckpoint());
        }

        @Test
        void the_id_of_a_refused_subscription_is_free_when_its_handle_fails() throws Exception {
            // Given a position that fails once, so the retry asks it again off the subscribing thread and the handle exists by then
            String subscriptionId = UUID.randomUUID().toString();
            AtomicInteger calls = new AtomicInteger();
            CountDownLatch mayAnswer = new CountDownLatch(1);
            StartAt startAt = StartAt.dynamic(ctx -> {
                if (calls.incrementAndGet() == 1) {
                    throw new IllegalStateException("Fails once");
                }
                try {
                    mayAnswer.await(20, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return StartAt.checkpoint(GlobalCheckpoint.of(0));
            });
            AtomicReference<Boolean> runningInTheCallback = new AtomicReference<>();
            AtomicReference<Throwable> thrownBySubscribingAgain = new AtomicReference<>();
            AtomicReference<Subscription> subscribedAgain = new AtomicReference<>();
            CountDownLatch called = new CountDownLatch(1);

            // And the handle's failure is reacted to by subscribing with the same id, on the thread that emits it
            Subscription refused = subscription.subscribe(subscriptionId, startAt, cloudEvent -> Mono.empty());
            try {
                refused.waitUntilStarted().doOnError(throwable -> {
                    runningInTheCallback.set(subscription.isRunning(subscriptionId));
                    thrownBySubscribingAgain.set(catchThrowable(() ->
                            subscribedAgain.set(subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> Mono.empty()))));
                    called.countDown();
                }).subscribe(__ -> {
                }, __ -> {
                });

                // When the position answers a catchup checkpoint, which refuses the subscription
                mayAnswer.countDown();

                // Then the id is free when the handle fails, so subscribing with it again throws nothing
                assertThat(called.await(10, SECONDS)).isTrue();
                assertAll(
                        () -> assertThat(runningInTheCallback.get()).isFalse(),
                        () -> assertThat(thrownBySubscribingAgain.get()).isNull()
                );
            } finally {
                mayAnswer.countDown();
                if (subscribedAgain.get() != null) {
                    subscription.cancelSubscription(subscriptionId).block(Duration.ofSeconds(10));
                }
            }
        }

        @Test
        void the_flux_subscribed_at_a_dynamic_position_answering_a_global_checkpoint_fails_when_subscribed_to() {
            assertFluxAtADynamicPositionFailsWhenSubscribedTo(GlobalCheckpoint.of(0), startAt -> subscription.subscribe(startAt));
        }

        @Test
        void the_flux_subscribed_at_a_dynamic_position_answering_a_catchup_time_checkpoint_fails_when_subscribed_to() {
            assertFluxAtADynamicPositionFailsWhenSubscribedTo(catchupTimeCheckpoint(), startAt -> subscription.subscribe(startAt));
        }

        @Test
        void the_flux_that_fails_if_the_start_is_lost_fails_when_subscribed_to_at_a_dynamic_position_answering_a_global_checkpoint() {
            assertFluxAtADynamicPositionFailsWhenSubscribedTo(GlobalCheckpoint.of(0), startAt -> subscription.subscribeFailingIfStartLost(null, startAt));
        }

        @Test
        void the_flux_that_fails_on_history_loss_fails_when_subscribed_to_at_a_dynamic_position_answering_a_global_checkpoint() {
            assertFluxAtADynamicPositionFailsWhenSubscribedTo(GlobalCheckpoint.of(0), startAt -> subscription.subscribeFailingOnHistoryLoss(null, startAt));
        }

        @Test
        void the_dcb_flux_subscribed_at_a_dynamic_position_answering_the_beginning_fails_when_subscribed_to() {
            // Given
            DcbSubscriptionModel dcbSubscription = DcbSubscriptionModel.from(subscription);

            // When
            Flux<CloudEvent> flux = dcbSubscription.subscribe(DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.dynamic(ctx -> DcbStartAt.beginning()));

            // Then
            StepVerifier.create(flux)
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                            .hasMessageContaining("\"" + GlobalCheckpoint.of(0).asString() + "\""))
                    .verify(Duration.ofSeconds(5));
        }

        @Test
        void the_dcb_named_subscription_at_a_dynamic_position_answering_the_beginning_stops_with_the_exception() {
            // Given
            DcbSubscriptionModel dcbSubscription = DcbSubscriptionModel.from(subscription);
            String subscriptionId = UUID.randomUUID().toString();

            // When
            Subscription started = dcbSubscription.subscribe(subscriptionId, DcbCriteria.tags(Tag.parse("name:1")), DcbStartAt.dynamic(ctx -> DcbStartAt.beginning()), cloudEvent -> Mono.empty());

            // Then
            StepVerifier.create(started.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                            .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                            .hasMessageContaining("\"" + GlobalCheckpoint.of(0).asString() + "\""))
                    .verify(Duration.ofSeconds(5));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription.isRunning(subscriptionId)).isFalse());
        }

        @Test
        void subscribing_at_a_dynamic_position_answering_a_live_checkpoint_delivers_the_events_after_it() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> first = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> second = new CopyOnWriteArrayList<>();
            Subscription live = subscription.subscribe(UUID.randomUUID().toString(), StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> first.add(cloudEvent)));
            assertThat(live.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            NameDefined e1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(e1)).block();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(first).extracting(CloudEvent::getId).contains(e1.eventId()));
            Checkpoint afterE1 = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(first.getFirst());
            NameWasChanged e2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            mongoEventStore.write("1", 1, serialize(e2)).block();

            // When
            Subscription started = subscription.subscribe(UUID.randomUUID().toString(), StartAt.dynamic(ctx -> StartAt.checkpoint(afterE1)), cloudEvent -> Mono.fromRunnable(() -> second.add(cloudEvent)));

            // Then it starts and delivers what was written after the checkpoint, and not what came before it
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(second).extracting(CloudEvent::getId).contains(e2.eventId()));
            assertThat(second).extracting(CloudEvent::getId).doesNotContain(e1.eventId());
        }

        @Test
        void the_flux_subscribed_at_a_dynamic_position_answering_a_live_checkpoint_delivers_the_events_after_it() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> first = new CopyOnWriteArrayList<>();
            Subscription live = subscription.subscribe(UUID.randomUUID().toString(), StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> first.add(cloudEvent)));
            assertThat(live.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            NameDefined e1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(e1)).block();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(first).extracting(CloudEvent::getId).contains(e1.eventId()));
            Checkpoint afterE1 = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(first.getFirst());
            NameWasChanged e2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            mongoEventStore.write("1", 1, serialize(e2)).block();

            // When
            Flux<CloudEvent> flux = subscription.subscribe(StartAt.dynamic(ctx -> StartAt.checkpoint(afterE1)));

            // Then it delivers what was written after the checkpoint, and not what came before it
            StepVerifier.create(flux)
                    .expectNextMatches(cloudEvent -> cloudEvent.getId().equals(e2.eventId()))
                    .thenCancel()
                    .verify(Duration.ofSeconds(10));
        }

        @Test
        void a_resume_of_a_run_a_pause_closed_before_it_opened_is_refused_when_the_position_then_answers_a_global_checkpoint() throws Exception {
            // Given a live checkpoint with an event written after it, and a position that answers it only to the first run
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint afterE1 = checkpointAfter(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"));
            mongoEventStore.write("1", 1, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2"))).block();
            Checkpoint catchup = GlobalCheckpoint.of(7);
            AtomicInteger calls = new AtomicInteger();
            CountDownLatch asked = new CountDownLatch(1);
            CountDownLatch mayAnswer = new CountDownLatch(1);
            StartAt startAt = StartAt.dynamic(ctx -> {
                if (calls.incrementAndGet() > 1) {
                    return StartAt.checkpoint(catchup);
                }
                asked.countDown();
                try {
                    mayAnswer.await(20, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return StartAt.checkpoint(afterE1);
            });

            // And the subscribing thread is held where the position is asked, since the subscribe asks it on that thread
            CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() ->
                    subscription.subscribe(subscriptionId, startAt, cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent))));
            try {
                assertThat(asked.await(10, SECONDS)).isTrue();

                // And the run is paused while it is asked, so it ends without opening a change stream
                subscription.pauseSubscription(subscriptionId);
                mayAnswer.countDown();
                subscribing.get(10, SECONDS);

                // When the subscription is resumed without a position
                Subscription resumed = subscription.resumeSubscription(subscriptionId);

                // Then the position answers the catch-up checkpoint, which refuses it instead of opening at the present
                StepVerifier.create(resumed.waitUntilStarted(Duration.ofSeconds(10)))
                        .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                                .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                                .hasMessageContaining("\"" + catchup.asString() + "\""))
                        .verify(Duration.ofSeconds(15));
                assertAll(
                        () -> assertThat(subscription.isRunning(subscriptionId)).isFalse(),
                        () -> assertThat(state).isEmpty()
                );
            } finally {
                mayAnswer.countDown();
            }
        }

        @Test
        void a_first_answer_the_server_rejects_does_not_end_the_check_of_a_named_subscription() throws Exception {
            // Given a model that reads quiet positions, and a dynamic position whose first answer is a resume token the server rejects
            assertThat(subscription.readsQuietPositions()).isTrue();
            String subscriptionId = UUID.randomUUID().toString();
            AtomicInteger answers = new AtomicInteger();
            CountDownLatch askedAgain = new CountDownLatch(1);
            StartAt startAt = StartAt.dynamic(ctx -> {
                if (answers.incrementAndGet() == 1) {
                    return StartAt.checkpoint(new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("not-a-token"))));
                }
                askedAgain.countDown();
                return StartAt.checkpoint(GlobalCheckpoint.of(0));
            });

            // When it subscribes, so the retry asks the position again, and the answer is a catch-up's
            Subscription started = subscription.subscribe(subscriptionId, startAt, cloudEvent -> Mono.empty());
            assertThat(askedAgain.await(10, SECONDS)).as("the position was asked again").isTrue();

            // Then no stream had opened at the first answer, so the second is refused and the run is gone
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription.isRunning(subscriptionId)).as("running after the second answer").isFalse());
            assertThat(answers).hasValue(2);
            // And the handle keeps the answer it gave when the first answer was subscribed to
            assertThat(started.waitUntilStarted(Duration.ofSeconds(5)).block()).isTrue();
        }

        @Test
        void a_refusal_arriving_after_a_stream_opened_still_stops_the_subscription() throws Exception {
            // Given a dynamic position that answers a live checkpoint, then fails with the refusal of a catch-up's position, as the
            // check does, which stands in for a replaced run's late start landing between the check and the catch
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint live = liveStart();
            AtomicInteger answers = new AtomicInteger();
            // And a listener the model asks only once the cursor has opened, and which delivers no event
            CountDownLatch cursorOpened = new CountDownLatch(1);
            subscription.addQuietPositionListener(id -> {
                if (id.equals(subscriptionId)) {
                    cursorOpened.countDown();
                }
                return Mono.empty();
            });
            subscription.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                if (answers.incrementAndGet() == 2) {
                    throw new UnsupportedStartAtException(StartAt.checkpoint(GlobalCheckpoint.of(0)), "thrown as the check does");
                }
                return StartAt.checkpoint(live);
            }), cloudEvent -> Mono.empty());
            assertThat(cursorOpened.await(10, SECONDS)).as("the cursor opened").isTrue();

            // When it is paused and resumed, so the position is asked again
            subscription.pauseSubscription(subscriptionId);
            subscription.resumeSubscription(subscriptionId);

            // Then the run is gone, and the position isn't asked a third time
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription.isRunning(subscriptionId)).isFalse());
            assertThat(answers).hasValue(2);
        }

        @Test
        void a_resume_after_the_cursor_opened_is_not_refused_when_the_position_answers_a_catchup_position() throws Exception {
            // Given a dynamic position that answers the present first and a global checkpoint after, and a listener the model asks only once the cursor has opened
            String subscriptionId = UUID.randomUUID().toString();
            AtomicInteger answers = new AtomicInteger();
            CountDownLatch cursorOpened = new CountDownLatch(1);
            subscription.addQuietPositionListener(id -> {
                if (id.equals(subscriptionId)) {
                    cursorOpened.countDown();
                }
                return Mono.empty();
            });
            subscription.subscribe(subscriptionId, StartAt.dynamic(ctx -> answers.incrementAndGet() == 1 ? StartAt.now() : StartAt.checkpoint(GlobalCheckpoint.of(0))), cloudEvent -> Mono.empty());
            assertThat(cursorOpened.await(10, SECONDS)).as("the cursor opened").isTrue();

            // When it is paused and resumed, so the position is asked again
            subscription.pauseSubscription(subscriptionId);
            Subscription resumed = subscription.resumeSubscription(subscriptionId);

            // Then an opened cursor has ended the check, so the resume isn't refused and the subscription runs
            assertThat(resumed.waitUntilStarted(Duration.ofSeconds(5)).block()).isTrue();
            assertThat(answers).hasValue(2);
            assertThat(subscription.isRunning(subscriptionId)).isTrue();
        }

        @Test
        void an_action_that_fails_with_the_refusal_type_on_a_delivering_subscription_is_delivered_the_event_again() {
            assertTheEventIsDeliveredAgainAfterTheActionFailsWith(new UnsupportedStartAtException(StartAt.now(), "failed by the action"));
        }

        @Test
        void an_action_that_fails_with_another_exception_on_a_delivering_subscription_is_delivered_the_event_again() {
            assertTheEventIsDeliveredAgainAfterTheActionFailsWith(new IllegalStateException("failed by the action"));
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

        private void assertNamedSubscribeAtADynamicPositionIsRefusedAndLeavesTheIdFree(Checkpoint checkpoint) {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> refused = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> later = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            NameDefined writtenBefore = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBefore)).block();
            StartAt answered = StartAt.checkpoint(checkpoint);
            StartAt startAt = StartAt.dynamic(ctx -> answered);

            // When
            AtomicReference<Subscription> started = new AtomicReference<>();
            Throwable thrownByTheCall = catchThrowable(() -> started.set(subscription.subscribe(subscriptionId, startAt, cloudEvent -> Mono.fromRunnable(() -> refused.add(cloudEvent)))));

            // Then the call returns, and the subscription stops with the exception, naming the id and the checkpoint
            assertThat(thrownByTheCall).isNull();
            StepVerifier.create(started.get().waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                            .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                            .hasMessageContaining("\"" + checkpoint.asString() + "\"")
                            .satisfies(unsupported -> assertThat(((UnsupportedStartAtException) unsupported).startAt()).isSameAs(answered)))
                    .verify(Duration.ofSeconds(5));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription.isRunning(subscriptionId)).isFalse());
            assertThat(subscription.isPaused(subscriptionId)).isFalse();

            // And the id can be used again, while the refused handler is never called
            Subscription restarted = subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> later.add(cloudEvent)));
            assertThat(restarted.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "sentinel");
            mongoEventStore.write("1", 1, serialize(sentinel)).block();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(later).extracting(CloudEvent::getId).contains(sentinel.eventId()));
            assertThat(refused).isEmpty();
        }

        private void assertFluxAtADynamicPositionFailsWhenSubscribedTo(Checkpoint checkpoint, Function<StartAt, Flux<CloudEvent>> subscribe) {
            // Given
            NameDefined writtenBefore = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBefore)).block();
            StartAt answered = StartAt.checkpoint(checkpoint);
            StartAt startAt = StartAt.dynamic(ctx -> answered);

            // When
            AtomicReference<Flux<CloudEvent>> flux = new AtomicReference<>();
            Throwable thrownByTheCall = catchThrowable(() -> flux.set(subscribe.apply(startAt)));

            // Then the call returns the Flux, which fails with the exception before it delivers anything, naming the checkpoint
            assertThat(thrownByTheCall).isNull();
            StepVerifier.create(flux.get())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                            .hasMessageContaining("\"" + checkpoint.asString() + "\"")
                            .satisfies(unsupported -> assertThat(((UnsupportedStartAtException) unsupported).startAt()).isSameAs(answered)))
                    .verify(Duration.ofSeconds(5));
        }

        private void assertTheEventIsDeliveredAgainAfterTheActionFailsWith(RuntimeException failure) {
            // Given a delivering subscription whose action fails for the first event it is handed
            String subscriptionId = UUID.randomUUID().toString();
            CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
            AtomicInteger calls = new AtomicInteger();
            Subscription started = subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> Mono.defer(() -> {
                handled.add(cloudEvent.getId());
                return calls.incrementAndGet() == 1 ? Mono.error(failure) : Mono.empty();
            }));
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();

            // When an event is written
            NameDefined written = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(written)).block();

            // Then the subscription restarts and is handed the event again, as for any exception an action fails with
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(handled).containsExactly(written.eventId(), written.eventId()));
            assertThat(subscription.subscriptionIds()).contains(subscriptionId);
        }

        // Writes the event through a subscription at the present and returns the checkpoint of what it delivered
        private Checkpoint checkpointAfter(NameDefined written) {
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            String helperId = UUID.randomUUID().toString();
            Subscription helper = subscription.subscribe(helperId, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent)));
            assertThat(helper.waitUntilStarted(Duration.ofSeconds(10)).block()).isTrue();
            mongoEventStore.write("1", 0, serialize(written)).block();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(delivered).extracting(CloudEvent::getId).contains(written.eventId()));
            subscription.cancelSubscription(helperId).block(Duration.ofSeconds(10));
            return CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(delivered.getFirst());
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