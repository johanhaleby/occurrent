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

package org.occurrent.subscription.reactor.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.awaitility.core.ConditionTimeoutException;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.RepositionableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * MongoDB answers error 286 when the live start a catch-up hands over is lost as the change stream opens.
 * A fail point does so once after the final check, with an event written in between.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupLostLiveStartMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String SUBSCRIBER_APPLICATION_NAME = "reactor-catchup-lost-live-start-subscriber";
    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Tag TAG = Tag.parse("name:1");
    private static final int HISTORY_LOST = 286;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private enum Replay {STREAM, DCB}

    private final AtomicInteger historyLostReplies = new AtomicInteger();
    private final CopyOnWriteArrayList<Disposable> disposables = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger catchupsStarted = new AtomicInteger();
    private MongoClient writerClient;
    private MongoClient subscriberClient;
    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel mongoModel;
    private ReactorCatchupSubscriptionModel catchup;
    private CloudEventConverter<DomainEvent> converter;

    @BeforeEach
    void createClients() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactorlostlivestart");
        String database = requireNonNull(connectionString.getDatabase());
        writerClient = MongoClients.create(connectionString);
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).applicationName(SUBSCRIBER_APPLICATION_NAME)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandFailed(CommandFailedEvent event) {
                        if (event.getThrowable() instanceof MongoCommandException e && e.getErrorCode() == HISTORY_LOST) {
                            historyLostReplies.incrementAndGet();
                        }
                    }
                }).build());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(writerClient, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(writerClient, database), config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        disposables.forEach(Disposable::dispose);
        Mono.from(writerClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"))).block(Duration.ofSeconds(10));
        if (catchup != null) {
            catchup.shutdown();
        }
        subscriberClient.close();
        writerClient.close();
    }

    @Test
    void a_stream_subscription_with_an_id_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_restarts_lost_subscriptions() {
        verifiesASubscriptionWithAnIdReplaysAgain(Replay.STREAM, true);
    }

    @Test
    void a_stream_subscription_with_an_id_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_ends_lost_subscriptions() {
        verifiesASubscriptionWithAnIdReplaysAgain(Replay.STREAM, false);
    }

    @Test
    void a_dcb_subscription_with_an_id_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_restarts_lost_subscriptions() {
        verifiesASubscriptionWithAnIdReplaysAgain(Replay.DCB, true);
    }

    @Test
    void a_dcb_subscription_with_an_id_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_ends_lost_subscriptions() {
        verifiesASubscriptionWithAnIdReplaysAgain(Replay.DCB, false);
    }

    @Test
    void a_stream_flux_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_restarts_lost_subscriptions() {
        verifiesAFluxReplaysAgain(Replay.STREAM, true);
    }

    @Test
    void a_stream_flux_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_ends_lost_subscriptions() {
        verifiesAFluxReplaysAgain(Replay.STREAM, false);
    }

    @Test
    void a_dcb_flux_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_restarts_lost_subscriptions() {
        verifiesAFluxReplaysAgain(Replay.DCB, true);
    }

    @Test
    void a_dcb_flux_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_ends_lost_subscriptions() {
        verifiesAFluxReplaysAgain(Replay.DCB, false);
    }

    @Test
    void a_start_of_the_wrapped_model_that_resumes_a_stream_subscription_where_the_oplog_lost_the_history_just_before_the_takeover_resumes_it_skips_nothing_when_the_model_restarts_lost_subscriptions() {
        verifiesAStartThatResumesTheLostLiveStartJustBeforeTheTakeoverResumesTheSubscriptionSkipsNothing(true);
    }

    @Test
    void a_start_of_the_wrapped_model_that_resumes_a_stream_subscription_where_the_oplog_lost_the_history_just_before_the_takeover_resumes_it_skips_nothing_when_the_model_ends_lost_subscriptions() {
        verifiesAStartThatResumesTheLostLiveStartJustBeforeTheTakeoverResumesTheSubscriptionSkipsNothing(false);
    }

    private void verifiesASubscriptionWithAnIdReplaysAgain(Replay replay, boolean restartLostSubscriptions) {
        NameDefined history1 = nameDefined();
        NameDefined history2 = nameDefined();
        NameDefined writtenInTheGap = nameDefined();
        NameDefined afterHandOver = nameDefined();
        write(replay, history1);
        write(replay, history2);
        catchup = catchupWhoseFinalCheckIsFollowedByALostLiveStart(writtenInTheGap, replay, restartLostSubscriptions);
        catchup.listenForCatchup(SUBSCRIPTION_ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                catchupsStarted.incrementAndGet();
            }

            @Override
            public void historyRead(Object episode) {
            }
        });

        Subscription started = catchup.subscribe(SUBSCRIPTION_ID, filter(replay), StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> Mono.fromRunnable(() -> received.add(event.getId())));

        started.waitUntilStarted().block(Duration.ofSeconds(30));
        assertThat(waitsFor(() -> catchupsStarted.get() == 2 && !catchup.isCatchingUp(SUBSCRIPTION_ID) && catchup.isRunning(SUBSCRIPTION_ID) && !catchup.isPaused(SUBSCRIPTION_ID)))
                .as("the catch-up started again after the oplog lost the live start, and the subscription runs again, started %s times", catchupsStarted.get()).isTrue();
        write(replay, afterHandOver);
        assertThat(waitsFor(() -> received.contains(afterHandOver.eventId()))).as("received after the hand over").isTrue();
        assertAll(
                () -> assertThat(historyLostReplies).as("the replies that said the oplog lost the history").hasValue(1),
                () -> assertThat(received).as("the event written between the final check and the open of the change stream").filteredOn(writtenInTheGap.eventId()::equals).hasSize(1),
                () -> assertThat(received).as("the history").contains(history1.eventId(), history2.eventId()),
                () -> assertThat(received).as("the event written after the hand over").filteredOn(afterHandOver.eventId()::equals).hasSize(1),
                () -> assertThat(catchupsStarted).as("the catch-up started once for each replay").hasValue(2)
        );
    }

    // The takeover's final check arms the fail point again and writes an event after the new live start, and the
    // takeover's resume at that live start is preceded by a start(true) that resumes the subscription at the lost one,
    // which the fail point then fails once more
    private void verifiesAStartThatResumesTheLostLiveStartJustBeforeTheTakeoverResumesTheSubscriptionSkipsNothing(boolean restartLostSubscriptions) {
        Replay replay = Replay.STREAM;
        NameDefined history1 = nameDefined();
        NameDefined history2 = nameDefined();
        NameDefined writtenInTheGap = nameDefined();
        NameDefined afterTheNewLiveStart = nameDefined();
        NameDefined afterHandOver = nameDefined();
        write(replay, history1);
        write(replay, history2);
        ArmsTheFailPointAfterTheFinalCheck armed = new ArmsTheFailPointAfterTheFinalCheck(mongoModel(restartLostSubscriptions),
                () -> failsTheNextChangeStreamOpenAfterWriting(replay, writtenInTheGap), () -> failsTheNextChangeStreamOpenAfterWriting(replay, afterTheNewLiveStart));
        catchup = new ReactorCatchupSubscriptionModel(armed, eventStore, eventStore, null, null, 100, 1000);
        catchup.listenForCatchup(SUBSCRIPTION_ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                catchupsStarted.incrementAndGet();
            }

            @Override
            public void historyRead(Object episode) {
            }
        });

        Subscription started = catchup.subscribe(SUBSCRIPTION_ID, filter(replay), StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> Mono.fromRunnable(() -> received.add(event.getId())));

        started.waitUntilStarted().block(Duration.ofSeconds(30));
        assertThat(waitsFor(() -> historyLostReplies.get() == 2 && catchupsStarted.get() >= 2 && !catchup.isCatchingUp(SUBSCRIPTION_ID) && catchup.isRunning(SUBSCRIPTION_ID) && !catchup.isPaused(SUBSCRIPTION_ID)))
                .as("the oplog lost the history twice and the subscription runs again, started %s times, with %s replies that said the oplog lost the history", catchupsStarted.get(), historyLostReplies.get()).isTrue();
        write(replay, afterHandOver);
        assertThat(waitsFor(() -> received.contains(afterHandOver.eventId()))).as("received after the hand over").isTrue();
        assertAll(
                () -> assertThat(armed.resumedWhereTheHistoryWasLost).as("the start resumed the subscription at the lost live start before the takeover resumed it").isTrue(),
                () -> assertThat(received).as("the event written after the live start the takeover hands over to").contains(afterTheNewLiveStart.eventId()),
                () -> assertThat(received).as("the event written between the final check and the open of the change stream").contains(writtenInTheGap.eventId()),
                () -> assertThat(received).as("the history").contains(history1.eventId(), history2.eventId()),
                () -> assertThat(received).as("the event written after the hand over").contains(afterHandOver.eventId())
        );
    }

    private void verifiesAFluxReplaysAgain(Replay replay, boolean restartLostSubscriptions) {
        NameDefined history1 = nameDefined();
        NameDefined history2 = nameDefined();
        NameDefined writtenInTheGap = nameDefined();
        NameDefined afterHandOver = nameDefined();
        write(replay, history1);
        write(replay, history2);
        catchup = catchupWhoseFinalCheckIsFollowedByALostLiveStart(writtenInTheGap, replay, restartLostSubscriptions);

        disposables.add(catchup.subscribe(filter(replay), StartAt.checkpoint(GlobalCheckpoint.of(0))).doOnNext(event -> received.add(event.getId())).subscribe());

        assertThat(waitsFor(() -> received.contains(writtenInTheGap.eventId()))).as("the event written between the final check and the open of the change stream is received").isTrue();
        write(replay, afterHandOver);
        assertThat(waitsFor(() -> received.contains(afterHandOver.eventId()))).as("received after the hand over").isTrue();
        assertAll(
                () -> assertThat(historyLostReplies).as("the replies that said the oplog lost the history").hasValue(1),
                () -> assertThat(received).as("the event written between the final check and the open of the change stream").filteredOn(writtenInTheGap.eventId()::equals).hasSize(1),
                () -> assertThat(received).as("the history").contains(history1.eventId(), history2.eventId()),
                () -> assertThat(received).as("the event written after the hand over").filteredOn(afterHandOver.eventId()::equals).hasSize(1)
        );
    }

    // Fails the next change stream open once with 286 and writes an event between the live start and the open
    private ReactorCatchupSubscriptionModel catchupWhoseFinalCheckIsFollowedByALostLiveStart(NameDefined writtenInTheGap, Replay replay, boolean restartLostSubscriptions) {
        ArmsTheFailPointAfterTheFinalCheck armed = new ArmsTheFailPointAfterTheFinalCheck(mongoModel(restartLostSubscriptions), () -> failsTheNextChangeStreamOpenAfterWriting(replay, writtenInTheGap), null);
        return new ReactorCatchupSubscriptionModel(armed, eventStore, eventStore, null, null, 100, 1000);
    }

    private ReactorMongoSubscriptionModel mongoModel(boolean restartLostSubscriptions) {
        mongoModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, requireNonNull(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactorlostlivestart").getDatabase())),
                "events", TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)).restartSubscriptionsOnChangeStreamHistoryLost(restartLostSubscriptions));
        return mongoModel;
    }

    private Mono<Void> failsTheNextChangeStreamOpenAfterWriting(Replay replay, DomainEvent event) {
        return Mono.from(writerClient.getDatabase("admin").runCommand(
                        new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", HISTORY_LOST).append("appName", SUBSCRIBER_APPLICATION_NAME))))
                .then(writeReactively(replay, event))
                // The ping brings the cluster time the client knows past the event, and the delay moves the present a second past it
                .then(Mono.from(subscriberClient.getDatabase("admin").runCommand(new Document("ping", 1))).then())
                .then(Mono.delay(Duration.ofMillis(1100)).then());
    }

    private static SubscriptionFilter filter(Replay replay) {
        return switch (replay) {
            case STREAM -> StreamSubscriptionFilter.filter(Filter.all());
            case DCB -> DcbSubscriptionFilter.filter(DcbCriteria.tags(TAG));
        };
    }

    // A wait that runs out is a failed assertion, not an error
    private static boolean waitsFor(BooleanSupplier condition) {
        try {
            await().atMost(Duration.ofSeconds(20)).until(condition::getAsBoolean);
            return true;
        } catch (ConditionTimeoutException e) {
            return false;
        }
    }

    private NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
    }

    private void write(Replay replay, DomainEvent event) {
        writeReactively(replay, event).block(Duration.ofSeconds(10));
    }

    private Mono<Void> writeReactively(Replay replay, DomainEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event));
        return switch (replay) {
            case STREAM -> eventStore.write(UUID.randomUUID().toString(), WriteCondition.anyStreamVersion(), Flux.fromIterable(cloudEvents)).then();
            case DCB -> eventStore.append(cloudEvents.stream().map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, Set.of(TAG))).toList()).then();
        };
    }

    // Runs the first task once after the first resume check that passes, and forwards every other call, capabilities
    // included. With a second task, it runs that once after the second resume check that passes, and the first resume
    // at a position starts the wrapped model first, which resumes every subscription it holds paused where it was held.
    private static final class ArmsTheFailPointAfterTheFinalCheck implements CheckpointAwareSubscriptionModel, SubscriptionModel, RepositionableSubscriptions {
        private final ReactorMongoSubscriptionModel delegate;
        private final Supplier<Mono<Void>> afterTheFirstFinalCheck;
        private final @Nullable Supplier<Mono<Void>> afterTheSecondFinalCheck;
        private final AtomicInteger checksPassed = new AtomicInteger();
        private final AtomicBoolean resumedAtAPosition = new AtomicBoolean();
        private volatile boolean resumedWhereTheHistoryWasLost;

        private ArmsTheFailPointAfterTheFinalCheck(ReactorMongoSubscriptionModel delegate, Supplier<Mono<Void>> afterTheFirstFinalCheck, @Nullable Supplier<Mono<Void>> afterTheSecondFinalCheck) {
            this.delegate = delegate;
            this.afterTheFirstFinalCheck = afterTheFirstFinalCheck;
            this.afterTheSecondFinalCheck = afterTheSecondFinalCheck;
        }

        @Override
        public Mono<Boolean> canResumeFrom(Checkpoint checkpoint) {
            return delegate.canResumeFrom(checkpoint).flatMap(canResume -> {
                if (!canResume) {
                    return Mono.just(false);
                }
                int passed = checksPassed.incrementAndGet();
                if (passed == 1) {
                    return Mono.defer(afterTheFirstFinalCheck).thenReturn(true);
                }
                Supplier<Mono<Void>> second = afterTheSecondFinalCheck;
                return passed == 2 && second != null ? Mono.defer(second).thenReturn(true) : Mono.just(true);
            });
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            if (afterTheSecondFinalCheck != null && resumedAtAPosition.compareAndSet(false, true)) {
                delegate.start(true);
                resumedWhereTheHistoryWasLost = delegate.isRunning(subscriptionId);
            }
            return delegate.resumeSubscription(subscriptionId, startAt);
        }

        @Override
        public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
            return type == RepositionableSubscriptions.class ? Optional.of(type.cast(this)) : delegate.capability(type);
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return delegate.globalCheckpoint();
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return delegate.globalCheckpointAsOfNow();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return delegate.subscribe(filter, startAt);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            return delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
