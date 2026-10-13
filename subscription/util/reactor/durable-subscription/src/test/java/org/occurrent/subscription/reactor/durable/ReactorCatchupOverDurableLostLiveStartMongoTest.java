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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
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
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
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
 * A catch-up over a durable model over a MongoDB model, where MongoDB answers error 286 when the live start the
 * catch-up hands over is lost as the change stream opens. A fail point does so once after the final check, with an
 * event written in between. The event must be delivered, and the durable model must store no position before the
 * subscription delivers an event live.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupOverDurableLostLiveStartMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String SUBSCRIBER_APPLICATION_NAME = "reactor-catchup-over-durable-lost-live-start-subscriber";
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
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Checkpoint> saved = new CopyOnWriteArrayList<>();
    private final AtomicInteger catchupsStarted = new AtomicInteger();
    private String database;
    private MongoClient writerClient;
    private MongoClient subscriberClient;
    private ReactorMongoEventStore eventStore;
    private ReactorCatchupSubscriptionModel catchup;

    @BeforeEach
    void createClients() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactorcatchupoverdurable");
        database = requireNonNull(connectionString.getDatabase());
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
    }

    @AfterEach
    void shutdown() {
        Mono.from(writerClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"))).block(Duration.ofSeconds(10));
        if (catchup != null) {
            catchup.shutdown();
        }
        subscriberClient.close();
        writerClient.close();
    }

    @Test
    void a_stream_subscription_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_restarts_lost_subscriptions() {
        verifiesTheSubscriptionReplaysAgain(Replay.STREAM, true);
    }

    @Test
    void a_stream_subscription_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_ends_lost_subscriptions() {
        verifiesTheSubscriptionReplaysAgain(Replay.STREAM, false);
    }

    @Test
    void a_dcb_subscription_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_restarts_lost_subscriptions() {
        verifiesTheSubscriptionReplaysAgain(Replay.DCB, true);
    }

    @Test
    void a_dcb_subscription_whose_live_start_the_oplog_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between_when_the_model_ends_lost_subscriptions() {
        verifiesTheSubscriptionReplaysAgain(Replay.DCB, false);
    }

    private void verifiesTheSubscriptionReplaysAgain(Replay replay, boolean restartLostSubscriptions) {
        CloudEvent history1 = event();
        CloudEvent history2 = event();
        CloudEvent writtenInTheGap = event();
        CloudEvent afterHandOver = event();
        write(replay, history1);
        write(replay, history2);
        catchup = catchupOverDurableWhoseFinalCheckIsFollowedByALostLiveStart(writtenInTheGap, replay, restartLostSubscriptions);
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
        List<Checkpoint> savedBeforeLiveEvents = List.copyOf(saved);
        write(replay, afterHandOver);
        assertThat(waitsFor(() -> received.contains(afterHandOver.getId()) && !saved.isEmpty())).as("received after the hand over, and its position stored").isTrue();
        assertAll(
                () -> assertThat(historyLostReplies).as("the replies that said the oplog lost the history").hasValue(1),
                () -> assertThat(received).as("the event written between the final check and the open of the change stream").filteredOn(writtenInTheGap.getId()::equals).hasSize(1),
                () -> assertThat(received).as("the history").contains(history1.getId(), history2.getId()),
                () -> assertThat(received).as("the event written after the hand over").filteredOn(afterHandOver.getId()::equals).hasSize(1),
                () -> assertThat(catchupsStarted).as("the catch-up started once for each replay").hasValue(2),
                () -> assertThat(savedBeforeLiveEvents).as("the positions the durable model stored before the subscription delivered an event live").isEmpty()
        );
    }

    // Fails the next change stream open once with 286 and writes an event between the live start and the open
    private ReactorCatchupSubscriptionModel catchupOverDurableWhoseFinalCheckIsFollowedByALostLiveStart(CloudEvent writtenInTheGap, Replay replay, boolean restartLostSubscriptions) {
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, database), "events", TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)).restartSubscriptionsOnChangeStreamHistoryLost(restartLostSubscriptions));
        ArmsTheFailPointAfterTheFinalCheck armed = new ArmsTheFailPointAfterTheFinalCheck(mongoModel, () -> Mono.from(writerClient.getDatabase("admin").runCommand(
                        new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", HISTORY_LOST).append("appName", SUBSCRIBER_APPLICATION_NAME))))
                .then(writeReactively(replay, writtenInTheGap))
                // The ping brings the cluster time the client knows past the event, and the delay moves the present a second past it
                .then(Mono.from(subscriberClient.getDatabase("admin").runCommand(new Document("ping", 1))).then())
                .then(Mono.delay(Duration.ofMillis(1100)).then()));
        ReactorDurableSubscriptionModel durable = new ReactorDurableSubscriptionModel(armed, recordingSavesOf(new ReactorCheckpointStorage(new ReactiveMongoTemplate(writerClient, database), "checkpoints")));
        return new ReactorCatchupSubscriptionModel(durable, eventStore, eventStore, null, null, 100, 1000);
    }

    private CheckpointStorage recordingSavesOf(CheckpointStorage storage) {
        return (CheckpointStorage) Proxy.newProxyInstance(CheckpointStorage.class.getClassLoader(), new Class<?>[]{CheckpointStorage.class}, (proxy, method, args) -> {
            if (method.getName().equals("save") && args[0].equals(SUBSCRIPTION_ID)) {
                saved.add((Checkpoint) args[1]);
            }
            try {
                return method.invoke(storage, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
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

    private static CloudEvent event() {
        String id = UUID.randomUUID().toString();
        return CloudEventBuilder.v1().withId(id).withSource(SOURCE).withType("NameDefined").withTime(OffsetDateTime.now()).build();
    }

    private void write(Replay replay, CloudEvent event) {
        writeReactively(replay, event).block(Duration.ofSeconds(10));
    }

    private Mono<Void> writeReactively(Replay replay, CloudEvent event) {
        return switch (replay) {
            case STREAM -> eventStore.write(UUID.randomUUID().toString(), WriteCondition.anyStreamVersion(), Flux.just(event)).then();
            case DCB -> eventStore.append(List.of(DcbCloudEvents.withTags(event, Set.of(TAG)))).then();
        };
    }

    // Runs the task once after the first resume check and forwards every other call, capabilities included
    private static final class ArmsTheFailPointAfterTheFinalCheck implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private final ReactorMongoSubscriptionModel delegate;
        private final Supplier<Mono<Void>> afterTheFirstFinalCheck;
        private final AtomicBoolean armed = new AtomicBoolean();

        private ArmsTheFailPointAfterTheFinalCheck(ReactorMongoSubscriptionModel delegate, Supplier<Mono<Void>> afterTheFirstFinalCheck) {
            this.delegate = delegate;
            this.afterTheFirstFinalCheck = afterTheFirstFinalCheck;
        }

        @Override
        public Mono<Boolean> canResumeFrom(Checkpoint checkpoint) {
            return delegate.canResumeFrom(checkpoint).flatMap(canResume -> canResume && armed.compareAndSet(false, true)
                    ? Mono.defer(afterTheFirstFinalCheck).thenReturn(true)
                    : Mono.just(canResume));
        }

        @Override
        public <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
            return delegate.capability(type);
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
