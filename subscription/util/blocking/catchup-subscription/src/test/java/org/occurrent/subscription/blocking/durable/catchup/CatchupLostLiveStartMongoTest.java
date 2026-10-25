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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * The oplog can lose the live start a catch-up hands over while the change stream opens, which MongoDB answers with
 * error 286. A failpoint does so once, right after the catch-up's final check, and an event is written at that moment.
 * The event must be delivered, whether the model restarts lost subscriptions from the present or gives them up.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupLostLiveStartMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String SUBSCRIPTION_APP = "catchup-lost-live-start-subscription";
    private static final String SUBSCRIPTION_ID = "subscription";
    private static final int HISTORY_LOST = 286;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    enum Driver {SPRING, NATIVE}

    enum Replay {POSITION, TIME, DCB}

    static Stream<Arguments> everyCombination() {
        return Stream.of(Replay.values()).flatMap(replay -> Stream.of(true, false).flatMap(restart ->
                Stream.of(Arguments.of(Driver.SPRING, replay, restart, true),
                        Arguments.of(Driver.SPRING, replay, restart, false),
                        Arguments.of(Driver.NATIVE, replay, restart, false))));
    }

    private MongoClient client;
    private MongoClient subscriptionClient;
    private SubscriptionModel liveModel;
    private ExecutorService nativeExecutor;
    private CatchupSubscriptionModel catchup;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private final AtomicInteger historyLostReplies = new AtomicInteger();

    @AfterEach
    void shutdown() {
        if (client != null) {
            client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        }
        if (catchup != null) {
            catchup.shutdown();
        }
        if (liveModel instanceof SpringMongoSubscriptionModel spring) {
            spring.shutdown();
        } else if (liveModel instanceof NativeMongoSubscriptionModel nativeModel) {
            nativeModel.shutdown();
        }
        if (nativeExecutor != null) {
            nativeExecutor.shutdownNow();
        }
        if (subscriptionClient != null) {
            subscriptionClient.close();
        }
        if (client != null) {
            client.close();
        }
    }

    @ParameterizedTest(name = "{0}, {1} replay, restartOnHistoryLost={2}, storing={3}")
    @MethodSource("everyCombination")
    void a_live_start_the_oplog_loses_as_the_change_stream_opens_replays_again_and_delivers_what_was_written_in_between(Driver driver, Replay replay, boolean restartOnHistoryLost, boolean storing) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        client = MongoClients.create(connectionString);
        subscriptionClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).applicationName(SUBSCRIPTION_APP)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandFailed(CommandFailedEvent event) {
                        if (event.getThrowable() instanceof MongoCommandException e && e.getErrorCode() == HISTORY_LOST) {
                            historyLostReplies.incrementAndGet();
                        }
                    }
                }).build());
        MongoTemplate template = new MongoTemplate(client, requireNonNull(connectionString.getDatabase()));
        EventStoreConfig.Builder storeConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, requireNonNull(connectionString.getDatabase()))))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING);
        SpringMongoEventStore eventStore = new SpringMongoEventStore(template, switch (replay) {
            case POSITION -> storeConfig.eventStoreCapabilities(STREAM).withStreamPosition().build();
            case TIME -> storeConfig.eventStoreCapabilities(STREAM).withoutStreamPosition().build();
            case DCB -> storeConfig.eventStoreCapabilities(STREAM, DCB).build();
        });
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"));
        history.forEach(event -> write(eventStore, replay, event));
        NameDefined writtenInTheGap = nameDefined("writtenInTheGap");
        RetryStrategy retries = RetryStrategy.fixed(Duration.ofMillis(100));
        CheckpointAwareSubscriptionModel mongoModel;
        if (driver == Driver.SPRING) {
            SpringMongoSubscriptionModel spring = new SpringMongoSubscriptionModel(new MongoTemplate(subscriptionClient, requireNonNull(connectionString.getDatabase())),
                    SpringMongoSubscriptionModelConfig.withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(restartOnHistoryLost).retryStrategy(retries));
            liveModel = spring;
            mongoModel = spring;
        } else {
            nativeExecutor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(subscriptionClient.getDatabase(requireNonNull(connectionString.getDatabase())), "events",
                    TimeRepresentation.RFC_3339_STRING, nativeExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(restartOnHistoryLost).retryStrategy(retries));
            liveModel = nativeModel;
            mongoModel = nativeModel;
        }
        ArmsTheFailpointAfterTheFinalCheck armed = new ArmsTheFailpointAfterTheFinalCheck(mongoModel, () -> {
            client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", HISTORY_LOST).append("appName", SUBSCRIPTION_APP)));
            write(eventStore, replay, writtenInTheGap);
        });
        SpringMongoCheckpointStorage storage = new SpringMongoCheckpointStorage(template, "storage");
        CatchupSubscriptionModelConfig config = storing
                ? new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1))
                : new CatchupSubscriptionModelConfig(100);
        catchup = switch (replay) {
            case POSITION, TIME -> new CatchupSubscriptionModel(armed, eventStore, config);
            case DCB -> new CatchupSubscriptionModel(armed, eventStore, DcbCriteria.tags(Tag.parse("name:1")), config);
        };
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> catchupStarts = new CopyOnWriteArrayList<>();
        catchup.listenForCatchup(SUBSCRIPTION_ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                catchupStarts.add("catchupStarted");
            }

            @Override
            public void historyRead(Object episode) {
            }
        });

        Subscription started = catchup.subscribe(SUBSCRIPTION_ID, replay == Replay.TIME ? StartAtTime.beginningOfTime() : StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));

        assertThat(started.waitUntilStarted(Duration.ofSeconds(20))).as("started").isTrue();
        assertThat(historyLostReplies).as("the replies that said the oplog lost the history").hasValue(1);
        NameDefined afterHandOver = nameDefined("afterHandOver");
        write(eventStore, replay, afterHandOver);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(received).as("received after the hand over").contains(afterHandOver));
        assertAll(
                () -> assertThat(received).as("the event written between the final check and the open of the change stream").filteredOn(writtenInTheGap::equals).hasSize(1),
                () -> assertThat(received).as("the history").containsAll(history),
                () -> assertThat(received).as("the event written after the hand over").filteredOn(afterHandOver::equals).hasSize(1),
                () -> assertThat(catchupStarts).as("the catch-up started once for each replay").hasSize(2),
                () -> assertThat(catchup.isRunning(SUBSCRIPTION_ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("paused").isFalse()
        );
        if (storing) {
            Checkpoint lostLiveStart = armed.probes.getFirst();
            Checkpoint liveStartHandedOver = armed.probes.getLast();
            assertAll(
                    () -> assertThat(liveStartHandedOver.asString()).as("the live start probed last differs from the one that was lost").isNotEqualTo(lostLiveStart.asString()),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("the position stored").isNotNull().extracting(Checkpoint::asString).isEqualTo(liveStartHandedOver.asString())
            );
        }
    }

    private static final class ArmsTheFailpointAfterTheFinalCheck implements CheckpointAwareSubscriptionModel, SubscriptionModelWrapper, RepositionableSubscriptions {
        private final CheckpointAwareSubscriptionModel delegate;
        private final Runnable afterTheFirstFinalCheck;
        private final AtomicBoolean armed = new AtomicBoolean();
        private volatile @Nullable Checkpoint armedFor;
        final List<Checkpoint> probes = new CopyOnWriteArrayList<>();

        private ArmsTheFailpointAfterTheFinalCheck(CheckpointAwareSubscriptionModel delegate, Runnable afterTheFirstFinalCheck) {
            this.delegate = delegate;
            this.afterTheFirstFinalCheck = afterTheFirstFinalCheck;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            Checkpoint lost = armedFor;
            if (lost != null && lost.asString().equals(checkpoint.asString())) {
                probes.add(checkpoint);
                return false;
            }
            boolean canResume = delegate.canResumeFrom(checkpoint);
            probes.add(checkpoint);
            if (canResume && armed.compareAndSet(false, true)) {
                armedFor = checkpoint;
                afterTheFirstFinalCheck.run();
            }
            return canResume;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return delegate.globalCheckpoint();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
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
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return RepositionableSubscriptions.findIn(delegate)
                    .orElseThrow(() -> new UnsupportedOperationException(delegate.getClass().getSimpleName() + " is not repositionable"))
                    .resumeSubscription(subscriptionId, startAt);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return RepositionableSubscriptions.findIn(delegate).map(model -> model.restartsAt(checkpoint)).orElse(true);
        }
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void write(SpringMongoEventStore eventStore, Replay replay, DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        if (replay == Replay.DCB) {
            eventStore.append(cloudEvents.stream().map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse("name:1")))).toList());
        } else {
            eventStore.write(UUID.randomUUID().toString(), WriteCondition.anyStreamVersion(), cloudEvents);
        }
    }
}
