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

package org.occurrent.subscription.blocking.competingconsumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A pause waits a second at most for an action that is running, and a cancel doesn't wait for it at all, so an action
 * can return after the run that called it was closed. What that run then writes must not change where the subscription
 * opens next, no attempt of the action starts once the run is closed, and a pause or a stop ends with the subscription
 * paused however its wait for the action ends.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ActionThatOutlivesItsRunTest {

    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    enum Model {SPRING, NATIVE}

    private final List<SubscriptionModel> started = new ArrayList<>();
    private MongoClient client;
    private MongoTemplate template;
    private SpringMongoEventStore eventStore;
    private String eventCollection;
    private SpringMongoCheckpointStorage storage;
    private final CountDownLatch releaseTheSlowAction = new CountDownLatch(1);
    // The model asks MongoDB for the present with a ping. Only the ping from the thread whose change stream open lost
    // its history is held, since the model also asks for the present when a subscription starts
    private final AtomicBoolean holdTheNextQuestionForThePresent = new AtomicBoolean();
    private volatile @Nullable Thread lostItsHistory;
    private final CountDownLatch askedForThePresent = new CountDownLatch(1);
    private final CountDownLatch answerThePresent = new CountDownLatch(1);

    @BeforeEach
    void connect() {
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        eventCollection = "events-" + UUID.randomUUID();
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(holdingTheQuestionForThePresent()).build());
        String database = requireNonNull(connectionString.getDatabase());
        template = new MongoTemplate(client, database);
        eventStore = new SpringMongoEventStore(template, new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection)
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING).build());
        storage = new SpringMongoCheckpointStorage(template, "checkpoints-" + UUID.randomUUID());
    }

    @AfterEach
    void shutdown() {
        releaseTheSlowAction.countDown();
        answerThePresent.countDown();
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        started.forEach(SubscriptionModel::shutdown);
        client.close();
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void an_action_that_returns_after_a_resume_moved_the_subscription_back_does_not_move_it_forward_again(Model model) {
        // Given an action that is still running on the second event when the pause returns
        CheckpointAwareSubscriptionModel subscriptionModel = model(model);
        NameDefined first = nameDefined();
        NameDefined slow = nameDefined();
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        AtomicBoolean slowActionReturned = new AtomicBoolean();
        subscriptionModel.subscribe("late", null, StartAt.now(), slowOn(slow, handled, slowActionReturned)).waitUntilStarted(Duration.ofSeconds(10));
        eventStore.write("first", serialize(first));
        eventStore.write("slow", serialize(slow));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(first.eventId(), slow.eventId()));
        Checkpoint afterTheFirst = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(handled.getFirst());
        subscriptionModel.pauseSubscription("late");
        assertThat(slowActionReturned).as("the slow action returned before the pause did").isFalse();

        // When the subscription is resumed right after the first event, and the slow action returns before the resumed
        // change stream has opened, which the failed first open makes wait for the retry
        failTheNextChangeStreamOpen();
        RepositionableSubscriptions.findIn(subscriptionModel).orElseThrow().resumeSubscription("late", StartAt.checkpoint(afterTheFirst));
        releaseTheSlowAction.countDown();
        await().atMost(5, SECONDS).untilTrue(slowActionReturned);

        // Then the resumed subscription delivers the second event again, from the position it was resumed at
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).as("handled, the resumed run's events last")
                .extracting(CloudEvent::getId).containsExactly(first.eventId(), slow.eventId(), slow.eventId()));
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void an_action_that_returns_after_its_durable_subscription_was_cancelled_stores_no_checkpoint(Model model) {
        // Given a durable subscription whose action is running when it is cancelled
        DurableSubscriptionModel durable = new DurableSubscriptionModel(model(model), storage, new DurableSubscriptionModelConfig(everyEvent()));
        started.addFirst(durable);
        NameDefined slow = nameDefined();
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        AtomicBoolean slowActionReturned = new AtomicBoolean();
        durable.subscribe("cancelled", null, StartAt.now(), slowOn(slow, handled, slowActionReturned)).waitUntilStarted(Duration.ofSeconds(10));
        eventStore.write("slow", serialize(slow));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(slow.eventId()));
        durable.cancelSubscription("cancelled");
        assertThat(storage.read("cancelled")).as("the checkpoint when the cancel returned").isNull();

        // When
        releaseTheSlowAction.countDown();
        await().atMost(5, SECONDS).untilTrue(slowActionReturned);

        // Then no checkpoint is stored, so a later subscribe with the same id starts where it asks to rather than after
        // the event the cancelled one handled
        await().during(Duration.ofMillis(500)).atMost(2, SECONDS).untilAsserted(() -> assertThat(storage.read("cancelled")).as("the checkpoint after the action returned").isNull());
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_stop_interrupted_while_it_waits_for_running_actions_still_pauses_every_subscription(Model model) throws InterruptedException {
        // Given two subscriptions whose actions are running
        CheckpointAwareSubscriptionModel subscriptionModel = model(model);
        CountDownLatch handling = new CountDownLatch(2);
        CopyOnWriteArrayList<CloudEvent> handledByA = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CloudEvent> handledByB = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("a", null, StartAt.now(), blockingOnTheFirstEvent(handling, handledByA)).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.subscribe("b", null, StartAt.now(), blockingOnTheFirstEvent(handling, handledByB)).waitUntilStarted(Duration.ofSeconds(10));
        eventStore.write("first", serialize(nameDefined()));
        assertThat(handling.await(10, SECONDS)).isTrue();

        // When
        CallResult stopping = interruptedWhileItWaits(subscriptionModel::stop);

        // Then
        assertThat(stopping.thrown).as("what stop() threw").isNull();
        assertThat(stopping.interruptedAfterwards).as("the interrupt set on the stopping thread afterwards").isTrue();
        assertThat(List.of("a", "b")).as("paused").allMatch(subscriptionModel::isPaused);
        releaseTheSlowAction.countDown();
        subscriptionModel.start(true);
        NameDefined afterTheStart = nameDefined();
        eventStore.write("after", serialize(afterTheStart));
        await().atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(handledByA).extracting(CloudEvent::getId).contains(afterTheStart.eventId());
            assertThat(handledByB).extracting(CloudEvent::getId).contains(afterTheStart.eventId());
        });
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_pause_interrupted_while_it_waits_for_the_running_action_still_pauses_the_subscription(Model model) throws InterruptedException {
        // Given
        CheckpointAwareSubscriptionModel subscriptionModel = model(model);
        CountDownLatch handling = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe("paused", null, StartAt.now(), blockingOnTheFirstEvent(handling, handled)).waitUntilStarted(Duration.ofSeconds(10));
        eventStore.write("first", serialize(nameDefined()));
        assertThat(handling.await(10, SECONDS)).isTrue();

        // When
        CallResult pausing = interruptedWhileItWaits(() -> subscriptionModel.pauseSubscription("paused"));

        // Then
        assertThat(pausing.thrown).as("what pauseSubscription(..) threw").isNull();
        assertThat(pausing.interruptedAfterwards).as("the interrupt set on the pausing thread afterwards").isTrue();
        assertThat(subscriptionModel.isPaused("paused")).as("paused").isTrue();
        releaseTheSlowAction.countDown();
        subscriptionModel.resumeSubscription("paused").waitUntilStarted(Duration.ofSeconds(10));
        NameDefined afterTheResume = nameDefined();
        eventStore.write("after", serialize(afterTheResume));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).contains(afterTheResume.eventId()));
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_stop_waits_one_second_for_all_running_actions_together_rather_than_one_second_for_each(Model model) throws InterruptedException {
        // Given three subscriptions whose actions run for longer than a stop waits
        CheckpointAwareSubscriptionModel subscriptionModel = model(model);
        CountDownLatch handling = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            subscriptionModel.subscribe("sub-" + i, null, StartAt.now(), blockingOnTheFirstEvent(handling, new CopyOnWriteArrayList<>())).waitUntilStarted(Duration.ofSeconds(10));
        }
        eventStore.write("first", serialize(nameDefined()));
        assertThat(handling.await(10, SECONDS)).isTrue();

        // When
        long startedStopping = System.nanoTime();
        subscriptionModel.stop();
        Duration stopping = Duration.ofNanos(System.nanoTime() - startedStopping);

        // Then
        assertThat(stopping).as("time to stop three subscriptions whose actions outlast the wait").isLessThan(Duration.ofMillis(1800));
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void no_attempt_of_the_action_starts_after_cancelSubscription_has_returned(Model model) throws InterruptedException {
        // Given an action that fails, and a retry that waits until the subscription is cancelled
        CountDownLatch waitingToRetry = new CountDownLatch(1);
        CountDownLatch cancelReturned = new CountDownLatch(1);
        AtomicBoolean attemptAfterTheCancel = new AtomicBoolean();
        CheckpointAwareSubscriptionModel subscriptionModel = model(model, RetryStrategy.fixed(Duration.ofMillis(10)).onBeforeRetry(__ -> {
            waitingToRetry.countDown();
            try {
                cancelReturned.await(10, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        subscriptionModel.subscribe("cancelled", null, StartAt.now(), __ -> {
            if (cancelReturned.getCount() == 0) {
                attemptAfterTheCancel.set(true);
            }
            throw new IllegalStateException("expected");
        }).waitUntilStarted(Duration.ofSeconds(10));
        eventStore.write("first", serialize(nameDefined()));
        assertThat(waitingToRetry.await(10, SECONDS)).isTrue();

        // When
        subscriptionModel.cancelSubscription("cancelled");
        cancelReturned.countDown();

        // Then
        await().during(Duration.ofMillis(500)).atMost(2, SECONDS).untilAsserted(() -> assertThat(attemptAfterTheCancel).as("an attempt started after cancelSubscription(..) returned").isFalse());
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_retry_that_a_cancel_ends_before_its_next_attempt_tells_the_error_listener_nothing_about_that_attempt(Model model) throws InterruptedException {
        // Given an action that fails, and a retry that waits until the subscription is cancelled
        CountDownLatch waitingToRetry = new CountDownLatch(1);
        CountDownLatch cancelReturned = new CountDownLatch(1);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        CheckpointAwareSubscriptionModel subscriptionModel = model(model, RetryStrategy.fixed(Duration.ofMillis(10)).onBeforeRetry(__ -> {
            waitingToRetry.countDown();
            try {
                cancelReturned.await(10, SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).onError((__, e) -> errors.add(e)));
        subscriptionModel.subscribe("cancelled", null, StartAt.now(), __ -> {
            throw new IllegalStateException("expected");
        }).waitUntilStarted(Duration.ofSeconds(10));
        eventStore.write("first", serialize(nameDefined()));
        assertThat(waitingToRetry.await(10, SECONDS)).isTrue();

        // When
        subscriptionModel.cancelSubscription("cancelled");
        cancelReturned.countDown();

        // Then the listener has heard of the failure of the action and of nothing else
        await().during(Duration.ofMillis(500)).atMost(2, SECONDS).untilAsserted(() -> assertThat(errors).as("the errors the listener was told of")
                .extracting(Throwable::getMessage).containsExactly("expected"));
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_run_that_lost_its_history_stores_no_restart_position_once_its_subscription_was_cancelled_and_subscribed_again(Model model) throws InterruptedException {
        // Given a durable subscription whose history is lost when its change stream opens, and whose run is waiting for
        // the present to restart from
        DurableSubscriptionModel durable = new DurableSubscriptionModel(model(model, RetryStrategy.fixed(Duration.ofSeconds(1)), true), storage, new DurableSubscriptionModelConfig(everyEvent()));
        started.addFirst(durable);
        holdTheNextQuestionForThePresent.set(true);
        loseTheHistoryOfTheNextChangeStreamOpen();
        durable.subscribe("lost", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        assertThat(askedForThePresent.await(10, SECONDS)).as("asked for the present").isTrue();

        // When the subscription is cancelled and subscribed again with the same id before the answer comes
        durable.cancelSubscription("lost");
        durable.subscribe("lost", null, StartAt.subscriptionModelDefault(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        Checkpoint whereTheNewSubscriptionStarted = storage.read("lost");
        assertThat(whereTheNewSubscriptionStarted).as("the position the new subscription recorded").isNotNull();
        answerThePresent.countDown();

        // Then the checkpoint stays where the new subscription started. The present the cancelled run is answered
        // comes after that, so a process restart from it would skip what the new subscription has not handled yet
        await().during(Duration.ofSeconds(1)).atMost(2, SECONDS).untilAsserted(() -> assertThat(storage.read("lost"))
                .as("the checkpoint once the cancelled run has its answer").isEqualTo(whereTheNewSubscriptionStarted));
    }

    @ParameterizedTest
    @EnumSource(Model.class)
    void a_run_that_lost_its_history_stores_no_restart_position_once_a_resume_has_replaced_it(Model model) throws InterruptedException {
        // Given a durable subscription whose history is lost when its change stream opens, and whose run is waiting for
        // the present to restart from
        DurableSubscriptionModel durable = new DurableSubscriptionModel(model(model, RetryStrategy.fixed(Duration.ofSeconds(1)), true), storage, new DurableSubscriptionModelConfig(everyEvent()));
        started.addFirst(durable);
        holdTheNextQuestionForThePresent.set(true);
        loseTheHistoryOfTheNextChangeStreamOpen();
        durable.subscribe("lost", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        Checkpoint recorded = storage.read("lost");
        assertThat(askedForThePresent.await(10, SECONDS)).as("asked for the present").isTrue();

        // When the subscription is paused and resumed before the answer comes
        durable.pauseSubscription("lost");
        durable.resumeSubscription("lost").waitUntilStarted(Duration.ofSeconds(10));
        answerThePresent.countDown();

        // Then the checkpoint stays where the resumed run started. The present the replaced run is answered comes
        // after that, so a process restart from it would skip what the resumed run has not handled yet
        await().during(Duration.ofSeconds(1)).atMost(2, SECONDS).untilAsserted(() -> assertThat(storage.read("lost"))
                .as("the checkpoint once the replaced run has its answer").isEqualTo(recorded));
    }

    private record CallResult(@Nullable Throwable thrown, boolean interruptedAfterwards) {
    }

    // Calls it on a thread of its own and interrupts that thread once it waits
    private static CallResult interruptedWhileItWaits(Runnable call) throws InterruptedException {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptedAfterwards = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            try {
                call.run();
            } catch (Throwable t) {
                thrown.set(t);
            }
            interruptedAfterwards.set(Thread.currentThread().isInterrupted());
        });
        caller.start();
        await().atMost(5, SECONDS).until(() -> caller.getState() == Thread.State.TIMED_WAITING);
        caller.interrupt();
        caller.join(SECONDS.toMillis(10));
        assertThat(caller.isAlive()).as("still calling").isFalse();
        return new CallResult(thrown.get(), interruptedAfterwards.get());
    }

    private Consumer<CloudEvent> blockingOnTheFirstEvent(CountDownLatch handling, List<CloudEvent> handled) {
        AtomicBoolean blocked = new AtomicBoolean();
        return cloudEvent -> {
            handled.add(cloudEvent);
            if (blocked.compareAndSet(false, true)) {
                handling.countDown();
                try {
                    releaseTheSlowAction.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Consumer<CloudEvent> slowOn(NameDefined slow, List<CloudEvent> handled, AtomicBoolean slowActionReturned) {
        return cloudEvent -> {
            handled.add(cloudEvent);
            if (cloudEvent.getId().equals(slow.eventId()) && !slowActionReturned.get()) {
                try {
                    releaseTheSlowAction.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                slowActionReturned.set(true);
            }
        };
    }

    private CheckpointAwareSubscriptionModel model(Model model) {
        // A second between the attempts to open a change stream, which is how long the slow action has to return in
        return model(model, RetryStrategy.fixed(Duration.ofSeconds(1)));
    }

    private CheckpointAwareSubscriptionModel model(Model model, RetryStrategy retryStrategy) {
        return model(model, retryStrategy, false);
    }

    private CheckpointAwareSubscriptionModel model(Model model, RetryStrategy retryStrategy, boolean restartAfterLostHistory) {
        CheckpointAwareSubscriptionModel subscriptionModel = switch (model) {
            case SPRING -> new SpringMongoSubscriptionModel(template, SpringMongoSubscriptionModelConfig.withConfig(eventCollection, TimeRepresentation.RFC_3339_STRING)
                    .maxAwaitTime(Duration.ofMillis(100)).retryStrategy(retryStrategy).restartSubscriptionsOnChangeStreamHistoryLost(restartAfterLostHistory));
            case NATIVE -> new NativeMongoSubscriptionModel(template.getDb(), eventCollection, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool(),
                    NativeMongoSubscriptionModelConfig.withConfig().maxAwaitTime(Duration.ofMillis(100)).retryStrategy(retryStrategy).restartSubscriptionsOnChangeStreamHistoryLost(restartAfterLostHistory));
        };
        started.add(subscriptionModel);
        return subscriptionModel;
    }

    // BadValue, which neither the driver nor the model treats as lost history, so the model opens again after its
    // retry strategy's wait
    private void failTheNextChangeStreamOpen() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 2)));
    }

    // ChangeStreamHistoryLost, after which the model restarts from the present when it is configured to
    private void loseTheHistoryOfTheNextChangeStreamOpen() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("errorCode", 286)));
    }

    private CommandListener holdingTheQuestionForThePresent() {
        return new CommandListener() {
            @Override
            public void commandFailed(CommandFailedEvent event) {
                if (event.getCommandName().equals("aggregate") && holdTheNextQuestionForThePresent.get()) {
                    lostItsHistory = Thread.currentThread();
                }
            }

            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("ping") && Thread.currentThread() == lostItsHistory && holdTheNextQuestionForThePresent.compareAndSet(true, false)) {
                    askedForThePresent.countDown();
                    try {
                        answerThePresent.await(10, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
    }

    private static NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0), "name", "value");
    }

    private static List<CloudEvent> serialize(NameDefined event) {
        return List.of(CloudEventBuilder.v1().withId(event.eventId()).withSource(URI.create("http://name"))
                .withType(event.getClass().getName()).withTime(toLocalDateTime(event.timestamp()).atOffset(UTC)).withSubject(event.name())
                .withDataContentType("application/json").withData(unchecked(new ObjectMapper()::writeValueAsBytes).apply(event)).build());
    }
}
