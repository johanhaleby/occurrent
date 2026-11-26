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

package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * {@code start(true)} reads what each of its resumes reads from checkpoint storage before its turn, and waits for those
 * reads up to ten seconds. A subscription whose read doesn't answer in time is resumed after the turn. These tests make
 * that wait expire, with the model stopped (so the call runs on the calling thread) and with the call made from a live
 * action (so it runs on the lifecycle thread).
 * <p>
 * The wait is over once the turn has begun, which is when the live delegate runs again for a stopped model. For a model
 * that already runs there is nothing to see, so the tests let more than the wait pass after the reads were entered.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(100)
class CatchupLifecycleStartWithSlowStorageTest {

    private static final String A = "a";
    private static final String X = "x";
    private static final String Y = "y";
    private static final Duration BOUND = Duration.ofSeconds(25);
    // A little more than the ten seconds start(true) waits for the reads made before its turn
    private static final Duration MORE_THAN_THE_WAIT_FOR_READS = Duration.ofMillis(11_500);
    private static final String X_FAILED = "the resume of x failed on purpose";

    private final StorageThatHoldsReads storage = new StorageThatHoldsReads();
    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(new DurableSubscriptionModel(live, storage), history,
            new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
    private final Map<String, List<String>> delivered = new ConcurrentHashMap<>();
    private final AtomicReference<@Nullable Throwable> thrownByTheAction = new AtomicReference<>();
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);

    @BeforeEach
    void capture_the_log() {
        logged.start();
        rootLogger.addAppender(logged);
    }

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        rootLogger.detachAppender(logged);
        storage.releaseAll();
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @ParameterizedTest(name = "when {0} is the one whose read never answers and {1} the one whose read answers late")
    @CsvSource({"x,y", "y,x"})
    void a_start_made_with_the_model_stopped_resumes_a_subscription_whose_read_answered_late_while_the_resume_of_another_whose_read_never_answers_still_waits(String stuck, String late) throws Exception {
        // Given two paused subscriptions in a stopped model, and a storage that holds back the read of both
        givenPaused(stuck);
        givenPaused(late);
        model.stop();
        storage.hold(stuck);
        storage.hold(late);

        // When the model is started, and its wait for the reads expires, which is when the live delegate runs again
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));
        assertThat(storage.awaitEntered(stuck)).as("whether the start read the storage for " + stuck).isTrue();
        assertThat(storage.awaitEntered(late)).as("whether the start read the storage for " + late).isTrue();
        await().atMost(BOUND).untilAsserted(() -> assertThat(live.isRunning()).as("whether the live delegate runs, which it does once the turn of the start has begun").isTrue());
        assertThat(model.isRunning(late)).as("whether " + late + " runs before its read answered").isFalse();

        // And the read of one answers, while the read of the other never does
        storage.release(late);

        // Then that one resumes and delivers, though the start still waits for the other
        await().atMost(BOUND).untilAsserted(() -> assertThat(model.isRunning(late)).as("whether " + late + " runs once its read answered, though the read of " + stuck + " never did").isTrue());
        publish("live-1");
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredTo(late)).as("the events " + late + " delivered").contains("live-1"));
        assertThat(start.isDone()).as("whether the start returned while the read of " + stuck + " still waited").isFalse();
        assertThat(model.isRunning(stuck)).as("whether " + stuck + " runs while its read waits").isFalse();

        // And once the read of the other answers, the start returns and that one resumes too
        storage.release(stuck);
        assertThat(start).as("the start once every read answered").succeedsWithin(BOUND);
        assertThat(model.isRunning(stuck)).as("whether " + stuck + " runs once its read answered").isTrue();
    }

    @ParameterizedTest(name = "when {0} is the one whose read never answers and {1} the one whose read answers late")
    @CsvSource({"x,y", "y,x"})
    void a_start_made_from_a_live_action_resumes_a_subscription_whose_read_answered_late_without_waiting_behind_the_resume_of_another_whose_read_never_answers(String stuck, String late) throws Exception {
        // Given a running subscription whose action starts the model, two paused subscriptions, and a storage that holds back the read of both
        givenTheActionOfAStartsTheModel();
        givenPaused(stuck);
        givenPaused(late);
        storage.hold(stuck);
        storage.hold(late);

        // When a receives an event, and the wait of the start for the reads expires
        publish("go");
        assertThat(storage.awaitEntered(stuck)).as("whether the start read the storage for " + stuck).isTrue();
        assertThat(storage.awaitEntered(late)).as("whether the start read the storage for " + late).isTrue();
        sleep(MORE_THAN_THE_WAIT_FOR_READS);
        assertThat(thrownByTheAction.get()).as("what the action threw").isNull();
        assertThat(model.isRunning(late)).as("whether " + late + " runs before its read answered").isFalse();

        // And the read of one answers, while the read of the other never does
        long releasedAt = System.nanoTime();
        storage.release(late);

        // Then that one resumes soon after, and not only once the call for the other is withdrawn ten seconds after its turn
        await().pollInterval(Duration.ofMillis(50)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(model.isRunning(late))
                .as("whether " + late + " runs within five seconds of its read answering, as it does when the resume of " + stuck + " holds nothing up").isTrue());
        assertThat(Duration.ofNanos(System.nanoTime() - releasedAt)).as("the time " + late + " took to resume after its read answered").isLessThan(Duration.ofSeconds(5));
        assertThat(model.isRunning(stuck)).as("whether " + stuck + " runs while its read waits").isFalse();
        publish("live-1");
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredTo(late)).as("the events " + late + " delivered").contains("live-1"));
    }

    @Test
    void a_start_made_with_the_model_stopped_still_resumes_a_subscription_whose_read_answered_late_when_the_resume_of_another_in_the_turn_throws_and_throws_that_failure() throws Exception {
        // Given a stopped model with x and y paused, a read of y the storage holds back, and a live delegate whose first resume throws, which is that of x in the turn
        givenPaused(X);
        givenPaused(Y);
        model.stop();
        storage.hold(Y);
        failTheFirstResumeOfTheLiveDelegate();

        // When the model is started, and its wait for the reads expires, which is when the live delegate runs again
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));
        assertThat(storage.awaitEntered(Y)).as("whether the start read the storage for y").isTrue();
        await().atMost(BOUND).untilAsserted(() -> assertThat(live.isRunning()).as("whether the live delegate runs, which it does once the turn of the start has begun").isTrue());

        // And the read of y answers
        storage.release(Y);

        // Then the start throws the failure of x, and y was resumed after the turn all the same
        assertThat(catchThrowable(() -> start.get(BOUND.toSeconds(), TimeUnit.SECONDS))).as("what the start threw")
                .isInstanceOf(ExecutionException.class).hasStackTraceContaining(X_FAILED);
        await().atMost(BOUND).untilAsserted(() -> assertThat(model.isRunning(Y)).as("whether y runs, though the resume of x in the turn threw").isTrue());
        publish("live-1");
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredTo(Y)).as("the events y delivered").contains("live-1"));
    }

    @Test
    void a_start_made_from_a_live_action_still_resumes_a_subscription_whose_read_answered_late_when_the_resume_of_another_in_the_turn_throws_and_logs_that_failure() throws Exception {
        // Given a running subscription whose action starts the model, x and y paused, a read of y the storage holds back, and a live delegate whose first resume throws, which is that of x in the turn
        givenTheActionOfAStartsTheModel();
        givenPaused(X);
        givenPaused(Y);
        storage.hold(Y);
        failTheFirstResumeOfTheLiveDelegate();

        // When a receives an event, and the wait of the start for the reads expires
        publish("go");
        assertThat(storage.awaitEntered(Y)).as("whether the start read the storage for y").isTrue();
        sleep(MORE_THAN_THE_WAIT_FOR_READS);
        assertThat(thrownByTheAction.get()).as("what the action threw, a start from an action being handed over").isNull();

        // And the read of y answers
        storage.release(Y);

        // Then y is resumed after the turn and delivers, and the failure of x reached the log and not the action
        await().atMost(BOUND).untilAsserted(() -> assertThat(model.isRunning(Y)).as("whether y runs, though the resume of x in the turn threw").isTrue());
        publish("live-1");
        await().atMost(BOUND).untilAsserted(() -> assertThat(deliveredTo(Y)).as("the events y delivered").contains("live-1"));
        await().atMost(BOUND).untilAsserted(() -> assertThat(logged.list).as("the log events once the start from the action ran").anySatisfy(event -> assertThat(describe(event)).contains(X_FAILED)));
        assertThat(thrownByTheAction.get()).as("what the action threw in the end").isNull();
    }

    private void givenPaused(String subscriptionId) {
        model.subscribe(subscriptionId, null, StartAt.now(), event -> delivered.computeIfAbsent(subscriptionId, id -> new CopyOnWriteArrayList<>()).add(event.getId()));
        model.pauseSubscription(subscriptionId);
        assertThat(model.isPaused(subscriptionId)).as("whether " + subscriptionId + " is paused before the test starts").isTrue();
    }

    private void givenTheActionOfAStartsTheModel() {
        model.subscribe(A, null, StartAt.now(), event -> {
            if (event.getId().equals("go")) {
                try {
                    model.start(true);
                } catch (Throwable e) {
                    thrownByTheAction.set(e);
                }
            }
        });
    }

    private void failTheFirstResumeOfTheLiveDelegate() {
        AtomicInteger resumes = new AtomicInteger();
        live.beforeResume = () -> {
            if (resumes.getAndIncrement() == 0) {
                throw new IllegalStateException(X_FAILED);
            }
        };
    }

    private void publish(String eventId) {
        CheckpointAwareCloudEvent event = new CheckpointAwareCloudEvent(cloudEvent(eventId), new StringBasedCheckpoint("checkpoint-" + eventId));
        CompletableFuture.runAsync(() -> live.publish(event), daemonExecutor("live-delivery-" + eventId));
    }

    private List<String> deliveredTo(String subscriptionId) {
        return delivered.getOrDefault(subscriptionId, List.of());
    }

    private static String describe(ILoggingEvent event) {
        StringBuilder text = new StringBuilder(event.getFormattedMessage());
        for (IThrowableProxy cause = event.getThrowableProxy(); cause != null; cause = cause.getCause()) {
            text.append(' ').append(cause.getMessage());
        }
        return text.toString();
    }

    private static void sleep(Duration duration) throws InterruptedException {
        Thread.sleep(duration.toMillis());
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    /**
     * Holds back every read of a subscription, once held, until it is released
     */
    private static final class StorageThatHoldsReads extends InMemoryCheckpointStorage {
        private final Map<String, CountDownLatch> released = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> entered = new ConcurrentHashMap<>();

        void hold(String subscriptionId) {
            entered.put(subscriptionId, new CountDownLatch(1));
            released.put(subscriptionId, new CountDownLatch(1));
        }

        boolean awaitEntered(String subscriptionId) throws InterruptedException {
            return entered.get(subscriptionId).await(20, TimeUnit.SECONDS);
        }

        void release(String subscriptionId) {
            released.get(subscriptionId).countDown();
        }

        void releaseAll() {
            released.values().forEach(CountDownLatch::countDown);
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            CountDownLatch release = released.get(subscriptionId);
            if (release != null) {
                entered.get(subscriptionId).countDown();
                try {
                    release.await(80, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.read(subscriptionId);
        }
    }
}
