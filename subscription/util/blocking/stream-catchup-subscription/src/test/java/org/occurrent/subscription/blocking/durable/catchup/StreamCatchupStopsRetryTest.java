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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A cancel or a stop that comes while the replay of a catch-up is inside a retry the wrapped model runs for
 * globalCheckpoint or canResumeFrom ends that retry. A cancelled replay ends as cancelled and a stopped one is parked
 * and runs again on start(true), and neither logs at ERROR.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupStopsRetryTest {

    private static final String ID = "subscription";
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("live-start");

    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;
    private Logger catchupLog;
    private Level originalLevel;
    private InMemoryEventStore eventStore;
    private RetryingLiveModel live;
    private StreamCatchupSubscriptionModel catchup;

    /**
     * Where the replay of a catch-up asks the wrapped model, and so which call the wrapped model retries.
     */
    enum Site {
        GLOBAL_CHECKPOINT_BEFORE_THE_REPLAY(true, false, false),
        CAN_RESUME_FROM_AFTER_THE_REPLAY(false, false, false),
        CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY(false, true, false),
        CAN_RESUME_FROM_AT_THE_START_OF_A_TIME_REPLAY(false, true, true);

        private final boolean retriesGlobalCheckpoint;
        private final boolean startsFromAStoredCheckpoint;
        private final boolean replaysByTime;

        Site(boolean retriesGlobalCheckpoint, boolean startsFromAStoredCheckpoint, boolean replaysByTime) {
            this.retriesGlobalCheckpoint = retriesGlobalCheckpoint;
            this.startsFromAStoredCheckpoint = startsFromAStoredCheckpoint;
            this.replaysByTime = replaysByTime;
        }
    }

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
        catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        originalLevel = catchupLog.getLevel();
        catchupLog.setLevel(Level.DEBUG);
        logged.list = new CopyOnWriteArrayList<>();
        logged.start();
        catchupLog.addAppender(logged);
    }

    @AfterEach
    void shut_down() {
        if (live != null) {
            live.stopRetrying();
        }
        if (catchup != null) {
            catchup.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
        catchupLog.detachAppender(logged);
        logged.stop();
        catchupLog.setLevel(originalLevel);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_cancel_during_the_retry_ends_it_and_the_replay_ends_as_cancelled_without_an_error_logged(Site site) {
        // Given
        Subscription subscription = subscribeWhileTheWrappedModelRetries(site);

        // When
        catchup.cancelSubscription(ID);

        // Then
        assertThat(live.retryEnded).as("the retry of the wrapped model ended once the subscription was cancelled").succeedsWithin(WAIT);
        AtomicBoolean started = new AtomicBoolean(true);
        assertThatCode(() -> started.set(subscription.waitUntilStarted(WAIT))).as("waiting until the subscription started").doesNotThrowAnyException();
        assertThat(started).as("the subscription started").isFalse();
        awaitTheLogLineForTheEndedRetry();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isFalse(),
                () -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("level of the line logged for the ended retry").isEqualTo(Level.DEBUG),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_stop_during_the_retry_ends_it_and_parks_the_replay_without_an_error_logged_and_start_runs_it_again_to_go_live(Site site) {
        // Given
        subscribeWhileTheWrappedModelRetries(site);

        // When
        catchup.stop();

        // Then
        assertThat(live.retryEnded).as("the retry of the wrapped model ended once the model was stopped").succeedsWithin(WAIT);
        awaitTheLogLineForTheEndedRetry();
        assertAll(
                () -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isTrue(),
                () -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("level of the line logged for the ended retry").isEqualTo(Level.DEBUG),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
        assertThatStartRunsTheParkedReplayAndTheSubscriptionGoesLive();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_stop_that_has_marked_the_model_stopped_but_not_yet_parked_the_replay_when_the_retry_ends_leaves_the_replay_parked_by_the_replay_itself(Site site) {
        // Given a stop held at its claim, after it marked the model stopped and before it parks the replay
        subscribeWhileTheWrappedModelRetries(site);
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            stopping = CompletableFuture.runAsync(() -> catchup.stop());

            // When the retry ends while the stop is held
            assertThat(live.retryEnded).as("the retry of the wrapped model ended once the model was stopped").succeedsWithin(WAIT);

            // Then
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked by the replay while the stop is held").isTrue());
            assertThat(stopping).as("the stop").isNotDone();
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
        awaitTheLogLineForTheEndedRetry();
        assertAll(
                () -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("level of the line logged for the ended retry").isEqualTo(Level.DEBUG),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
        assertThatStartRunsTheParkedReplayAndTheSubscriptionGoesLive();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_shutdown_during_the_retry_ends_it_and_the_replay_ends_as_cancelled_without_an_error_logged(Site site) {
        // Given
        Subscription subscription = subscribeWhileTheWrappedModelRetries(site);

        // When
        catchup.shutdown();

        // Then
        assertThat(live.retryEnded).as("the retry of the wrapped model ended once the model was shut down").succeedsWithin(WAIT);
        AtomicBoolean started = new AtomicBoolean(true);
        assertThatCode(() -> started.set(subscription.waitUntilStarted(WAIT))).as("waiting until the subscription started").doesNotThrowAnyException();
        assertThat(started).as("the subscription started").isFalse();
        awaitTheLogLineForTheEndedRetry();
        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isFalse(),
                () -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("level of the line logged for the ended retry").isEqualTo(Level.DEBUG),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_stop_that_the_replay_sees_before_a_start_returns_relaunches_the_replay_itself_when_the_start_found_nothing_parked(Site site) {
        // Given a stop that marked the model stopped and is held at its claim, and the replay thread held before it takes the handover lock
        subscribeWhileTheWrappedModelRetries(site);
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            try (AbstractCatchupSubscriptionModel.HandoverLock alsoIgnored = catchup.lockHandover(ID)) {
                stopping = CompletableFuture.runAsync(() -> catchup.stop());
                assertThat(live.retryEnded).as("the retry of the wrapped model ended once the model was stopped").succeedsWithin(WAIT);

                // When start(true) returns, having found nothing parked
                catchup.start(true);
                assertThat(catchup.hasParkedReplay(ID)).as("replay parked by the start").isFalse();
            }

            // Then the replay thread relaunches the replay, while the stop is still held
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.isRunning(ID)).as("running").isTrue());
            assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isFalse();
            assertThat(stopping).as("the stop").isNotDone();
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
        // The stop went on to stop the live delegate after the start, so this start is what brings it up again
        assertThatStartRunsTheParkedReplayAndTheSubscriptionGoesLive();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_retry_that_gives_up_without_asking_stillWanted_while_a_stop_has_not_parked_the_replay_leaves_it_parked_and_start_runs_it_again_to_go_live(Site site) {
        // Given a stop held at its claim, after it marked the model stopped and before it parks the replay
        subscribeWhileTheWrappedModelRetries(site, true);
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            stopping = CompletableFuture.runAsync(() -> catchup.stop());
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped").isTrue());

            // When the retry gives up on its last attempt while the stop is held
            live.stopRetrying();
            assertThat(live.retryEnded).as("the retry of the wrapped model gave up").succeedsWithin(WAIT);

            // Then
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked by the replay while the stop is held").isTrue());
            assertThat(stopping).as("the stop").isNotDone();
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
        assertThat(catchup.hasParkedReplay(ID)).as("replay parked after the stop").isTrue();
        awaitTheLogLineForTheEndedRetry();
        assertAll(
                () -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("level of the line logged for the ended retry").isEqualTo(Level.DEBUG),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
        live.stopRetrying();
        catchup.start(true);
        assertThatTheSubscriptionGoesLive();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Site.class)
    void a_retry_that_gives_up_without_asking_stillWanted_after_a_stop_and_then_a_start_came_in_during_it_ends_and_the_replay_runs_again_to_go_live(Site site) {
        // Given a stop that marked the model stopped and is held at its claim, and a start that has since come in
        subscribeWhileTheWrappedModelRetries(site, true);
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            stopping = CompletableFuture.runAsync(() -> catchup.stop());
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped").isTrue());
            catchup.start(true);
            assertThat(catchup.stopped).as("the model is stopped after the start").isFalse();

            // When the retry gives up on its last attempt, after the start
            live.stopRetrying();
            assertThat(live.retryEnded).as("the retry of the wrapped model gave up").succeedsWithin(WAIT);

            // Then the failure is the one that ended the retry, so it is logged at DEBUG and the replay runs again
            awaitTheLogLineForTheEndedRetry();
            assertAll(
                    () -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("level of the line logged for the ended retry").isEqualTo(Level.DEBUG),
                    () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
            );
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
        // The stop went on to stop the live delegate after the start, so this start is what brings it up again
        catchup.start(true);
        assertThatTheSubscriptionGoesLive();
    }

    @Test
    void a_failure_of_the_replay_itself_while_a_stop_has_not_parked_it_leaves_the_replay_parked_by_the_replay_and_start_runs_it_again_to_go_live() {
        Site site = Site.GLOBAL_CHECKPOINT_BEFORE_THE_REPLAY;
        // Given a replay whose action fails once an event reaches it, and a wrapped model that does not retry
        AtomicBoolean actionFails = new AtomicBoolean(true);
        CompletableFuture<Void> inTheAction = new CompletableFuture<>();
        CompletableFuture<Void> actionMayFail = new CompletableFuture<>();
        StartAt startAt = createModelsFor(site, false);
        live.doesNotRetry();
        Consumer<CloudEvent> delivering = toDomainEvents(received);
        catchup.subscribe(ID, startAt, cloudEvent -> {
            if (actionFails.get()) {
                inTheAction.complete(null);
                actionMayFail.join();
                actionFails.set(false);
                throw new IllegalStateException("the action failed");
            }
            delivering.accept(cloudEvent);
        });
        assertThat(inTheAction).as("the replay reached the action").succeedsWithin(WAIT);

        // And a stop held at its claim, after it marked the model stopped and before it parks the replay
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            stopping = CompletableFuture.runAsync(() -> catchup.stop());
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped").isTrue());

            // When the action fails, which the wrapped model did not throw for the stop
            actionMayFail.complete(null);

            // Then the replay parks itself, as the stop would
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked by the replay while the stop is held").isTrue());
            assertThat(stopping).as("the stop").isNotDone();
            assertThat(errorsLogged()).as("errors logged").singleElement().satisfies(event -> assertThat(event.getFormattedMessage()).contains("it runs again once the subscription is resumed or start(true) is called on this model"));
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
        assertThat(catchup.hasParkedReplay(ID)).as("replay parked after the stop").isTrue();
        catchup.start(true);
        assertThatTheSubscriptionGoesLive();
    }

    @Test
    void a_stillWanted_that_answered_false_does_not_make_a_later_failure_of_the_replay_an_ended_retry_once_canResumeFrom_returned() {
        Site site = Site.CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY;
        // Given a wrapped model whose canResumeFrom reads stillWanted while a stop has marked the model stopped, and then
        // answers true, and a replay whose action fails once an event reaches it
        AtomicBoolean actionFails = new AtomicBoolean(true);
        CompletableFuture<Void> readStillWanted = new CompletableFuture<>();
        CompletableFuture<Void> mayAnswer = new CompletableFuture<>();
        CompletableFuture<Boolean> answeredByStillWanted = new CompletableFuture<>();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        StartAt startAt = createModelsFor(site, false);
        live.doesNotRetry();
        live.readsStillWantedInsteadOfRetrying(stillWanted -> {
            if (firstCall.getAndSet(false)) {
                readStillWanted.join();
                answeredByStillWanted.complete(stillWanted.getAsBoolean());
                mayAnswer.join();
            }
        });
        Consumer<CloudEvent> delivering = toDomainEvents(received);
        Subscription subscription = catchup.subscribe(ID, startAt, cloudEvent -> {
            if (actionFails.getAndSet(false)) {
                throw new IllegalStateException("the action failed");
            }
            delivering.accept(cloudEvent);
        });
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            stopping = CompletableFuture.runAsync(() -> catchup.stop());
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped").isTrue());
            readStillWanted.complete(null);
            assertThat(answeredByStillWanted).as("what stillWanted answered while the model was stopped").succeedsWithin(WAIT).isEqualTo(false);

            // When a start comes in before canResumeFrom returns, and the replay then goes on and fails
            catchup.start(true);
            assertThat(catchup.stopped).as("the model is stopped after the start").isFalse();
            mayAnswer.complete(null);

            // Then it is an ordinary failure of a replay a subscribe started
            await().atMost(WAIT).untilAsserted(() -> assertThat(errorsLogged()).as("errors logged").singleElement().satisfies(event -> assertThat(event.getFormattedMessage()).contains("did not go live")));
            assertThatThrownBy(() -> subscription.waitUntilStarted(WAIT)).as("waiting until the subscription started").isInstanceOf(IllegalStateException.class).hasMessage("the action failed");
            assertAll(
                    () -> assertThat(logged.list).as("lines logged for an ended retry").noneMatch(event -> event.getFormattedMessage().contains("stop retrying")),
                    () -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isFalse(),
                    () -> assertThat(catchup.isRunning(ID)).as("running").isFalse()
            );
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
    }

    @Test
    void a_failure_of_a_wrapped_model_call_that_a_start_alone_came_in_during_is_an_ordinary_failure_of_the_replay() {
        // Given a replay a subscribe started, in a wrapped model call that gives up without asking stillWanted, and no stop
        Site site = Site.CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY;
        Subscription subscription = subscribeWhileTheWrappedModelRetries(site, true);

        // When a start comes in on the model that is already started, and the call then throws
        catchup.start(true);
        live.stopRetrying();
        assertThat(live.retryEnded).as("the retry of the wrapped model gave up").succeedsWithin(WAIT);

        // Then it is an ordinary failure of a replay a subscribe started
        await().atMost(WAIT).untilAsserted(() -> assertThat(errorsLogged()).as("errors logged").singleElement().satisfies(event -> assertThat(event.getFormattedMessage()).contains("did not go live")));
        assertThatThrownBy(() -> subscription.waitUntilStarted(WAIT)).as("waiting until the subscription started").isSameAs(live.retryEnded.getNow(null));
        assertAll(
                () -> assertThat(logged.list).as("lines logged for an ended retry").noneMatch(event -> event.getFormattedMessage().contains("stop retrying")),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isFalse(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse()
        );
    }

    @Test
    void a_canResumeFrom_that_answers_false_once_a_stop_came_during_it_does_not_hold_the_subscription_as_unable_to_resume_and_start_runs_the_replay_again_to_go_live() {
        Site site = Site.CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY;
        // Given a wrapped model whose first canResumeFrom answers false once a stop has marked the model stopped, and true after
        CompletableFuture<Void> readStillWanted = new CompletableFuture<>();
        CompletableFuture<Boolean> answeredByStillWanted = new CompletableFuture<>();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        createModelsFor(site, false);
        // A replay origin after the beginning, so a false that is read as lost history holds the subscription as unable to resume
        StartAt startAt = StartAt.checkpoint(new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, LIVE_START)));
        live.doesNotRetry();
        live.answersFalseOnceAfterReadingStillWanted(stillWanted -> {
            if (Thread.currentThread().isVirtual() && firstCall.getAndSet(false)) {
                readStillWanted.join();
                answeredByStillWanted.complete(stillWanted.getAsBoolean());
            }
        });
        Subscription subscription = catchup.subscribe(ID, startAt, toDomainEvents(received));
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            stopping = CompletableFuture.runAsync(() -> catchup.stop());
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped").isTrue());

            // When canResumeFrom answers false while the stop is held
            readStillWanted.complete(null);
            assertThat(answeredByStillWanted).as("what stillWanted answered while the model was stopped").succeedsWithin(WAIT).isEqualTo(false);

            // Then the replay parks itself instead of the false being read as lost history
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.hasParkedReplay(ID)).as("replay parked by the replay while the stop is held").isTrue());
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);

        // And start runs the replay again, which goes live
        assertThatCode(() -> catchup.start(true)).as("starting the model, which runs the parked replay again").doesNotThrowAnyException();
        AtomicBoolean started = new AtomicBoolean(false);
        assertThatCode(() -> started.set(subscription.waitUntilStarted(WAIT))).as("waiting until the subscription started").doesNotThrowAnyException();
        assertThat(started).as("the subscription started").isTrue();
        assertThatTheSubscriptionGoesLive();
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void a_failure_of_canResumeFrom_for_a_replay_that_was_not_wanted_before_the_call_is_not_an_ordinary_failure_once_a_start_cleared_the_stop_during_it_and_the_replay_runs_again_to_go_live() throws Exception {
        Site site = Site.CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY;
        // Given a subscribe whose replay registers after a stop copied the replays in flight, so the stop does not park it,
        // and a wrapped model whose first canResumeFrom reads stillWanted, waits, and then throws
        CompletableFuture<Boolean> answeredByStillWanted = new CompletableFuture<>();
        CompletableFuture<Void> mayThrow = new CompletableFuture<>();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        StartAt startAt = createModelsFor(site, false);
        live.doesNotRetry();
        live.failsOnceAfterReadingStillWanted(stillWanted -> {
            if (Thread.currentThread().isVirtual() && firstCall.getAndSet(false)) {
                answeredByStillWanted.complete(stillWanted.getAsBoolean());
                mayThrow.join();
            }
        });
        CompletableFuture<Void> inTheListener = new CompletableFuture<>();
        CompletableFuture<Void> mayRegister = new CompletableFuture<>();
        AtomicBoolean firstStart = new AtomicBoolean(true);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (firstStart.getAndSet(false)) {
                    inTheListener.complete(null);
                    mayRegister.join();
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> catchup.subscribe(ID, startAt, toDomainEvents(received)));
        assertThat(inTheListener).as("the subscribe found the model running and is about to register its replay").succeedsWithin(WAIT);
        catchup.stop();
        assertThat(catchup.stopped).as("the model is stopped").isTrue();
        mayRegister.complete(null);
        Subscription subscription = subscribing.get(WAIT.toSeconds(), TimeUnit.SECONDS);
        assertThat(answeredByStillWanted).as("what stillWanted answered in the first canResumeFrom of the replay").succeedsWithin(WAIT).isEqualTo(false);

        // When a start clears the stop before canResumeFrom throws
        catchup.start(true);
        assertThat(catchup.stopped).as("the model is stopped after the start").isFalse();
        mayThrow.complete(null);

        // Then the failure is not read as an ordinary one, so the replay is parked and run again instead of ending as one that did not go live
        AtomicBoolean started = new AtomicBoolean(false);
        assertThatCode(() -> started.set(subscription.waitUntilStarted(WAIT))).as("waiting until the subscription started").doesNotThrowAnyException();
        assertThat(started).as("the subscription started").isTrue();
        assertThatTheSubscriptionGoesLive();
        assertThat(errorsLogged()).as("errors logged").isEmpty();
    }

    @Test
    void a_canResumeFrom_that_answers_false_once_a_stop_came_during_it_and_a_start_cleared_the_stop_before_it_answered_does_not_hold_the_subscription_as_unable_to_resume_and_the_replay_runs_again_to_go_live() {
        Site site = Site.CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY;
        // Given a wrapped model whose first canResumeFrom begins while the replay is wanted, waits, and then answers false
        CompletableFuture<Void> began = new CompletableFuture<>();
        CompletableFuture<Void> mayAnswer = new CompletableFuture<>();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        createModelsFor(site, false);
        // A replay origin after the beginning, so a false that is read as lost history holds the subscription as unable to resume
        StartAt startAt = StartAt.checkpoint(new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, LIVE_START)));
        live.doesNotRetry();
        live.answersFalseOnceAfterReadingStillWanted(stillWanted -> {
            if (Thread.currentThread().isVirtual() && firstCall.getAndSet(false)) {
                began.complete(null);
                mayAnswer.join();
            }
        });
        Subscription subscription = catchup.subscribe(ID, startAt, toDomainEvents(received));
        assertThat(began).as("canResumeFrom began").succeedsWithin(WAIT);
        CompletableFuture<Void> stopping;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            // And a stop held at its claim, after it marked the model stopped and before it parks the replay, and then a start that cleared it
            stopping = CompletableFuture.runAsync(() -> catchup.stop());
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped").isTrue());
            catchup.start(true);
            assertThat(catchup.stopped).as("the model is stopped after the start").isFalse();

            // When canResumeFrom answers false
            mayAnswer.complete(null);

            // Then the false is not read as lost history, so the replay is parked and run again instead
            AtomicBoolean started = new AtomicBoolean(false);
            assertThatCode(() -> started.set(subscription.waitUntilStarted(WAIT))).as("waiting until the subscription started").doesNotThrowAnyException();
            assertThat(started).as("the subscription started").isTrue();
        }
        assertThat(stopping).as("the stop").succeedsWithin(WAIT);
        // The stop went on to stop the live delegate after the start, so this start is what brings it up again
        catchup.start(true);
        assertThatTheSubscriptionGoesLive();
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    @Test
    void a_canResumeFrom_that_answers_false_once_a_failed_start_stopped_the_model_again_during_it_and_another_start_cleared_the_stop_does_not_hold_the_subscription_as_unable_to_resume_and_the_replay_runs_again_to_go_live() {
        Site site = Site.CAN_RESUME_FROM_AT_THE_START_OF_A_POSITION_REPLAY;
        // Given a wrapped model whose first canResumeFrom begins while the replay is wanted, waits, and then answers false
        CompletableFuture<Void> began = new CompletableFuture<>();
        CompletableFuture<Void> mayAnswer = new CompletableFuture<>();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        createModelsFor(site, false);
        // A replay origin after the beginning, so a false that is read as lost history holds the subscription as unable to resume
        StartAt startAt = StartAt.checkpoint(new StringBasedCheckpoint(CatchupCheckpoints.format(2, 1, 3, LIVE_START)));
        live.doesNotRetry();
        live.answersFalseOnceAfterReadingStillWanted(stillWanted -> {
            if (Thread.currentThread().isVirtual() && firstCall.getAndSet(false)) {
                began.complete(null);
                mayAnswer.join();
            }
        });
        // And a model that was stopped, whose next start has its live delegate throw while the delegate does not run
        catchup.stop();
        CompletableFuture<Void> inTheLiveStart = new CompletableFuture<>();
        CompletableFuture<Void> mayFailTheLiveStart = new CompletableFuture<>();
        live.failsTheNextStartOnceAllowed(inTheLiveStart, mayFailTheLiveStart);
        CompletableFuture<Void> failingStart = CompletableFuture.runAsync(() -> catchup.start(true));
        assertThat(inTheLiveStart).as("the start reached the live delegate, after it allowed replays to run").succeedsWithin(WAIT);
        assertThat(catchup.stopped).as("the model is stopped while the live delegate starts").isFalse();

        // And a replay whose canResumeFrom began while the start was under way
        Subscription subscription = catchup.subscribe(ID, startAt, toDomainEvents(received));
        assertThat(began).as("canResumeFrom began").succeedsWithin(WAIT);
        CompletableFuture<Void> secondStart;
        try (AbstractCatchupSubscriptionModel.Claim ignored = catchup.claim(ID)) {
            // And the failed start stopping the model again, held at its claim before it parks the replay, and then another start that cleared that stop
            mayFailTheLiveStart.complete(null);
            await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.stopped).as("the model is stopped again by the failed start").isTrue());
            secondStart = CompletableFuture.runAsync(() -> catchup.start(true));
            assertThat(secondStart).as("the second start").succeedsWithin(WAIT);
            assertThat(catchup.stopped).as("the model is stopped after the second start").isFalse();

            // When canResumeFrom answers false
            mayAnswer.complete(null);

            // Then the false is not read as lost history, so the replay is parked and run again instead
            AtomicBoolean started = new AtomicBoolean(false);
            assertThatCode(() -> started.set(subscription.waitUntilStarted(WAIT))).as("waiting until the subscription started").doesNotThrowAnyException();
            assertThat(started).as("the subscription started").isTrue();
        }
        assertThat(failingStart).as("the failed start").failsWithin(WAIT).withThrowableThat().withMessageContaining("the live delegate failed to start");
        assertThatTheSubscriptionGoesLive();
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(errorsLogged()).as("errors logged").isEmpty()
        );
    }

    private Subscription subscribeWhileTheWrappedModelRetries(Site site) {
        return subscribeWhileTheWrappedModelRetries(site, false);
    }

    private Subscription subscribeWhileTheWrappedModelRetries(Site site, boolean givesUpWithoutAskingStillWanted) {
        StartAt startAt = createModelsFor(site, givesUpWithoutAskingStillWanted);
        Subscription subscription = catchup.subscribe(ID, startAt, toDomainEvents(received));
        assertThat(live.retrying).as("the wrapped model is retrying").succeedsWithin(WAIT);
        return subscription;
    }

    private StartAt createModelsFor(Site site, boolean givesUpWithoutAskingStillWanted) {
        eventStore = site.replaysByTime ? new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition() : new InMemoryEventStore(inMemorySubscriptionModel);
        List.of("event1", "event2", "event3").forEach(name -> write(eventStore, nameDefined(name)));
        live = new RetryingLiveModel(inMemorySubscriptionModel, site.retriesGlobalCheckpoint, givesUpWithoutAskingStillWanted);
        catchup = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
        StartAt startAt;
        if (site.replaysByTime) {
            // Replays from the beginning of time, since an origin after it is refused or held paused on the calling
            // thread, which keeps the one argument canResumeFrom
            String beginningOfTime = TimeBasedCheckpoint.beginningOfTime().asString();
            startAt = StartAt.checkpoint(new StringBasedCheckpoint(CatchupCheckpoints.format(beginningOfTime, beginningOfTime, LIVE_START)));
        } else if (site.startsFromAStoredCheckpoint) {
            startAt = StartAt.checkpoint(new StringBasedCheckpoint(CatchupCheckpoints.format(1, 0, 3, LIVE_START)));
        } else {
            startAt = StartAt.checkpoint(GlobalCheckpoint.of(0));
        }
        return startAt;
    }

    private void assertThatStartRunsTheParkedReplayAndTheSubscriptionGoesLive() {
        live.stopRetrying();

        catchup.start(true);

        assertThatTheSubscriptionGoesLive();
        assertThat(errorsLogged()).as("errors logged").isEmpty();
    }

    private void assertThatTheSubscriptionGoesLive() {
        await().atMost(WAIT).untilAsserted(() -> assertThat(catchup.isRunning(ID)).as("running").isTrue());
        NameDefined afterStart = nameDefined("afterStart");
        write(eventStore, afterStart);
        await().atMost(WAIT).untilAsserted(() -> assertThat(received).as("received").contains(afterStart));
        assertThat(catchup.hasParkedReplay(ID)).as("replay parked").isFalse();
    }

    // The line logged with the failure the retry threw, which is how a test knows the replay thread has handled it
    private void awaitTheLogLineForTheEndedRetry() {
        await().atMost(WAIT).untilAsserted(() -> assertThat(levelOfTheLogLineForTheEndedRetry()).as("a line logged for the failure the retry threw").isNotNull());
    }

    private @Nullable Level levelOfTheLogLineForTheEndedRetry() {
        RuntimeException failure = live.retryEnded.getNow(null);
        return logged.list.stream()
                .filter(event -> event.getThrowableProxy() instanceof ThrowableProxy proxy && proxy.getThrowable() == failure)
                .map(ILoggingEvent::getLevel)
                .findFirst()
                .orElse(null);
    }

    private List<ILoggingEvent> errorsLogged() {
        return logged.list.stream().filter(event -> event.getLevel().isGreaterOrEqual(Level.ERROR)).toList();
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void write(InMemoryEventStore eventStore, DomainEvent event) {
        eventStore.write(event.eventId(), cloudEventConverter.toCloudEvents(List.of(event)));
    }

    /**
     * A wrapped model whose first globalCheckpoint, or its first canResumeFrom, retries a failure for as long as the
     * stillWanted it is given answers true, and then throws the last failure, as the MongoDB models do for
     * canResumeFrom. With givesUpWithoutAskingStillWanted it never reads stillWanted and throws the failure once the test
     * calls stopRetrying, as a finite retry does when it gives up on its last attempt. The one argument forms retry the same way, for as long as the test lets them, since they have no
     * stillWanted to read.
     */
    private static final class RetryingLiveModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        private final boolean retriesGlobalCheckpoint;
        private final boolean givesUpWithoutAskingStillWanted;
        private final AtomicBoolean retriesOnce = new AtomicBoolean(true);
        private final AtomicBoolean answersFalseOnce = new AtomicBoolean(false);
        private final AtomicBoolean failsOnce = new AtomicBoolean(false);
        private volatile @Nullable CompletableFuture<Void> enteredTheStartThatFails;
        private volatile @Nullable CompletableFuture<Void> mayFailTheStart;
        private volatile boolean stopRetrying;
        private volatile @Nullable Consumer<BooleanSupplier> readsStillWantedInstead;
        final CompletableFuture<Void> retrying = new CompletableFuture<>();
        final CompletableFuture<RuntimeException> retryEnded = new CompletableFuture<>();

        private RetryingLiveModel(InMemorySubscriptionModel delegate, boolean retriesGlobalCheckpoint, boolean givesUpWithoutAskingStillWanted) {
            this.delegate = delegate;
            this.retriesGlobalCheckpoint = retriesGlobalCheckpoint;
            this.givesUpWithoutAskingStillWanted = givesUpWithoutAskingStillWanted;
        }

        // The test then wants the first call to the wrapped model to answer at once, instead of retrying
        void doesNotRetry() {
            retriesOnce.set(false);
        }

        // The test then wants canResumeFrom to hand its stillWanted to the reader and answer true, instead of retrying
        void readsStillWantedInsteadOfRetrying(Consumer<BooleanSupplier> reader) {
            readsStillWantedInstead = reader;
        }

        // The test then wants the first canResumeFrom to read stillWanted through the reader, answer false, and later ones true
        void answersFalseOnceAfterReadingStillWanted(Consumer<BooleanSupplier> reader) {
            readsStillWantedInstead = reader;
            answersFalseOnce.set(true);
        }

        // The test then wants the first canResumeFrom to read stillWanted through the reader and throw, and later ones to answer true
        void failsOnceAfterReadingStillWanted(Consumer<BooleanSupplier> reader) {
            readsStillWantedInstead = reader;
            failsOnce.set(true);
        }

        // The test then wants the next start to signal that it reached this model, wait for the second future, and throw without starting the delegate
        void failsTheNextStartOnceAllowed(CompletableFuture<Void> entered, CompletableFuture<Void> mayFail) {
            mayFailTheStart = mayFail;
            enteredTheStartThatFails = entered;
        }

        void stopRetrying() {
            stopRetrying = true;
        }

        private void retryWhile(BooleanSupplier stillWanted) {
            if (!retriesOnce.getAndSet(false)) {
                return;
            }
            retrying.complete(null);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!stopRetrying && (givesUpWithoutAskingStillWanted || stillWanted.getAsBoolean())) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the retry was never ended");
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            }
            RuntimeException failure = new IllegalStateException("connection lost");
            retryEnded.complete(failure);
            throw failure;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint(() -> true);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint(BooleanSupplier stillWanted) {
            if (retriesGlobalCheckpoint) {
                retryWhile(stillWanted);
            }
            return LIVE_START;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return canResumeFrom(checkpoint, () -> true);
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
            Consumer<BooleanSupplier> reader = readsStillWantedInstead;
            if (reader != null) {
                reader.accept(stillWanted);
                if (Thread.currentThread().isVirtual() && failsOnce.getAndSet(false)) {
                    throw new IllegalStateException("connection lost");
                }
                return !(Thread.currentThread().isVirtual() && answersFalseOnce.getAndSet(false));
            }
            if (!retriesGlobalCheckpoint) {
                retryWhile(stillWanted);
            }
            return true;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            StartAt startAtToUse = resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
            return delegate.subscribe(subscriptionId, filter, startAtToUse, action);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            CompletableFuture<Void> entered = enteredTheStartThatFails;
            CompletableFuture<Void> mayFail = mayFailTheStart;
            if (entered != null && mayFail != null) {
                enteredTheStartThatFails = null;
                entered.complete(null);
                mayFail.join();
                throw new IllegalStateException("the live delegate failed to start");
            }
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
    }
}
