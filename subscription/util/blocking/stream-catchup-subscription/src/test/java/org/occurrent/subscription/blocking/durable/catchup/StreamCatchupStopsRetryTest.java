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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
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
        assertThat(subscription.waitUntilStarted(WAIT)).as("the subscription started").isFalse();
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
        assertThat(subscription.waitUntilStarted(WAIT)).as("the subscription started").isFalse();
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
        assertThat(errorsLogged()).as("errors logged").singleElement().satisfies(event -> assertThat(event.getFormattedMessage()).contains("it runs again once the subscription is resumed or start(true) is called on this model"));
        live.stopRetrying();
        catchup.start(true);
        assertThatTheSubscriptionGoesLive();
    }

    private Subscription subscribeWhileTheWrappedModelRetries(Site site) {
        return subscribeWhileTheWrappedModelRetries(site, false);
    }

    private Subscription subscribeWhileTheWrappedModelRetries(Site site, boolean givesUpWithoutAskingStillWanted) {
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
        Subscription subscription = catchup.subscribe(ID, startAt, toDomainEvents(received));
        assertThat(live.retrying).as("the wrapped model is retrying").succeedsWithin(WAIT);
        return subscription;
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
        private volatile boolean stopRetrying;
        final CompletableFuture<Void> retrying = new CompletableFuture<>();
        final CompletableFuture<RuntimeException> retryEnded = new CompletableFuture<>();

        private RetryingLiveModel(InMemorySubscriptionModel delegate, boolean retriesGlobalCheckpoint, boolean givesUpWithoutAskingStillWanted) {
            this.delegate = delegate;
            this.retriesGlobalCheckpoint = retriesGlobalCheckpoint;
            this.givesUpWithoutAskingStillWanted = givesUpWithoutAskingStillWanted;
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
