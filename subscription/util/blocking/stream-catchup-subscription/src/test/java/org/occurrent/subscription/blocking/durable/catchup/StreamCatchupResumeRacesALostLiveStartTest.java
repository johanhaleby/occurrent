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
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A resume that comes while a replay is held in its history read, after the live model lost the live start that replay
 * goes live from, and so gets the handle of that replay rather than a replay of its own.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeRacesALostLiveStartTest {

    private static final String ID = "subscription";
    private static final Duration WAIT = Duration.ofSeconds(10);
    // Longer than WAIT, so a resume that waits for the held replay runs out of WAIT before the hold ends
    private static final Duration HOLD = WAIT.multipliedBy(3);
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning, so the loss of LIVE_START is recorded
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final CountDownLatch replayHeldInTheHistoryRead = new CountDownLatch(1);
    private final CountDownLatch releaseTheReplay = new CountDownLatch(1);
    private final AtomicBoolean heldOnce = new AtomicBoolean();
    private final AtomicBoolean replayLeftTheHold = new AtomicBoolean();
    private final AtomicInteger replaysStarted = new AtomicInteger();
    private final HeldPausedReports reports = new HeldPausedReports();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel liveModel;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        List.of(nameDefined(time, "event1"), nameDefined(time, "event2"), nameDefined(time, "event3")).forEach(this::write);
        storage = new InMemoryCheckpointStorage();
        liveModel = new HistoryLosingLiveModel();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                replaysStarted.incrementAndGet();
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        catchup.addStartRefusalListener(reports);
    }

    @AfterEach
    void shutdown() {
        releaseTheReplay.countDown();
        catchup.shutdown();
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void fails_the_handle_of_a_resume_that_came_while_the_replay_of_an_earlier_resume_read_the_history_and_holds_the_subscription_paused_for_the_lost_live_start() {
        // Given
        catchup.subscribe(ID, StartAt.checkpoint(LIVE_START), actionThatLosesTheLiveStartAndIsHeld());
        catchup.pauseSubscription(ID);
        storage.save(ID, STORED);
        Subscription firstResume = catchup.resumeSubscription(ID);
        assertThat(reached(replayHeldInTheHistoryRead)).as("the replay of the first resume reached its history read").isTrue();

        // When
        CompletableFuture<Subscription> secondResume = CompletableFuture.supplyAsync(() -> catchup.resumeSubscription(ID));
        boolean secondResumeReturnedWhileTheReplayWasHeld = returnedWithinTheWait(secondResume);
        boolean replayLeftTheHoldBeforeTheSecondResumeReturned = replayLeftTheHold.get();
        int replaysStartedWhenTheSecondResumeReturned = replaysStarted.get();
        Throwable thrownByTheSecondResume = thrownBy(secondResume);
        Subscription second = returnedBy(secondResume);
        boolean secondEndedWhileTheReplayWasHeld = endedAtOnce(second);
        int heldPausedReportsWhileTheReplayWasHeld = reports.heldPaused.size();
        releaseTheReplay.countDown();
        Throwable thrownWaitingForTheSecond = second == null ? null : catchThrowable(() -> second.waitUntilStarted(WAIT));
        Throwable thrownWaitingForTheFirst = catchThrowable(() -> firstResume.waitUntilStarted(WAIT));
        await().atMost(WAIT).untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").isNotEmpty());

        // Then
        assertAll("the test entered the window, with the second resume returned while the replay of the first was held in its history read",
                () -> assertThat(secondResumeReturnedWhileTheReplayWasHeld).as("window: the second resume returned while the replay was held").isTrue(),
                () -> assertThat(thrownByTheSecondResume).as("window: the second resume threw nothing").isNull(),
                () -> assertThat(replayLeftTheHoldBeforeTheSecondResumeReturned).as("window: the replay was still held when the second resume returned").isFalse(),
                () -> assertThat(replaysStartedWhenTheSecondResumeReturned).as("window: replays started when the second resume returned, so it started none of its own").isEqualTo(1),
                () -> assertThat(second).as("window: the second resume returned the handle of the replay of the first").isEqualTo(firstResume),
                () -> assertThat(secondEndedWhileTheReplayWasHeld).as("window: the handle of the second resume had neither started nor failed while the replay was held").isFalse(),
                () -> assertThat(heldPausedReportsWhileTheReplayWasHeld).as("window: nothing was reported held paused while the replay was held").isZero()
        );
        assertHeldPausedForTheLostLiveStart(thrownWaitingForTheFirst, thrownWaitingForTheSecond);
    }

    @Test
    void fails_the_handle_of_a_resume_that_came_while_the_replay_of_a_subscribe_read_the_history_and_holds_the_subscription_paused_for_the_lost_live_start() {
        // Given
        Subscription subscribed = catchup.subscribe(ID, StartAt.checkpoint(STORED), actionThatLosesTheLiveStartAndIsHeld());
        assertThat(reached(replayHeldInTheHistoryRead)).as("the replay of the subscribe reached its history read").isTrue();

        // When
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> catchup.resumeSubscription(ID));
        boolean resumeReturnedWhileTheReplayWasHeld = returnedWithinTheWait(resume);
        boolean replayLeftTheHoldBeforeTheResumeReturned = replayLeftTheHold.get();
        int replaysStartedWhenTheResumeReturned = replaysStarted.get();
        Throwable thrownByTheResume = thrownBy(resume);
        Subscription resumed = returnedBy(resume);
        boolean resumedEndedWhileTheReplayWasHeld = endedAtOnce(resumed);
        int heldPausedReportsWhileTheReplayWasHeld = reports.heldPaused.size();
        releaseTheReplay.countDown();
        Throwable thrownWaitingForTheResume = resumed == null ? null : catchThrowable(() -> resumed.waitUntilStarted(WAIT));
        Throwable thrownWaitingForTheSubscribe = catchThrowable(() -> subscribed.waitUntilStarted(WAIT));
        await().atMost(WAIT).untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").isNotEmpty());

        // Then
        assertAll("the test entered the window, with the resume returned while the replay of the subscribe was held in its history read",
                () -> assertThat(resumeReturnedWhileTheReplayWasHeld).as("window: the resume returned while the replay was held").isTrue(),
                () -> assertThat(thrownByTheResume).as("window: the resume threw nothing").isNull(),
                () -> assertThat(replayLeftTheHoldBeforeTheResumeReturned).as("window: the replay was still held when the resume returned").isFalse(),
                () -> assertThat(replaysStartedWhenTheResumeReturned).as("window: replays started when the resume returned, so it started none of its own").isEqualTo(1),
                () -> assertThat(resumed).as("window: the resume returned the handle of the replay of the subscribe").isEqualTo(subscribed),
                () -> assertThat(resumedEndedWhileTheReplayWasHeld).as("window: the handle of the resume had neither started nor failed while the replay was held").isFalse(),
                () -> assertThat(heldPausedReportsWhileTheReplayWasHeld).as("window: nothing was reported held paused while the replay was held").isZero()
        );
        assertHeldPausedForTheLostLiveStart(thrownWaitingForTheSubscribe, thrownWaitingForTheResume);
        assertThat(liveModel.subscribeCalls).as("running subscribes on the live model").isEmpty();
    }

    @Test
    void tells_the_next_start_refusal_listener_that_the_subscription_is_held_paused_when_the_one_before_it_throws() throws InterruptedException {
        // Given a first start refusal listener that throws when told a subscription is held paused, and a second one
        catchup.removeStartRefusalListener(reports);
        catchup.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
                throw new IllegalStateException("The listener failed.");
            }
        });
        catchup.addStartRefusalListener(reports);

        // When the catch-up holds the subscription paused for a lost live start
        Subscription subscribed = catchup.subscribe(ID, StartAt.checkpoint(STORED), actionThatLosesTheLiveStartAndIsHeld());
        assertThat(reached(replayHeldInTheHistoryRead)).as("the replay reached its history read").isTrue();
        releaseTheReplay.countDown();
        Throwable thrownWaitingForTheReplay = catchThrowable(() -> subscribed.waitUntilStarted(WAIT));
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (reports.heldPaused.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        // Then the second listener is told, with the refusal the start failed with
        assertThat(thrownWaitingForTheReplay).as("thrown waiting for the replay").isInstanceOf(CatchupCannotResumeException.class);
        assertThat(reports.heldPaused).as("held paused reports").containsExactly(new Report(ID, (CatchupCannotResumeException) thrownWaitingForTheReplay));
    }

    private void assertHeldPausedForTheLostLiveStart(Throwable thrownWaitingForTheReplay, Throwable thrownWaitingForTheResumeThatCameDuringIt) {
        Report report = reports.heldPaused.get(0);
        Throwable thrownByALaterResume = catchThrowable(() -> catchup.resumeSubscription(ID));
        assertAll("waiting for the replay threw " + thrownWaitingForTheReplay + ", waiting for the resume that came during it threw " + thrownWaitingForTheResumeThatCameDuringIt
                  + ", the later resume threw " + thrownByALaterResume + ", reported held paused " + reports.heldPaused,
                () -> assertThat(thrownWaitingForTheResumeThatCameDuringIt).as("thrown waiting for the handle of the resume that came during the replay")
                        .isInstanceOf(CatchupCannotResumeException.class).isSameAs(report.refusal()),
                () -> assertThat(thrownWaitingForTheReplay).as("thrown waiting for the handle that started the replay")
                        .isInstanceOf(CatchupCannotResumeException.class).isSameAs(report.refusal()),
                () -> assertThat(reports.heldPaused).as("held paused reports").containsExactly(new Report(ID, report.refusal())),
                () -> assertThat(catchup.isHeldPausedFor(ID, report.refusal())).as("held paused for the reported refusal").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.hasParkedReplay(ID)).as("replay held").isTrue(),
                () -> assertThat(thrownByALaterResume).as("thrown by a later resume").isInstanceOf(CatchupCannotResumeException.class),
                () -> assertThat(replaysStarted).as("replays started").hasValue(1),
                () -> assertThat(liveModel.isRunning(ID)).as("running on the live model").isFalse()
        );
    }

    // Loses the live start of the replay on the first event and holds the replay there until released
    private Consumer<CloudEvent> actionThatLosesTheLiveStartAndIsHeld() {
        return cloudEvent -> {
            if (heldOnce.compareAndSet(false, true)) {
                liveModel.withoutHistoryBack(LIVE_START);
                replayHeldInTheHistoryRead.countDown();
                try {
                    awaitLatch(releaseTheReplay, HOLD);
                } finally {
                    replayLeftTheHold.set(true);
                }
            }
            received.add(cloudEventConverter.toDomainEvent(cloudEvent));
        };
    }

    private static NameDefined nameDefined(LocalDateTime time, String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }

    private static @Nullable Subscription returnedBy(CompletableFuture<Subscription> resume) {
        return resume.isDone() && !resume.isCompletedExceptionally() ? resume.join() : null;
    }

    private static @Nullable Throwable thrownBy(CompletableFuture<Subscription> resume) {
        return resume.handle((returned, thrown) -> thrown).getNow(null);
    }

    // Whether the handle started or failed without waiting
    private static boolean endedAtOnce(@Nullable Subscription subscription) {
        if (subscription == null) {
            return false;
        }
        try {
            return subscription.waitUntilStarted(Duration.ZERO);
        } catch (RuntimeException e) {
            return true;
        }
    }

    // False when it threw, unlike reached in StreamCatchupResumeRacesTheHandoverTest
    private static boolean returnedWithinTheWait(CompletableFuture<?> future) {
        try {
            future.get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            return false;
        }
    }

    private static boolean reached(CountDownLatch latch) {
        try {
            return latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void awaitLatch(CountDownLatch latch, Duration timeout) {
        try {
            if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Not released within " + timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private record Report(String subscriptionId, CatchupCannotResumeException refusal) {
    }

    // Records each subscription reported held paused. HistoryLosingLiveModel reports no refused start, so nothing calls
    // handleStartRefusal
    private static final class HeldPausedReports implements StartRefusalListener {
        private final List<Report> heldPaused = new CopyOnWriteArrayList<>();

        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
        }

        @Override
        public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            heldPaused.add(new Report(subscriptionId, refusal));
        }
    }
}
