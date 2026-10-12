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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * What a catch-up reports and does for a subscription while it takes the subscription over after the live model lost
 * the history from its live start.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupTakeoverLifecycleTest {

    private static final String ID = "subscription";

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;
    private InMemoryEventStore eventStore;
    private List<DomainEvent> history;
    private NameDefined writtenInTheGap;
    private InMemoryCheckpointStorage storage;
    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> signals = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<Throwable> failedOnTheirOwnThreads = new ConcurrentLinkedQueue<>();
    private final AtomicInteger deliveries = new AtomicInteger();
    private final CountDownLatch inTheTakeoverReplay = new CountDownLatch(1);
    private final CountDownLatch letTheTakeoverReplayGo = new CountDownLatch(1);
    private volatile boolean blocksInTheTakeoverReplay;
    private final Logger catchupLogger = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
    private final LoggedEvents logged = new LoggedEvents();
    private LiveStartLosingSubscriptionModel liveModel;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(event));
        writtenInTheGap = nameDefined("writtenInTheGap");
        storage = new InMemoryCheckpointStorage();
        logged.start();
        catchupLogger.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        catchupLogger.detachAppender(logged);
        letTheTakeoverReplayGo.countDown();
        if (catchup != null) {
            catchup.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void a_takeover_counts_towards_the_replays_a_catch_up_makes_before_it_gives_up(boolean storing) {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> {
        }).losingEveryOpen();
        create(storing);
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());

        boolean gaveUp = waitUpTo(Duration.ofSeconds(10), () -> logged.events.stream().anyMatch(StreamCatchupTakeoverLifecycleTest::givesUpAfterFourReplays));

        assertThat(gaveUp).as("the replay gave up because the live start was lost during or right after each of its 4 replays").isTrue();
        assertAll(
                () -> assertThat(liveModel.lostLiveStarts).as("the live starts the live model lost").hasSizeGreaterThanOrEqualTo(4),
                () -> assertThat(received.subList(0, 4 * history.size())).as("the history, once for each of the 4 replays")
                        .containsExactlyElementsOf(Stream.of(1, 2, 3, 4).flatMap(replay -> history.stream()).toList()),
                () -> assertThat(liveModel.openedAt).as("the live starts the live model opened").isEmpty(),
                () -> assertThat(liveModel.restartedFromThePresent).as("restarts from the present").isEmpty()
        );
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void a_subscription_the_live_model_holds_paused_reads_as_running_and_not_paused_while_the_takeover_replays(boolean storing) throws InterruptedException {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> write(writtenInTheGap));
        blocksInTheTakeoverReplay = true;
        create(storing);

        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("the takeover replays").isTrue();

        assertAll(
                () -> assertThat(liveModel.isPaused(ID)).as("held paused by the live model").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").hasValue(0)
        );
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void the_handle_of_a_subscription_stays_pending_while_the_takeover_replays_and_starts_once_it_has_handed_over(boolean storing) throws InterruptedException {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> write(writtenInTheGap));
        blocksInTheTakeoverReplay = true;
        create(storing);

        Subscription started = catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("the takeover replays").isTrue();

        assertThat(started.waitUntilStarted(Duration.ofMillis(300))).as("started while the takeover replays").isFalse();
        letTheTakeoverReplayGo.countDown();
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).as("started once the takeover has handed over").isTrue();
        assertThat(liveModel.openedAt).as("the live starts the live model opened").hasSize(1);
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void a_pause_while_the_takeover_replays_leaves_the_subscription_paused_until_it_is_resumed(boolean storing) throws InterruptedException {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> write(writtenInTheGap));
        blocksInTheTakeoverReplay = true;
        create(storing);
        Subscription started = catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("the takeover replays").isTrue();

        catchup.pauseSubscription(ID);
        letTheTakeoverReplayGo.countDown();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(catchup.isCatchingUp(ID)).as("catching up").isFalse());
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").hasValue(0),
                () -> assertThat(liveModel.openedAt).as("the live starts the live model opened").isEmpty()
        );
        NameDefined writtenWhilePaused = nameDefined("writtenWhilePaused");
        write(writtenWhilePaused);
        assertThat(received).as("received while paused").doesNotContain(writtenWhilePaused);

        Subscription resumed = catchup.resumeSubscription(ID);

        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(10))).as("started once resumed").isTrue();
        assertAll(
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(liveModel.openedAt).as("the live starts the live model opened").hasSize(1),
                () -> assertThat(received).as("the event written in between").contains(writtenInTheGap)
        );
        if (!storing) {
            assertThat(received).as("the event written while paused, which the replay that ran again delivers").filteredOn(writtenWhilePaused::equals).hasSize(1);
        }
        assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).as("the first handle started").isTrue();
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void a_cancel_while_the_takeover_replays_ends_it_and_the_live_model_is_never_resumed(boolean storing) throws InterruptedException {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> write(writtenInTheGap));
        blocksInTheTakeoverReplay = true;
        create(storing);
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("the takeover replays").isTrue();

        Thread canceller = onItsOwnThread(() -> catchup.cancelSubscription(ID));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(catchup.isCatchingUp(ID)).as("catching up once the cancel has ended the replay").isFalse());
        letTheTakeoverReplayGo.countDown();
        canceller.join(Duration.ofSeconds(10));

        assertThat(canceller.isAlive()).as("the cancel is still waiting").isFalse();
        NameDefined afterCancel = nameDefined("afterCancel");
        write(afterCancel);
        assertAll(
                () -> assertThat(failedOnTheirOwnThreads).as("failures").isEmpty(),
                () -> assertThat(received).as("received after the cancel").doesNotContain(writtenInTheGap, afterCancel),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").hasValue(0),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse()
        );
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void the_handle_of_a_subscription_cancelled_while_the_takeover_replays_stops_waiting_for_it_to_start(boolean storing) throws InterruptedException {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> write(writtenInTheGap));
        blocksInTheTakeoverReplay = true;
        create(storing);
        Subscription handle = catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        assertThat(inTheTakeoverReplay.await(10, TimeUnit.SECONDS)).as("the takeover replays").isTrue();

        Thread canceller = onItsOwnThread(() -> catchup.cancelSubscription(ID));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(catchup.isCatchingUp(ID)).as("catching up once the cancel has ended the replay").isFalse());
        letTheTakeoverReplayGo.countDown();
        canceller.join(Duration.ofSeconds(10));

        long before = System.nanoTime();
        boolean started = handle.waitUntilStarted(Duration.ofSeconds(6));
        Duration waited = Duration.ofNanos(System.nanoTime() - before);
        assertAll(
                () -> assertThat(failedOnTheirOwnThreads).as("failures").isEmpty(),
                () -> assertThat(started).as("started").isFalse(),
                () -> assertThat(waited).as("how long the handle waited for a subscription that was cancelled").isLessThan(Duration.ofSeconds(2))
        );
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void a_pause_that_gets_the_subscription_before_the_takeover_does_leaves_it_paused_until_it_is_resumed(boolean storing) throws InterruptedException {
        TheTakeoverIsAskedForWhileAnotherCallHoldsTheSubscription theTakeover = new TheTakeoverIsAskedForWhileAnotherCallHoldsTheSubscription(storing, () -> catchup.pauseSubscription(ID));

        theTakeover.isAskedForWhileACallIsQueued();

        assertAll(
                () -> assertThat(catchup.isRunning(ID)).as("running while the takeover starts").isTrue(),
                () -> assertThat(catchup.isPaused(ID)).as("paused while the takeover starts").isFalse()
        );
        theTakeover.letTheCallsGo();
        boolean decided = waitUpTo(Duration.ofSeconds(10), () -> logged.events.stream().anyMatch(StreamCatchupTakeoverLifecycleTest::decidesTheTakeover));
        assertThat(decided).as("the takeover found the pause or started its replay").isTrue();
        assertAll(
                () -> assertThat(failedOnTheirOwnThreads).as("failures").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(signals).as("the catch-up starts and has read its history once, since no replay runs for a paused subscription").containsExactly("catchupStarted", "historyRead"),
                () -> assertThat(liveModel.resumeCalls).as("resumes of the live model").hasValue(0),
                () -> assertThat(liveModel.openedAt).as("the live starts the live model opened").isEmpty()
        );
        NameDefined writtenWhilePaused = nameDefined("writtenWhilePaused");
        write(writtenWhilePaused);
        assertThat(received).as("received while paused").doesNotContain(writtenWhilePaused);

        Subscription resumed = catchup.resumeSubscription(ID);

        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(10))).as("started once resumed").isTrue();
        assertAll(
                () -> assertThat(received).as("the event written in between").contains(writtenInTheGap),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isTrue(),
                () -> assertThat(liveModel.openedAt).as("the live starts the live model opened").hasSize(1)
        );
        if (!storing) {
            assertThat(received).as("the event written while paused, which the replay that ran again delivers").filteredOn(writtenWhilePaused::equals).hasSize(1);
        }
    }

    @ParameterizedTest(name = "storing={0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void a_cancel_that_gets_the_subscription_before_the_takeover_does_drops_the_takeover(boolean storing) throws InterruptedException {
        TheTakeoverIsAskedForWhileAnotherCallHoldsTheSubscription theTakeover = new TheTakeoverIsAskedForWhileAnotherCallHoldsTheSubscription(storing, () -> catchup.cancelSubscription(ID));

        theTakeover.isAskedForWhileACallIsQueued();
        theTakeover.letTheCallsGo();

        boolean takenOverAnyway = waitUpTo(Duration.ofMillis(1500), () -> signals.size() > 2 || liveModel.resumeCalls.get() > 0);
        assertThat(takenOverAnyway).as("a replay ran or the live model was resumed for a cancelled subscription").isFalse();
        assertAll(
                () -> assertThat(failedOnTheirOwnThreads).as("failures").isEmpty(),
                () -> assertThat(signals).as("the catch-up starts and has read its history once, since no replay runs for a cancelled subscription").containsExactly("catchupStarted", "historyRead"),
                () -> assertThat(received).as("received").doesNotContain(writtenInTheGap).hasSize(history.size()),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isFalse()
        );
    }

    /**
     * Gets the live model to lose the live start while a pause that holds the subscription for as long as the test
     * wants is under way, and another call for the subscription waits for it. The takeover is asked for in between,
     * and waits for the subscription behind the call that waits.
     */
    private final class TheTakeoverIsAskedForWhileAnotherCallHoldsTheSubscription {
        private final CountDownLatch openGate = new CountDownLatch(1);
        private final CountDownLatch holding = new CountDownLatch(1);
        private final CountDownLatch holdGate = new CountDownLatch(1);
        private final Runnable queuedCall;
        private Thread holder;
        private Thread queued;

        private TheTakeoverIsAskedForWhileAnotherCallHoldsTheSubscription(boolean storing, Runnable queuedCall) {
            this.queuedCall = queuedCall;
            liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> write(writtenInTheGap))
                    .beforeTheFirstOpen(() -> awaitUnchecked(openGate))
                    .beforeTheFirstPause(() -> {
                        holding.countDown();
                        awaitUnchecked(holdGate);
                    });
            create(storing);
        }

        void isAskedForWhileACallIsQueued() throws InterruptedException {
            catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(signals).as("signals").contains("historyRead");
                assertThat(catchup.isCatchingUp(ID)).as("catching up").isFalse();
            });
            holder = onItsOwnThread(() -> catchup.pauseSubscription(ID));
            assertThat(holding.await(10, TimeUnit.SECONDS)).as("a pause holds the subscription").isTrue();
            queued = onItsOwnThread(queuedCall);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(queued.getState()).as("the queued call").isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING));
            openGate.countDown();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(liveModel.takeOversAsked).as("takeovers asked for").hasValue(1));
        }

        void letTheCallsGo() throws InterruptedException {
            holdGate.countDown();
            holder.join(Duration.ofSeconds(10));
            queued.join(Duration.ofSeconds(10));
            assertThat(holder.isAlive() || queued.isAlive()).as("a call is still waiting").isFalse();
        }
    }

    private void create(boolean storing) {
        CatchupSubscriptionModelConfig config = storing
                ? new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1))
                : new CatchupSubscriptionModelConfig(100);
        catchup = new StreamCatchupSubscriptionModel(liveModel, eventStore, config);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("catchupStarted");
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
            }
        });
    }

    private Consumer<CloudEvent> action() {
        return cloudEvent -> {
            received.add(cloudEventConverter.toDomainEvent(cloudEvent));
            if (blocksInTheTakeoverReplay && deliveries.incrementAndGet() == history.size() + 1) {
                inTheTakeoverReplay.countDown();
                awaitUnchecked(letTheTakeoverReplayGo);
            }
        };
    }

    private Thread onItsOwnThread(Runnable call) {
        Thread thread = new Thread(() -> {
            try {
                call.run();
            } catch (Throwable e) {
                failedOnTheirOwnThreads.add(e);
            }
        });
        thread.start();
        return thread;
    }

    private static final class LoggedEvents extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
    }

    private static boolean decidesTheTakeover(ILoggingEvent logged) {
        return logged.getFormattedMessage().contains("lost the history from the live start that catch-up subscription");
    }

    private static boolean givesUpAfterFourReplays(ILoggingEvent logged) {
        IThrowableProxy failure = logged.getThrowableProxy();
        return failure != null && failure.getMessage() != null
                && failure.getMessage().contains("lost the live start during or right after each of its 4 replays");
    }

    private static boolean waitUpTo(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }
}
