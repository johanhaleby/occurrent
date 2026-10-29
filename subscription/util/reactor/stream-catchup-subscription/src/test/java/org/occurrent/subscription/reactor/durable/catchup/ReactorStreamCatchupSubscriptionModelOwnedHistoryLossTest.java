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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModelHistoryLossTest.LosingLiveStartModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModelHistoryLossTest.ScriptedReader;
import org.slf4j.helpers.MessageFormatter;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A named catch-up owns every subscription it makes, from its subscribe until its cancel, whether it replayed or went
 * straight to live. The wrapped model restarts a lost subscription from the present unless the catch-up answers for
 * it, so each case here asserts that nothing is restarted from the present. The wrapped model is a double that
 * restarts from the present when nobody takes the loss over, as the Mongo model does with its default configuration.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class ReactorStreamCatchupSubscriptionModelOwnedHistoryLossTest {

    private static final String ID = "sub";
    private static final SubscriptionFilter FILTER = StreamSubscriptionFilter.filter(Filter.all());
    private static final Checkpoint LIVE_TOKEN = new StringBasedCheckpoint("a live token");
    private static final Checkpoint OTHER_LIVE_TOKEN = new StringBasedCheckpoint("another live token");
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("a token after reading on");

    private final ScriptedReader reader = new ScriptedReader();
    private final LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger catchupsStarted = new AtomicInteger();
    private final ErrorsLoggedByTheCatchup logged = new ErrorsLoggedByTheCatchup();
    private ReactorStreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void createCatchup() {
        catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                catchupsStarted.incrementAndGet();
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
    }

    @AfterEach
    void shutdown() {
        catchup.shutdown();
        logged.close();
    }

    @Test
    void a_named_live_start_from_a_lost_token_delivers_no_event_before_it_is_paused() {
        // The store holds e1 to e3, written after the lost token, so a replay or a read from the store would deliver them
        assertThat(reader.head).as("events in the store after the lost token").isEqualTo(3);
        // The wrapped model opens asynchronously, so the loss comes after subscribe has returned
        catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());

        wrapped.opensAndLosesTheLiveStart();
        List<String> deliveredWhenPaused = catchup.isPaused(ID) ? List.copyOf(received) : null;

        assertThat(deliveredWhenPaused).as("delivered when the subscription was seen paused").isNotNull().isEmpty();
        assertPausedForGoodFrom(LIVE_TOKEN);
        assertThat(received).as("delivered").isEmpty();
        assertThat(wrapped.resumedAt).as("resumed at a position by a takeover replay").isEmpty();
        assertThat(catchupsStarted).as("catch-ups started").hasValue(0);
    }

    @Test
    void a_live_started_subscription_whose_token_leaves_the_oplog_while_it_is_paused_is_paused_for_good_when_resumed() {
        catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());
        catchup.pauseSubscription(ID);
        wrapped.losesTheNextOpen();

        catchup.resumeSubscription(ID);
        wrapped.opensAndLosesTheLiveStart();

        assertPausedForGoodFrom(LIVE_TOKEN);
        assertThat(received).as("delivered").isEmpty();
    }

    @Test
    void a_subscription_that_replayed_handed_over_and_read_on_is_paused_for_good_by_a_loss_from_a_later_token() {
        replayAndHandOver();

        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);

        assertPausedForGoodFrom(LATER_TOKEN);
        assertThat(received).as("delivered").containsExactly("e1", "e2", "e3");
        assertThat(catchupsStarted).as("catch-ups started").hasValue(1);
        assertThat(wrapped.resumedAt).as("resumed at a position by a takeover replay").isEmpty();
    }

    @Test
    void a_loss_the_wrapped_model_reports_on_the_thread_of_the_subscribe_before_it_returns_pauses_the_subscription_for_good() {
        wrapped.loses = LosingLiveStartModel.Loses.ON_THE_THREAD_OF_SUBSCRIBE;

        catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());

        assertPausedForGoodFrom(LIVE_TOKEN);
    }

    @Test
    void a_loss_the_wrapped_model_reports_on_another_thread_while_the_subscribe_waits_pauses_the_subscription_for_good() {
        wrapped.loses = LosingLiveStartModel.Loses.ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS;

        catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());

        assertPausedForGoodFrom(LIVE_TOKEN);
    }

    @Test
    void a_subscribe_the_wrapped_model_refuses_leaves_no_record_of_the_subscription_so_the_id_can_be_subscribed_again() {
        wrapped.refusesTheSubscribeAt(LIVE_TOKEN, new IllegalStateException("refused by the wrapped model"));

        assertThatThrownBy(() -> catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action()))
                .isInstanceOf(IllegalStateException.class).hasMessage("refused by the wrapped model");
        HistoryLossHandling answeredForTheRefusedSubscribe = wrapped.asksTheListeners(ID, LIVE_TOKEN);
        Subscription subscribedAgain = catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());

        assertThat(answeredForTheRefusedSubscribe).as("the answer for a loss of the refused subscribe").isInstanceOf(HistoryLossHandling.LeaveToSubscriptionModel.class);
        assertThat(subscribedAgain.id()).isEqualTo(ID);
        assertThat(catchup.isRunning(ID)).isTrue();
    }

    @Test
    void a_second_straight_to_live_subscribe_of_a_running_id_is_refused_before_the_wrapped_model_is_asked_and_the_first_stays_owned() {
        catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());
        int delegated = wrapped.subscribeCalls.get();

        Throwable refusal = catchThrowable(() -> catchup.subscribe(ID, FILTER, StartAt.checkpoint(OTHER_LIVE_TOKEN), action()));
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);

        assertAll(
                () -> assertPausedForGoodFrom(LATER_TOKEN),
                () -> assertThat(refusal).as("what the second subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(wrapped.subscribeCalls).as("subscribes delegated").hasValue(delegated)
        );
    }

    @Test
    void a_second_subscribe_through_a_replay_of_a_running_id_is_refused_before_anything_is_replayed_or_delegated_and_the_first_stays_owned() {
        catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action());
        int delegated = wrapped.subscribeCalls.get();

        Throwable refusal = catchThrowable(() -> catchup.subscribe(ID, FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), action()));
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);

        assertAll(
                () -> assertPausedForGoodFrom(LATER_TOKEN),
                () -> assertThat(refusal).as("what the second subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(wrapped.subscribeCalls).as("subscribes delegated").hasValue(delegated),
                () -> assertThat(catchupsStarted).as("catch-ups started").hasValue(0),
                () -> assertThat(received).as("replayed").isEmpty()
        );
    }

    @Test
    void a_straight_to_live_subscribe_of_an_id_whose_replay_is_in_flight_is_refused_before_the_wrapped_model_is_asked_and_the_replay_stays_owned() {
        var replayBlocked = reader.blockReads();
        Subscription replaying = catchup.subscribe(ID, FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        // A wrapped model that would refuse the second subscribe, to see that a refusal cannot cost the first its record
        wrapped.refusesTheSubscribeAt(OTHER_LIVE_TOKEN, new IllegalStateException("refused by the wrapped model"));

        Throwable refusal = catchThrowable(() -> catchup.subscribe(ID, FILTER, StartAt.checkpoint(OTHER_LIVE_TOKEN), action()));
        replayBlocked.tryEmitEmpty();
        replaying.waitUntilStarted().block(Duration.ofSeconds(5));
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);

        assertAll(
                () -> assertPausedForGoodFrom(LATER_TOKEN),
                () -> assertThat(refusal).as("what the second subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class),
                () -> assertThat(wrapped.subscribeCalls).as("subscribes delegated, the handover of the replay only").hasValue(1)
        );
    }

    @Test
    void a_cancel_that_races_a_straight_to_live_subscribe_of_the_same_id_waits_for_it_and_leaves_no_running_subscription() throws Exception {
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        AtomicBoolean blockedOnce = new AtomicBoolean();
        wrapped.beforeSubscribeRegisters = () -> {
            if (blockedOnce.compareAndSet(false, true)) {
                insideSubscribe.countDown();
                await(releaseSubscribe);
            }
        };
        Thread subscriber = new Thread(() -> catchup.subscribe(ID, FILTER, StartAt.checkpoint(LIVE_TOKEN), action()), "subscriber");
        Thread canceller = new Thread(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(10)), "canceller");
        subscriber.start();
        assertThat(insideSubscribe.await(5, TimeUnit.SECONDS)).as("the subscribe reached the wrapped model").isTrue();
        canceller.start();
        // Parked on the lock of the id, or finished when nothing makes it wait
        waitUntilNotRunnable(canceller);

        releaseSubscribe.countDown();
        subscriber.join(5_000);
        canceller.join(5_000);

        assertAll(
                () -> assertThat(subscriber.isAlive()).as("the subscribe is still running").isFalse(),
                () -> assertThat(canceller.isAlive()).as("the cancel is still running").isFalse(),
                () -> assertThat(wrapped.isRunning(ID)).as("the wrapped model runs the subscription").isFalse(),
                () -> assertThat(wrapped.isPaused(ID)).as("the wrapped model holds the subscription paused").isFalse(),
                () -> assertThat(wrapped.cancelled).as("cancels that reached the wrapped model").containsExactly(ID),
                () -> assertThat(catchup.isRunning(ID)).isFalse()
        );
    }

    @Test
    void a_subscribe_or_cancel_from_inside_a_replay_that_runs_on_the_thread_that_launched_it_is_refused_and_leaves_the_replay_alone() {
        AtomicReference<Throwable> straightToLive = new AtomicReference<>();
        AtomicReference<Throwable> throughAReplay = new AtomicReference<>();
        AtomicReference<Throwable> cancel = new AtomicReference<>();
        AtomicBoolean triedOnce = new AtomicBoolean();

        Subscription subscription = catchup.subscribe(ID, FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> Mono.fromRunnable(() -> {
            received.add(event.getId());
            if (triedOnce.compareAndSet(false, true)) {
                straightToLive.set(catchThrowable(() -> catchup.subscribe("other", FILTER, StartAt.checkpoint(LIVE_TOKEN), action())));
                throughAReplay.set(catchThrowable(() -> catchup.subscribe("other", FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), action())));
                cancel.set(catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5))));
            }
        }));
        Throwable waitForTheStart = catchThrowable(() -> subscription.waitUntilStarted().block(Duration.ofSeconds(5)));

        assertAll(
                () -> assertThat(waitForTheStart).as("what waiting for the start threw").isNull(),
                () -> assertThat(straightToLive.get()).as("a straight to live subscribe").isInstanceOf(IllegalStateException.class).hasMessageContaining("Subscribe or cancel from another thread"),
                () -> assertThat(throughAReplay.get()).as("a subscribe through a replay").isInstanceOf(IllegalStateException.class).hasMessageContaining("Subscribe or cancel from another thread"),
                () -> assertThat(cancel.get()).as("a cancel").isInstanceOf(IllegalStateException.class).hasMessageContaining("Subscribe or cancel from another thread"),
                () -> assertThat(received).as("replayed").containsExactly("e1", "e2", "e3"),
                () -> assertThat(wrapped.subscribeCalls).as("subscribes delegated, the handover of the replay only").hasValue(1),
                () -> assertThat(wrapped.cancelled).as("cancels that reached the wrapped model").isEmpty(),
                () -> assertThat(catchup.isRunning(ID)).as("the replayed subscription runs").isTrue()
        );
        // The mark is gone once the replay has launched
        assertThat(catchup.subscribe("other", FILTER, StartAt.checkpoint(LIVE_TOKEN), action()).id()).isEqualTo("other");
    }

    @Test
    void a_replay_that_fails_before_its_handover_leaves_no_record_of_the_subscription_so_a_subscribe_of_the_id_replays_again() {
        reader.failReads(new IllegalStateException("the event store is down"));
        Subscription failed = catchup.subscribe(ID, FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        Throwable failure = catchThrowable(() -> failed.waitUntilStarted().block(Duration.ofSeconds(5)));
        HistoryLossHandling answeredForTheFailedReplay = wrapped.asksTheListeners(ID, LIVE_TOKEN);
        reader.readsAgain();

        replayAndHandOver();

        assertAll(
                () -> assertThat(failure).as("what waiting for the start threw").isInstanceOf(IllegalStateException.class).hasMessage("the event store is down"),
                () -> assertThat(answeredForTheFailedReplay).as("the answer for a loss of the failed replay").isInstanceOf(HistoryLossHandling.LeaveToSubscriptionModel.class),
                () -> assertThat(catchupsStarted).as("catch-ups started, one for each replay").hasValue(2),
                () -> assertThat(catchup.isRunning(ID)).isTrue()
        );
    }

    @Test
    void a_refused_resume_of_a_subscription_paused_for_good_runs_no_takeover_replay_and_does_not_start_a_stopped_model() {
        replayAndHandOver();
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);
        catchup.stop();
        int startsBefore = wrapped.starts.get();

        Throwable refusal = catchThrowable(() -> catchup.resumeSubscription(ID));

        assertAll(
                () -> assertThat(refusal).as("what the resume threw").isInstanceOf(CatchupCannotResumeException.class),
                () -> assertThat(wrapped.starts).as("starts of the wrapped model").hasValue(startsBefore),
                () -> assertThat(catchup.isRunning()).as("the model runs").isFalse(),
                () -> assertThat(catchupsStarted).as("catch-ups started").hasValue(1),
                () -> assertThat(wrapped.resumedAt).as("resumed at a position by a takeover replay").isEmpty(),
                () -> assertThat(received).as("delivered").containsExactly("e1", "e2", "e3")
        );
    }

    @Test
    void a_refused_resume_after_a_failed_takeover_replay_and_a_loss_from_a_later_token_runs_no_takeover_replay() {
        replayAndHandOver();
        reader.head = 4;
        reader.failReads(new IllegalStateException("the event store is down"));
        wrapped.opensAndLosesTheLiveStart();
        assertThat(catchupsStarted).as("catch-ups started, the replay and the takeover replay that failed").hasValue(2);
        reader.readsAgain();
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);

        Throwable refusal = catchThrowable(() -> catchup.resumeSubscription(ID));

        assertAll(
                () -> assertThat(refusal).as("what the resume threw").isInstanceOf(CatchupCannotResumeException.class),
                () -> assertThat(catchupsStarted).as("catch-ups started").hasValue(2),
                () -> assertThat(wrapped.resumedAt).as("resumed at a position by a takeover replay").isEmpty(),
                () -> assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty(),
                () -> assertThat(received).as("delivered").containsExactly("e1", "e2", "e3")
        );
    }

    @Test
    void a_cancel_clears_a_subscription_paused_for_good_so_the_id_can_be_subscribed_and_resumed_again() {
        replayAndHandOver();
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);
        assertThatThrownBy(() -> catchup.resumeSubscription(ID)).isInstanceOf(CatchupCannotResumeException.class);

        catchup.cancelSubscription(ID).block(Duration.ofSeconds(5));
        received.clear();
        replayAndHandOver();
        catchup.pauseSubscription(ID);
        catchup.resumeSubscription(ID);

        assertThat(catchup.isRunning(ID)).isTrue();
        assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty();
    }

    @Test
    void a_start_that_resumes_a_subscription_paused_for_good_pauses_it_again_with_one_more_error_and_restarts_nothing_from_the_present() {
        wrapped.startResumesPausedSubscriptions = true;
        replayAndHandOver();
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);
        catchup.stop();

        catchup.start(true);
        assertThat(wrapped.isRunning(ID)).as("the wrapped model resumed the subscription where it was held").isTrue();
        wrapped.losesTheHistoryFrom(ID, LATER_TOKEN);

        assertThat(logged.errors()).as("errors logged, one for each loss").hasSize(2).containsOnly(terminalFailureLog(LATER_TOKEN));
        assertAll(
                () -> assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty(),
                () -> assertThat(wrapped.ended).as("subscriptions the wrapped model ended").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).isTrue(),
                () -> assertThat(received).as("delivered").containsExactly("e1", "e2", "e3"),
                () -> assertThat(catchupsStarted).as("catch-ups started").hasValue(1),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription(ID))).as("what a resume throws").isInstanceOf(CatchupCannotResumeException.class)
        );
    }

    // Paused with the one error the catch-up logs, nothing restarted from the present, and every resume refused
    private void assertPausedForGoodFrom(Checkpoint lostFrom) {
        assertAll(
                () -> assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty(),
                () -> assertThat(wrapped.ended).as("subscriptions the wrapped model ended").isEmpty(),
                () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                () -> assertThat(catchup.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(logged.errors()).as("errors logged").containsExactly(terminalFailureLog(lostFrom)),
                () -> assertThat(catchThrowable(() -> catchup.resumeSubscription(ID))).as("what a resume throws")
                        .isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("\"" + lostFrom.asString() + "\"")
        );
    }

    private static String terminalFailureLog(Checkpoint lostFrom) {
        return MessageFormatter.arrayFormat(HistoryLossMessages.TERMINAL_FAILURE_LOG, new Object[]{ID, lostFrom.asString(), lostFrom.asString()}).getMessage();
    }

    private void replayAndHandOver() {
        Subscription subscription = catchup.subscribe(ID, FILTER, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        subscription.waitUntilStarted().block(Duration.ofSeconds(5));
        assertThat(received).containsExactly("e1", "e2", "e3");
    }

    private Function<CloudEvent, Mono<Void>> action() {
        return event -> Mono.fromRunnable(() -> received.add(event.getId()));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Not released within 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting", e);
        }
    }

    private static void waitUntilNotRunnable(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() == Thread.State.NEW || thread.getState() == Thread.State.RUNNABLE) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Thread " + thread.getName() + " neither waited nor finished within 5 seconds");
            }
            Thread.onSpinWait();
        }
    }
}
