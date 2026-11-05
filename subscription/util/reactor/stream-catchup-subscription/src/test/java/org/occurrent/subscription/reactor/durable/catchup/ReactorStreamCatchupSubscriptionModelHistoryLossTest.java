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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.reactor.RepositionableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A catch-up over a wrapped model that loses the live start as it opens at it.
 * The wrapped model is a double that asks its listeners when the test says so.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class ReactorStreamCatchupSubscriptionModelHistoryLossTest {

    private static final String ID = "sub";

    private final ScriptedReader reader = new ScriptedReader();
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger catchupsStarted = new AtomicInteger();

    @Test
    void a_live_start_lost_as_the_wrapped_model_opens_at_it_is_taken_over_and_the_event_written_in_the_gap_is_delivered_when_the_model_restarts_from_the_present() {
        verifiesTheGapEventIsDeliveredAndTheSubscriptionResumedAtANewLiveStart(new LosingLiveStartModel(true));
    }

    @Test
    void a_live_start_lost_as_the_wrapped_model_opens_at_it_is_taken_over_and_the_event_written_in_the_gap_is_delivered_when_the_model_ends_the_subscription() {
        verifiesTheGapEventIsDeliveredAndTheSubscriptionResumedAtANewLiveStart(new LosingLiveStartModel(false));
    }

    @Test
    void a_live_start_the_wrapped_model_reports_lost_as_another_checkpoint_type_with_the_same_string_form_is_taken_over() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        wrapped.reportsTheLostPositionAs = position -> new TypedPosition(position.asString());
        verifiesTheGapEventIsDeliveredAndTheSubscriptionResumedAtANewLiveStart(wrapped);
    }

    private record TypedPosition(String asString) implements Checkpoint {
    }

    private void verifiesTheGapEventIsDeliveredAndTheSubscriptionResumedAtANewLiveStart(LosingLiveStartModel wrapped) {
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            Checkpoint handedOver = wrapped.issued.get(0);
            assertThat(wrapped.positionOf(ID)).as("handed over at the live start read before the first replay").isEqualTo(handedOver);

            reader.head = 4;
            wrapped.opensAndLosesTheLiveStart();

            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(catchupsStarted).as("told again that a catch-up started").hasValue(2);
            assertThat(wrapped.resumedAt).as("resumed once, at a live start read for the replay that ran again")
                    .containsExactly(wrapped.issued.get(1));
            assertThat(wrapped.issued.get(1)).isNotEqualTo(handedOver);
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
            assertThat(catchup.isCatchingUp(ID)).as("the takeover replay is over").isFalse();

            wrapped.deliver(liveEvent("e5", 5));
            assertThat(received).as("the resumed subscription delivers what is written after it").endsWith("e4", "e5");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void the_catch_up_answers_that_the_subscription_runs_and_is_not_paused_while_it_replays_again_after_taking_it_over() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            Sinks.Empty<Void> replayBlocked = reader.blockReads();

            wrapped.opensAndLosesTheLiveStart();

            assertThat(wrapped.isPaused(ID)).as("the wrapped model holds it paused meanwhile").isTrue();
            assertThat(catchup.isCatchingUp(ID)).isTrue();
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
            assertThat(wrapped.resumedAt).as("not resumed before the replay is over").isEmpty();

            replayBlocked.tryEmitEmpty();

            assertThat(wrapped.resumedAt).hasSize(1);
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
            assertThat(received).endsWith("e4");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_pause_while_the_catch_up_replays_again_after_taking_the_subscription_over_leaves_it_paused_after_the_handover_and_a_resume_delivers() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            Sinks.Empty<Void> replayBlocked = reader.blockReads();
            wrapped.opensAndLosesTheLiveStart();

            catchup.pauseSubscription(ID);
            assertThat(catchup.isPaused(ID)).isTrue();
            assertThat(catchup.isRunning(ID)).isFalse();
            replayBlocked.tryEmitEmpty();

            assertThat(wrapped.resumedAt).as("handed over, at the new live start").hasSize(1);
            assertThat(wrapped.isPaused(ID)).as("and paused once it was").isTrue();
            assertThat(catchup.isPaused(ID)).isTrue();
            assertThat(catchup.isRunning(ID)).isFalse();
            wrapped.deliver(liveEvent("e5", 5));
            assertThat(received).as("a paused subscription delivers nothing").endsWith("e4").doesNotContain("e5");

            catchup.resumeSubscription(ID);

            assertThat(catchup.isRunning(ID)).isTrue();
            wrapped.deliver(liveEvent("e6", 6));
            assertThat(received).endsWith("e4", "e6");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_while_the_catch_up_replays_again_after_taking_the_subscription_over_ends_it_without_resuming_the_wrapped_model() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            Sinks.Empty<Void> replayBlocked = reader.blockReads();
            wrapped.opensAndLosesTheLiveStart();
            assertThat(catchup.isCatchingUp(ID)).as("the replay that runs again is in flight").isTrue();

            catchup.cancelSubscription(ID).block(Duration.ofSeconds(5));
            replayBlocked.tryEmitEmpty();

            assertThat(wrapped.resumedAt).as("the cancelled subscription is never resumed").isEmpty();
            assertThat(received).as("the replay that ran again delivered nothing").containsExactly("e1", "e2", "e3");
            assertThat(wrapped.cancelled).containsExactly(ID);
            assertThat(catchup.isCatchingUp(ID)).isFalse();
            assertThat(catchup.isRunning(ID)).isFalse();
            assertThat(catchup.isPaused(ID)).isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_the_wrapped_model_refuses_while_a_takeover_replays_keeps_the_record_so_a_later_history_loss_pauses_the_subscription_for_good() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            reader.blockReads();
            wrapped.opensAndLosesTheLiveStart();
            assertThat(catchup.isCatchingUp(ID)).as("the takeover replay is in flight").isTrue();
            IllegalStateException refusal = new IllegalStateException("the wrapped model refuses the cancel");
            wrapped.nextCancelThrows = refusal;

            Throwable cancelFailure = catchThrowable(() -> catchup.cancelSubscription(ID));
            Checkpoint lostFrom = new StringBasedCheckpoint("a later lost position");
            wrapped.losesTheHistoryFrom(ID, lostFrom);

            assertAll(
                    () -> assertThat(cancelFailure).as("what the cancel threw").isSameAs(refusal),
                    () -> assertThat(wrapped.cancelled).as("subscriptions the wrapped model cancelled").isEmpty(),
                    () -> assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty(),
                    () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue(),
                    () -> assertThat(catchThrowable(() -> catchup.resumeSubscription(ID))).as("what a resume throws")
                            .isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(lostFrom.asString())
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_whose_mono_from_the_wrapped_model_fails_keeps_the_record_of_a_straight_to_live_subscription_so_a_later_history_loss_pauses_it_for_good() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(new StringBasedCheckpoint("a live token")),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            IllegalStateException refusal = new IllegalStateException("the wrapped model fails the cancel");
            wrapped.cancelCompletesWhen = Mono.error(refusal);

            Throwable cancelFailure = catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5)));
            boolean wrappedRunsTheSubscription = wrapped.isRunning(ID);
            wrapped.losesTheHistoryFrom(ID, new StringBasedCheckpoint("a later lost position"));

            assertAll(
                    () -> assertThat(cancelFailure).as("what the cancel failed with").isSameAs(refusal),
                    () -> assertThat(wrappedRunsTheSubscription).as("whether the wrapped model runs the subscription after the failed cancel").isTrue(),
                    () -> assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty(),
                    () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_whose_mono_from_the_wrapped_model_fails_keeps_the_record_of_a_handed_over_catch_up_so_a_later_history_loss_pauses_it_for_good() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            IllegalStateException refusal = new IllegalStateException("the wrapped model fails the cancel");
            wrapped.cancelCompletesWhen = Mono.error(refusal);

            Throwable cancelFailure = catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5)));
            wrapped.losesTheHistoryFrom(ID, new StringBasedCheckpoint("a later lost position"));

            assertAll(
                    () -> assertThat(cancelFailure).as("what the cancel failed with").isSameAs(refusal),
                    () -> assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty(),
                    () -> assertThat(catchup.isPaused(ID)).as("paused").isTrue()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_that_lands_after_the_wrapped_model_paused_the_subscription_and_before_it_ran_the_takeover_drops_the_takeover() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            wrapped.beforeTheTakeoverRuns = () -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5));

            wrapped.opensAndLosesTheLiveStart();

            assertThat(wrapped.takeOversRun).as("the listener took the subscription over").hasValue(1);
            assertThat(wrapped.resumedAt).isEmpty();
            assertThat(received).as("no replay ran again").containsExactly("e1", "e2", "e3");
            assertThat(catchupsStarted).hasValue(1);
            assertThat(catchup.isCatchingUp(ID)).isFalse();
            assertThat(catchup.isRunning(ID)).isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_takeover_replay_that_fails_leaves_the_subscription_paused_and_a_resume_replays_again_without_resuming_the_wrapped_model_where_it_lost_the_history() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            reader.failReads(new IllegalStateException("the event store is down"));

            wrapped.opensAndLosesTheLiveStart();

            assertThat(wrapped.resumedAt).as("a replay that failed hands nothing over").isEmpty();
            assertThat(catchup.isCatchingUp(ID)).isFalse();
            assertThat(catchup.isPaused(ID)).as("stays with the wrapped model, paused").isTrue();
            assertThat(catchup.isRunning(ID)).isFalse();

            reader.readsAgain();
            catchup.resumeSubscription(ID);

            assertThat(received).endsWith("e1", "e2", "e3", "e4");
            assertThat(wrapped.resumedAt).as("resumed at a live start read for the second takeover replay").containsExactly(wrapped.issued.get(2));
            assertThat(wrapped.resumedWhereItWasHeld).as("resumed where it lost the history").hasValue(0);
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_takeover_that_gave_up_after_losing_the_live_start_of_each_replay_again_replays_again_once_resumed() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            for (int loss = 1; loss <= 3; loss++) {
                wrapped.losesTheNextOpen();
                wrapped.opensAndLosesTheLiveStart();
            }
            wrapped.opensAndLosesTheLiveStart();
            assertThat(wrapped.resumedAt).as("handed over after each of the 3 replays again, and not after the takeover that gave up").hasSize(3);
            assertThat(catchup.isPaused(ID)).as("paused once the takeover gave up").isTrue();
            assertThat(catchup.isRunning(ID)).isFalse();
            received.clear();

            catchup.resumeSubscription(ID);

            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e4");
            assertThat(wrapped.resumedAt).as("resumed at a live start read for the replay the resume ran").hasSize(4).last().isEqualTo(wrapped.issued.getLast());
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_live_start_lost_on_the_thread_of_the_handover_before_it_returns_is_taken_over_and_the_event_written_in_the_gap_is_delivered() {
        verifiesTheGapEventIsDeliveredWhenTheLiveStartIsLostBeforeTheHandOverReturns(LosingLiveStartModel.Loses.ON_THE_THREAD_OF_SUBSCRIBE);
    }

    @Test
    void a_live_start_lost_on_another_thread_while_the_handover_waits_is_taken_over_and_the_event_written_in_the_gap_is_delivered() {
        verifiesTheGapEventIsDeliveredWhenTheLiveStartIsLostBeforeTheHandOverReturns(LosingLiveStartModel.Loses.ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS);
    }

    private void verifiesTheGapEventIsDeliveredWhenTheLiveStartIsLostBeforeTheHandOverReturns(LosingLiveStartModel.Loses loses) {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        wrapped.loses = loses;
        wrapped.beforeTheTakeoverRuns = () -> reader.head = 4;
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            subscription.waitUntilStarted().block(Duration.ofSeconds(5));

            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(catchupsStarted).as("told again that a catch-up started").hasValue(2);
            assertThat(wrapped.resumedAt).as("resumed once, at a live start read for the replay that ran again")
                    .containsExactly(wrapped.issued.get(1));
            assertThat(wrapped.issued.get(1)).isNotEqualTo(wrapped.issued.get(0));
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
            assertThat(catchup.isCatchingUp(ID)).as("the takeover replay is over").isFalse();

            wrapped.deliver(liveEvent("e5", 5));
            assertThat(received).as("the resumed subscription delivers what is written after it").endsWith("e4", "e5");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_pause_asked_during_the_replay_before_the_live_start_is_lost_on_the_thread_of_the_handover_leaves_the_subscription_paused_after_the_takeover_and_a_resume_delivers() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        wrapped.loses = LosingLiveStartModel.Loses.ON_THE_THREAD_OF_SUBSCRIBE;
        AtomicReference<Sinks.Empty<Void>> takeoverReplayBlocked = new AtomicReference<>();
        wrapped.beforeTheTakeoverRuns = () -> {
            reader.head = 4;
            takeoverReplayBlocked.set(reader.blockReads());
        };
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            Sinks.Empty<Void> replayBlocked = reader.blockReads();
            Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            catchup.pauseSubscription(ID);

            replayBlocked.tryEmitEmpty();
            subscription.waitUntilStarted().block(Duration.ofSeconds(5));

            assertThat(catchup.isCatchingUp(ID)).as("the replay that runs again is in flight").isTrue();
            assertThat(catchup.isPaused(ID)).as("paused as asked, while it replays again").isTrue();
            assertThat(catchup.isRunning(ID)).isFalse();

            takeoverReplayBlocked.get().tryEmitEmpty();

            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(wrapped.resumedAt).as("resumed at a live start read for the replay that ran again, and paused after")
                    .containsExactly(wrapped.issued.get(1));
            assertThat(catchup.isPaused(ID)).isTrue();
            assertThat(catchup.isRunning(ID)).isFalse();
            assertThat(wrapped.isPaused(ID)).isTrue();

            catchup.resumeSubscription(ID);
            wrapped.deliver(liveEvent("e5", 5));
            assertThat(received).as("a resume delivers what is written after the new live start").endsWith("e4", "e5");
        } finally {
            catchup.shutdown();
        }
    }

    // The handover that the takeover replaced returns while the takeover replay still runs, and a start(true) of the
    // wrapped model has resumed the subscription where it lost the history
    @Test
    void a_takeover_that_replaces_a_handover_still_under_way_holds_the_live_events_after_that_handover_returns() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        wrapped.loses = LosingLiveStartModel.Loses.ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS;
        AtomicReference<Sinks.Empty<Void>> takeoverReplayBlocked = new AtomicReference<>();
        wrapped.beforeTheTakeoverRuns = () -> {
            reader.head = 4;
            takeoverReplayBlocked.set(reader.blockReads());
            wrapped.resumeSubscription(ID);
        };
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            subscription.waitUntilStarted().block(Duration.ofSeconds(5));
            assertThat(catchup.isCatchingUp(ID)).as("the takeover replay is in flight").isTrue();

            Disposable delivery = wrapped.actionOf(ID).apply(liveEvent("lost", 5)).subscribe();
            assertThat(received).as("events the caller's action was given while the takeover replays").containsExactly("e1", "e2", "e3");

            // As the pause in the takeover's handover does
            delivery.dispose();
            takeoverReplayBlocked.get().tryEmitEmpty();

            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isCatchingUp(ID)).as("the takeover replay is over").isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_after_the_wrapped_model_paused_the_subscription_for_the_takeover_and_before_it_ran_the_takeover_replays_instead_of_resuming_the_wrapped_model_where_it_lost_the_history() {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            AtomicBoolean resumes = new AtomicBoolean(true);
            wrapped.beforeTheTakeoverRuns = () -> {
                if (resumes.compareAndSet(true, false)) {
                    catchup.resumeSubscription(ID);
                }
            };

            wrapped.opensAndLosesTheLiveStart();

            assertThat(wrapped.takeOversRun).as("the listener took the subscription over").hasValue(1);
            assertThat(wrapped.resumedWhereItWasHeld).as("resumed where it lost the history").hasValue(0);
            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(catchupsStarted).as("told again that a catch-up started").hasValue(2);
            assertThat(wrapped.resumedAt).as("resumed once, at a live start read for the replay that ran again").containsExactly(wrapped.issued.get(1));
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_that_a_shutdown_overtakes_after_it_read_the_handover_whose_takeover_was_asked_for_throws_SubscriptionModelShutdownException_and_does_not_resume_the_wrapped_model_where_it_lost_the_history() {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover it hasn't run yet
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);

            // When a shutdown sets its flag right after a resume read the handover
            NamedCatchupMapHook.installIn(catchup, "handedOver").afterTheReadIn("takeOverOnResume", catchup.namedSubscriptions::beginShutdown);
            Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));

            // Then the resume is refused, and the wrapped model is not resumed where it lost the history
            assertAll(
                    () -> assertThat(thrown).as("what the resume throws")
                            .isInstanceOf(SubscriptionModelShutdownException.class).hasMessage("Cannot resume subscription " + ID + " because the subscription model is shutdown."),
                    () -> assertThat(wrapped.resumedWhereItWasHeld).as("resumed where it lost the history").hasValue(0)
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_right_after_a_failed_takeover_put_back_the_handover_runs_the_takeover_again_and_does_not_resume_the_wrapped_model_where_it_lost_the_history() throws InterruptedException {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover whose replay fails
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            HistoryLossHandling.TakeOver lossOfTheLiveStart = (HistoryLossHandling.TakeOver) wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);
            reader.head = 4;
            reader.failReads(new IllegalStateException("the event store is down"));

            // When the subscription is resumed right after the failed takeover put back the handover, so that a resume
            // replays again
            AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
            NamedCatchupMapHook.installIn(catchup, "handedOver").afterTheComputeIfPresentIn("launcher", () -> {
                reader.readsAgain();
                thrown.set(catchThrowable(() -> catchup.resumeSubscription(ID)));
            });
            lossOfTheLiveStart.afterPause().run();
            awaitNoReplayInFlight(catchup);

            // Then the resume runs the takeover again, which delivers the event written in the gap, and the wrapped model is
            // not resumed where it lost the history
            assertAll(
                    () -> assertThat(thrown.get()).as("what the resume throws").isNull(),
                    () -> assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4"),
                    () -> assertThat(wrapped.resumedWhereItWasHeld).as("resumed where it lost the history").hasValue(0)
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_whose_handover_a_failed_takeover_replaced_after_the_resume_read_it_runs_the_takeover_and_does_not_resume_the_wrapped_model_where_it_lost_the_history() throws InterruptedException {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover whose replay fails
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            HistoryLossHandling.TakeOver lossOfTheLiveStart = (HistoryLossHandling.TakeOver) wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);
            reader.head = 4;
            reader.failReads(new IllegalStateException("the event store is down"));

            // When the takeover the wrapped model runs fails and puts back a record of its own after a resume read the
            // handover, and the event store reads again before the resume goes on
            AtomicBoolean takeoverEnded = new AtomicBoolean();
            NamedCatchupMapHook.installIn(catchup, "handedOver").afterTheReadIn("takeOverOnResume", () -> {
                Thread takeover = new Thread(lossOfTheLiveStart.afterPause(), "takeover");
                takeover.start();
                try {
                    takeover.join(10_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                takeoverEnded.set(!takeover.isAlive());
                reader.readsAgain();
            });
            Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));
            awaitNoReplayInFlight(catchup);

            // Then the resume runs the takeover, which delivers the event written in the gap, and the wrapped model is not
            // resumed where it lost the history
            assertAll(
                    () -> assertThat(takeoverEnded).as("whether the failing takeover ended within 10 seconds").isTrue(),
                    () -> assertThat(thrown).as("what the resume throws").isNull(),
                    () -> assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4"),
                    () -> assertThat(wrapped.resumedWhereItWasHeld).as("resumed where it lost the history").hasValue(0)
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_while_a_cancel_of_a_subscription_paused_for_a_takeover_is_under_way_throws_UnknownSubscriptionException_and_does_not_resume_the_wrapped_model_where_it_lost_the_history() {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover it hasn't run yet
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        Sinks.Empty<Void> cancelCompletes = Sinks.empty();
        try {
            subscribeAndHandOver(catchup);
            wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);
            wrapped.cancelCompletesWhen = cancelCompletes.asMono();

            // When the subscription is resumed after a cancel began and before the wrapped model has cancelled it
            catchup.cancelSubscription(ID);
            Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));
            int resumedWhereItWasHeld = wrapped.resumedWhereItWasHeld.get();
            cancelCompletes.tryEmitEmpty();

            // Then the resume is refused as one of a cancelled subscription, and the wrapped model is not resumed where it
            // lost the history
            assertAll(
                    () -> assertThat(thrown).as("what the resume throws").isInstanceOf(UnknownSubscriptionException.class),
                    () -> assertThat(resumedWhereItWasHeld).as("resumed where it lost the history, while the cancel was under way").isZero()
            );
        } finally {
            cancelCompletes.tryEmitEmpty();
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_whose_handover_a_shutdown_cleared_before_the_wrapped_model_is_shut_down_throws_SubscriptionModelShutdownException_and_does_not_resume_the_wrapped_model_where_it_lost_the_history() throws InterruptedException {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover it hasn't run yet
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        CountDownLatch shutsDownTheWrappedModel = new CountDownLatch(1);
        CountDownLatch mayShutDownTheWrappedModel = new CountDownLatch(1);
        try {
            subscribeAndHandOver(catchup);
            wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);
            wrapped.beforeShutdown = () -> {
                shutsDownTheWrappedModel.countDown();
                await(mayShutDownTheWrappedModel);
            };

            // When a shutdown on another thread clears the handovers after the resume passed its first check, and waits
            // to shut down the wrapped model
            Thread shutdown = new Thread(catchup::shutdown, "shutdown");
            NamedCatchupMapHook.installIn(catchup, "owned").afterTheReadIn("resumeSubscription", () -> {
                shutdown.start();
                await(shutsDownTheWrappedModel);
            });
            Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));
            int resumedWhereItWasHeld = wrapped.resumedWhereItWasHeld.get();
            mayShutDownTheWrappedModel.countDown();
            shutdown.join(10_000);

            // Then the resume is refused, and the wrapped model is not resumed where it lost the history
            assertAll(
                    () -> assertThat(thrown).as("what the resume throws")
                            .isInstanceOf(SubscriptionModelShutdownException.class).hasMessage("Cannot resume subscription " + ID + " because the subscription model is shutdown."),
                    () -> assertThat(resumedWhereItWasHeld).as("resumed where it lost the history, before the wrapped model was shut down").isZero(),
                    () -> assertThat(shutdown.isAlive()).as("whether the shutdown still runs 10 seconds after it may shut down the wrapped model").isFalse()
            );
        } finally {
            mayShutDownTheWrappedModel.countDown();
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_while_the_takeover_the_wrapped_model_runs_has_begun_and_not_yet_registered_its_replay_is_refused_as_already_running_and_does_not_resume_the_wrapped_model_where_it_lost_the_history() throws InterruptedException {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            HistoryLossHandling.TakeOver lossOfTheLiveStart = (HistoryLossHandling.TakeOver) wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);
            reader.head = 4;

            // When the wrapped model runs the takeover on its own thread, and a resume comes after that takeover began and
            // before it registered its replay
            CountDownLatch registers = new CountDownLatch(1);
            CountDownLatch mayRegister = new CountDownLatch(1);
            NamedCatchupMapHook.installIn(catchup, "catchingUp").beforeTheComputeIn("takeOver", () -> {
                registers.countDown();
                await(mayRegister);
            });
            Thread takeover = new Thread(lossOfTheLiveStart.afterPause(), "takeover");
            takeover.start();
            await(registers);
            Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));
            int resumedWhereItWasHeld = wrapped.resumedWhereItWasHeld.get();
            mayRegister.countDown();
            takeover.join(10_000);
            awaitNoReplayInFlight(catchup);

            // Then the resume is refused as one of a running subscription, the wrapped model is not resumed where it lost
            // the history, and the takeover delivers the event written in the gap
            assertAll(
                    () -> assertThat(thrown).as("what the resume throws").isInstanceOf(SubscriptionAlreadyRunningException.class),
                    () -> assertThat(resumedWhereItWasHeld).as("resumed where it lost the history, right after the resume").isZero(),
                    () -> assertThat(takeover.isAlive()).as("whether the takeover still runs 10 seconds after it may register").isFalse(),
                    () -> assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4")
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_during_a_shutdown_while_the_takeover_the_wrapped_model_runs_has_begun_and_not_yet_registered_its_replay_throws_SubscriptionModelShutdownException() throws InterruptedException {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        CountDownLatch mayRegister = new CountDownLatch(1);
        try {
            subscribeAndHandOver(catchup);
            HistoryLossHandling.TakeOver lossOfTheLiveStart = (HistoryLossHandling.TakeOver) wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);

            // When the wrapped model runs the takeover on its own thread, and a shutdown begins after a resume read the
            // handover, while that takeover has begun and not yet registered its replay
            CountDownLatch registers = new CountDownLatch(1);
            NamedCatchupMapHook.installIn(catchup, "catchingUp").beforeTheComputeIn("takeOver", () -> {
                registers.countDown();
                await(mayRegister);
            });
            Thread takeover = new Thread(lossOfTheLiveStart.afterPause(), "takeover");
            takeover.start();
            await(registers);
            NamedCatchupMapHook.installIn(catchup, "handedOver").afterTheReadIn("takeOverOnResume", catchup.namedSubscriptions::beginShutdown);
            Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID));
            int resumedWhereItWasHeld = wrapped.resumedWhereItWasHeld.get();
            mayRegister.countDown();
            takeover.join(10_000);

            // Then the resume is refused as one of a shut-down model, and the wrapped model is not resumed where it lost
            // the history
            assertAll(
                    () -> assertThat(thrown).as("what the resume throws")
                            .isInstanceOf(SubscriptionModelShutdownException.class).hasMessage("Cannot resume subscription " + ID + " because the subscription model is shutdown."),
                    () -> assertThat(resumedWhereItWasHeld).as("resumed where it lost the history, right after the resume").isZero(),
                    () -> assertThat(takeover.isAlive()).as("whether the takeover still runs 10 seconds after it may register").isFalse()
            );
        } finally {
            mayRegister.countDown();
            catchup.shutdown();
        }
    }

    @Test
    void a_subscription_whose_takeover_the_wrapped_model_runs_has_begun_and_not_yet_registered_its_replay_is_running_and_not_paused() throws InterruptedException {
        // Given a subscription whose live start the wrapped model lost, and which it paused for a takeover
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        CountDownLatch mayRegister = new CountDownLatch(1);
        try {
            subscribeAndHandOver(catchup);
            HistoryLossHandling.TakeOver lossOfTheLiveStart = (HistoryLossHandling.TakeOver) wrapped.asksTheListeners(ID, wrapped.positionOf(ID));
            wrapped.pauseSubscription(ID);
            reader.head = 4;

            // When the wrapped model runs the takeover on its own thread, which has begun and not yet registered its
            // replay
            CountDownLatch registers = new CountDownLatch(1);
            NamedCatchupMapHook.installIn(catchup, "catchingUp").beforeTheComputeIn("takeOver", () -> {
                registers.countDown();
                await(mayRegister);
            });
            Thread takeover = new Thread(lossOfTheLiveStart.afterPause(), "takeover");
            takeover.start();
            await(registers);
            boolean running = catchup.isRunning(ID);
            boolean paused = catchup.isPaused(ID);
            mayRegister.countDown();
            takeover.join(10_000);
            awaitNoReplayInFlight(catchup);

            // Then the subscription is answered for as one whose replay runs, which a resume meanwhile is refused as
            assertAll(
                    () -> assertThat(running).as("whether the subscription is running before the takeover registered its replay").isTrue(),
                    () -> assertThat(paused).as("whether the subscription is paused before the takeover registered its replay").isFalse(),
                    () -> assertThat(takeover.isAlive()).as("whether the takeover still runs 10 seconds after it may register").isFalse()
            );
        } finally {
            mayRegister.countDown();
            catchup.shutdown();
        }
    }

    @Test
    void a_loss_of_the_lost_live_start_reported_again_as_the_takeover_hands_over_is_taken_over_when_the_model_restarts_from_the_present() {
        verifiesALossReportedAgainAsTheTakeoverHandsOverIsTakenOver(new LosingLiveStartModel(true));
    }

    @Test
    void a_loss_of_the_lost_live_start_reported_again_as_the_takeover_hands_over_is_taken_over_when_the_model_ends_the_subscription() {
        verifiesALossReportedAgainAsTheTakeoverHandsOverIsTakenOver(new LosingLiveStartModel(false));
    }

    // A start(true) of the wrapped model resumes the subscription where it lost the history while the takeover replays,
    // and the wrapped model reports the loss again as the takeover hands over
    private void verifiesALossReportedAgainAsTheTakeoverHandsOverIsTakenOver(LosingLiveStartModel wrapped) {
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            reader.head = 4;
            AtomicBoolean resumes = new AtomicBoolean(true);
            wrapped.beforeTheTakeoverRuns = () -> {
                if (resumes.compareAndSet(true, false)) {
                    wrapped.resumeSubscription(ID);
                    wrapped.losesTheLiveStartOnTheNextCheckWhetherItRuns();
                }
            };

            wrapped.opensAndLosesTheLiveStart();

            assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty();
            assertThat(wrapped.ended).as("subscriptions the wrapped model ended").isEmpty();
            assertThat(wrapped.takeOversRun).as("the losses the listener took over").hasValue(2);
            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(wrapped.resumedAt).as("resumed once, at a live start read for the replay that ran again").containsExactly(wrapped.issued.get(1));
            assertThat(catchup.isRunning(ID)).isTrue();
            assertThat(catchup.isPaused(ID)).isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_start_of_the_wrapped_model_that_resumes_the_subscription_where_it_lost_the_history_just_before_the_takeover_resumes_it_skips_nothing_when_the_model_restarts_from_the_present() {
        verifiesAStartThatResumesTheLostLiveStartJustBeforeTheTakeoverResumesTheSubscriptionSkipsNothing(new LosingLiveStartModel(true));
    }

    @Test
    void a_start_of_the_wrapped_model_that_resumes_the_subscription_where_it_lost_the_history_just_before_the_takeover_resumes_it_skips_nothing_when_the_model_ends_the_subscription() {
        verifiesAStartThatResumesTheLostLiveStartJustBeforeTheTakeoverResumesTheSubscriptionSkipsNothing(new LosingLiveStartModel(false));
    }

    // The start runs after the takeover paused the subscription for its resume at the new live start, and the
    // subscription it resumes loses the history again once that resume has been asked for
    private void verifiesAStartThatResumesTheLostLiveStartJustBeforeTheTakeoverResumesTheSubscriptionSkipsNothing(LosingLiveStartModel wrapped) {
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            Checkpoint lost = wrapped.issued.get(0);
            reader.head = 4;
            wrapped.beforeTheNextResumeAtAPosition = () -> wrapped.resumeSubscription(ID);

            wrapped.opensAndLosesTheLiveStart();
            wrapped.oplogLoses(lost);

            assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty();
            assertThat(wrapped.ended).as("subscriptions the wrapped model ended").isEmpty();
            assertThat(received).as("the history again, now with the event written in the gap").containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4");
            assertThat(wrapped.positionOf(ID)).as("runs from the live start read for the replay that ran again").isEqualTo(wrapped.issued.get(1));
            assertThat(catchup.isRunning(ID)).isTrue();
            wrapped.deliver(liveEvent("e5", 5));
            assertThat(received).as("the resumed subscription delivers what is written after it").endsWith("e4", "e5");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_takeover_whose_resume_at_the_new_live_start_throws_replays_again_once_the_subscription_is_resumed() {
        verifiesATakeoverWhoseResumeThrowsReplaysAgain((catchup, wrapped) -> catchup.resumeSubscription(ID));
    }

    @Test
    void a_takeover_whose_resume_at_the_new_live_start_throws_replays_again_once_a_start_of_the_wrapped_model_resumes_the_subscription_where_it_lost_the_history() {
        verifiesATakeoverWhoseResumeThrowsReplaysAgain((catchup, wrapped) -> {
            wrapped.resumeSubscription(ID);
            wrapped.oplogLoses(wrapped.issued.get(0));
        });
    }

    private void verifiesATakeoverWhoseResumeThrowsReplaysAgain(BiConsumer<ReactorStreamCatchupSubscriptionModel, LosingLiveStartModel> resume) {
        LosingLiveStartModel wrapped = new LosingLiveStartModel(true);
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            subscribeAndHandOver(catchup);
            Checkpoint lost = wrapped.issued.get(0);
            reader.head = 4;
            wrapped.beforeTheNextResumeAtAPosition = () -> {
                throw new IllegalStateException("Refused while the subscription model keeps the subscription");
            };
            wrapped.opensAndLosesTheLiveStart();
            assertThat(wrapped.isPaused(ID)).as("held paused once the resume at the new live start was refused").isTrue();

            resume.accept(catchup, wrapped);
            wrapped.oplogLoses(lost);

            assertThat(wrapped.restartedFromThePresent).as("restarts from the present").isEmpty();
            assertThat(received).as("the history twice more, each time with the event written in the gap")
                    .containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e4", "e1", "e2", "e3", "e4");
            assertThat(wrapped.positionOf(ID)).as("runs from the live start read for the replay that ran once more").isEqualTo(wrapped.issued.get(2));
            assertThat(catchup.isRunning(ID)).isTrue();
        } finally {
            catchup.shutdown();
        }
    }

    private ReactorStreamCatchupSubscriptionModel catchupOver(LosingLiveStartModel wrapped) {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                catchupsStarted.incrementAndGet();
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        return catchup;
    }

    private void subscribeAndHandOver(ReactorStreamCatchupSubscriptionModel catchup) {
        Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
        subscription.waitUntilStarted().block(Duration.ofSeconds(5));
        assertThat(received).containsExactly("e1", "e2", "e3");
    }

    // Until no replay of the id is in flight, as when a takeover replay has handed over
    private static void awaitNoReplayInFlight(ReactorStreamCatchupSubscriptionModel catchup) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (catchup.isCatchingUp(ID)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("A replay of " + ID + " still in flight after 10 seconds");
            }
            Thread.sleep(10);
        }
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

    private static CloudEvent liveEvent(String id, long position) {
        return OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build(), position);
    }

    // Takes the place of a map of the catch-up and runs a hook once, right after the first read of it, right before the
    // first compute on it, or right after the first computeIfPresent on it that a given method of the catch-up makes
    // itself. That puts the hook between two steps of a
    // call, which no thread schedule reaches every time.
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final class NamedCatchupMapHook extends ConcurrentHashMap {
        private final AtomicReference<@Nullable String> readIn = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterRead = new AtomicReference<>();
        private final AtomicReference<@Nullable String> computeIn = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeCompute = new AtomicReference<>();
        private final AtomicReference<@Nullable String> computeIfPresentIn = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterComputeIfPresent = new AtomicReference<>();

        static NamedCatchupMapHook installIn(ReactorStreamCatchupSubscriptionModel model, String fieldName) {
            NamedCatchupMapHook hook = new NamedCatchupMapHook();
            try {
                Field field = NamedCatchupSupport.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                hook.putAll((Map) field.get(model.namedSubscriptions));
                field.set(model.namedSubscriptions, hook);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
            return hook;
        }

        void afterTheReadIn(String method, Runnable hook) {
            readIn.set(method);
            afterRead.set(hook);
        }

        void beforeTheComputeIn(String method, Runnable hook) {
            computeIn.set(method);
            beforeCompute.set(hook);
        }

        void afterTheComputeIfPresentIn(String method, Runnable hook) {
            computeIfPresentIn.set(method);
            afterComputeIfPresent.set(hook);
        }

        @Override
        public Object get(Object key) {
            Object value = super.get(key);
            runOnceIfCalledFrom(readIn, afterRead);
            return value;
        }

        @Override
        public Object compute(Object key, BiFunction remapping) {
            runOnceIfCalledFrom(computeIn, beforeCompute);
            return super.compute(key, remapping);
        }

        @Override
        public Object computeIfPresent(Object key, BiFunction remapping) {
            Object value = super.computeIfPresent(key, remapping);
            runOnceIfCalledFrom(computeIfPresentIn, afterComputeIfPresent);
            return value;
        }

        private static void runOnceIfCalledFrom(AtomicReference<@Nullable String> method, AtomicReference<@Nullable Runnable> hook) {
            String name = method.get();
            if (name == null || hook.get() == null || !calledFrom(name)) {
                return;
            }
            Runnable run = hook.getAndSet(null);
            if (run != null) {
                method.set(null);
                run.run();
            }
        }

        private static boolean calledFrom(String method) {
            return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE).walk(frames -> frames
                    .filter(frame -> frame.getDeclaringClass() == NamedCatchupSupport.class)
                    .findFirst()
                    .map(frame -> frame.getMethodName().equals(method) || frame.getMethodName().startsWith("lambda$" + method + "$"))
                    .orElse(false));
        }
    }

    // Events at positions 1 to head, where a read can be blocked or failed
    static final class ScriptedReader implements UnscopedPositionOrderedReader {
        volatile long head = 3;
        private final AtomicReference<Mono<Void>> beforeEachRead = new AtomicReference<>(Mono.empty());

        Sinks.Empty<Void> blockReads() {
            Sinks.Empty<Void> sink = Sinks.empty();
            beforeEachRead.set(sink.asMono());
            return sink;
        }

        void failReads(Throwable failure) {
            beforeEachRead.set(Mono.error(failure));
        }

        void readsAgain() {
            beforeEachRead.set(Mono.empty());
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.defer(() -> {
                long from = range.afterPosition().orElse(0L) + 1;
                long to = range.upToPosition().orElse(0L);
                return beforeEachRead.get().thenMany(Flux.fromStream(LongStream.rangeClosed(from, to).boxed()
                        .map(position -> CloudEventBuilder.v1().withId("e" + position).withSource(URI.create("urn:test")).withType("type").build())));
            });
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.fromSupplier(() -> head);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Loses the live start it opens at when the test says so and asks its listeners as the Mongo model does.
    // Without a taker, restarts from the present or ends the subscription.
    static final class LosingLiveStartModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {
        private enum State {RUNNING, PAUSED}

        // ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS has subscribe wait for the thread that runs the takeover, while the
        // handover holds the state monitor. It may wait only because no cancel runs in the tests that use it. The
        // NamedCatchupSupport javadoc forbids this for a production wrapped model, since a cancel holding the id lock and
        // waiting for that monitor would deadlock with the takeover waiting for the id lock.
        enum Loses {WHEN_THE_TEST_SAYS, ON_THE_THREAD_OF_SUBSCRIBE, ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS}

        private record Held(Function<CloudEvent, Mono<Void>> action, Checkpoint position, State state) {
            Held at(Checkpoint position, State state) {
                return new Held(action, position, state);
            }
        }

        private final boolean restartsFromThePresent;
        private final AtomicInteger tokens = new AtomicInteger();
        private final ConcurrentMap<String, Held> held = new ConcurrentHashMap<>();
        private final List<HistoryLossListener> listeners = new CopyOnWriteArrayList<>();
        private final AtomicInteger opensToLose = new AtomicInteger(1);
        private volatile @Nullable String openThatLoses;
        final List<Checkpoint> issued = new CopyOnWriteArrayList<>();
        final List<Checkpoint> resumedAt = new CopyOnWriteArrayList<>();
        final List<String> cancelled = new CopyOnWriteArrayList<>();
        final AtomicInteger takeOversRun = new AtomicInteger();
        final AtomicInteger resumedWhereItWasHeld = new AtomicInteger();
        final List<Checkpoint> restartedFromThePresent = new CopyOnWriteArrayList<>();
        final List<String> ended = new CopyOnWriteArrayList<>();
        private final AtomicBoolean losesOnTheNextCheckWhetherItRuns = new AtomicBoolean();
        volatile Runnable beforeTheTakeoverRuns = () -> {
        };
        // The next cancelSubscription(id) throws this synchronously, before it cancels anything
        volatile @Nullable RuntimeException nextCancelThrows;
        volatile Runnable beforeTheNextResumeAtAPosition = () -> {
        };
        // A cancel subscribed to removes the subscription only once this completes, so a test holds a cancel under way
        volatile Mono<Void> cancelCompletesWhen = Mono.empty();
        // Runs on the thread of every subscribe(id, ..) before the model registers the subscription
        volatile Runnable beforeSubscribeRegisters = () -> {
        };
        // Runs on the thread of every shutdown()
        volatile Runnable beforeShutdown = () -> {
        };
        final AtomicInteger subscribeCalls = new AtomicInteger();
        final AtomicInteger starts = new AtomicInteger();
        private volatile @Nullable Checkpoint refusedStart;
        private volatile @Nullable RuntimeException refusal;
        volatile boolean startResumesPausedSubscriptions;
        volatile Loses loses = Loses.WHEN_THE_TEST_SAYS;
        volatile UnaryOperator<Checkpoint> reportsTheLostPositionAs = UnaryOperator.identity();

        LosingLiveStartModel(boolean restartsFromThePresent) {
            this.restartsFromThePresent = restartsFromThePresent;
        }

        Checkpoint positionOf(String subscriptionId) {
            return held.get(subscriptionId).position();
        }

        // The action the subscription runs, for a test that gives it an event without blocking
        Function<CloudEvent, Mono<Void>> actionOf(String subscriptionId) {
            return held.get(subscriptionId).action();
        }

        void losesTheNextOpen() {
            opensToLose.set(1);
        }

        // The next subscribe(id, ..) at start throws failure instead of registering the subscription
        void refusesTheSubscribeAt(Checkpoint start, RuntimeException failure) {
            refusal = failure;
            refusedStart = start;
        }

        // What the listeners answer for a loss, without pausing or restarting anything
        HistoryLossHandling asksTheListeners(String subscriptionId, Checkpoint lostFrom) {
            for (HistoryLossListener listener : listeners) {
                HistoryLossHandling handling = listener.handleHistoryLoss(subscriptionId, lostFrom);
                if (handling instanceof HistoryLossHandling.TakeOver) {
                    return handling;
                }
            }
            return HistoryLossHandling.leaveToSubscriptionModel();
        }

        // The oplog loses the history from lostFrom for a subscription, wherever it opened
        void losesTheHistoryFrom(String subscriptionId, Checkpoint lostFrom) {
            reportsLoss(subscriptionId, lostFrom);
        }

        // The subscription fails in a way the model does not retry, and the model no longer holds it, as the Mongo model
        // drops a run that ends with an unrecoverable error
        void endsTheSubscriptionOnItsOwn(String subscriptionId) {
            held.remove(subscriptionId);
        }

        void losesTheLiveStartOnTheNextCheckWhetherItRuns() {
            losesOnTheNextCheckWhetherItRuns.set(true);
        }

        // The open that read the lost live start, the only call that asks the listeners
        void opensAndLosesTheLiveStart() {
            String subscriptionId = openThatLoses;
            openThatLoses = null;
            losesTheLiveStartOf(subscriptionId);
        }

        // A subscription that runs from a position the oplog no longer holds loses it, and one that runs from
        // anywhere else doesn't
        void oplogLoses(Checkpoint lost) {
            List.copyOf(held.keySet()).forEach(subscriptionId -> {
                Held current = held.get(subscriptionId);
                if (current != null && current.state() == State.RUNNING && current.position().equals(lost)) {
                    losesTheLiveStartOf(subscriptionId);
                }
            });
        }

        private void losesTheLiveStartOf(String subscriptionId) {
            reportsLoss(subscriptionId, held.get(subscriptionId).position());
        }

        private void reportsLoss(String subscriptionId, Checkpoint lostFrom) {
            for (HistoryLossListener listener : listeners) {
                if (listener.handleHistoryLoss(subscriptionId, reportsTheLostPositionAs.apply(lostFrom)) instanceof HistoryLossHandling.TakeOver takeOver) {
                    held.computeIfPresent(subscriptionId, (id, current) -> current.at(current.position(), State.PAUSED));
                    beforeTheTakeoverRuns.run();
                    takeOversRun.incrementAndGet();
                    takeOver.afterPause().run();
                    return;
                }
            }
            if (restartsFromThePresent) {
                Checkpoint present = newToken();
                restartedFromThePresent.add(present);
                held.computeIfPresent(subscriptionId, (id, current) -> current.at(present, State.RUNNING));
            } else {
                ended.add(subscriptionId);
                held.remove(subscriptionId);
            }
        }

        // Delivers to every running subscription
        void deliver(CloudEvent cloudEvent) {
            held.values().stream().filter(subscription -> subscription.state() == State.RUNNING)
                    .forEach(subscription -> subscription.action().apply(cloudEvent).block(Duration.ofSeconds(5)));
        }

        private Checkpoint newToken() {
            Checkpoint token = new StringBasedCheckpoint("live start " + tokens.incrementAndGet());
            issued.add(token);
            return token;
        }

        private void opened(String subscriptionId) {
            if (opensToLose.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                openThatLoses = subscriptionId;
            }
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.fromSupplier(this::newToken);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Flux<CloudEvent> subscribeFailingIfStartLost(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Flux<CloudEvent> subscribeFailingOnHistoryLoss(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribeCalls.incrementAndGet();
            beforeSubscribeRegisters.run();
            Checkpoint position = ((StartAt.StartAtCheckpoint) startAt).checkpoint;
            RuntimeException refused = refusal;
            if (refused != null && position.equals(refusedStart)) {
                refusal = null;
                throw refused;
            }
            held.put(subscriptionId, new Held(action, position, State.RUNNING));
            opened(subscriptionId);
            if (openThatLoses != null && loses != Loses.WHEN_THE_TEST_SAYS) {
                losesBeforeSubscribeReturns();
            }
            return subscription(subscriptionId);
        }

        private void losesBeforeSubscribeReturns() {
            if (loses == Loses.ON_THE_THREAD_OF_SUBSCRIBE) {
                opensAndLosesTheLiveStart();
                return;
            }
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread loser = new Thread(() -> {
                try {
                    opensAndLosesTheLiveStart();
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            }, "history-loss");
            loser.setDaemon(true);
            loser.start();
            try {
                loser.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while the takeover ran", e);
            }
            if (loser.isAlive()) {
                throw new AssertionError("The takeover did not return within 5 seconds while subscribe waited for it");
            }
            if (failure.get() != null) {
                throw new AssertionError("The takeover failed", failure.get());
            }
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            Runnable beforeThisResume = beforeTheNextResumeAtAPosition;
            beforeTheNextResumeAtAPosition = () -> {
            };
            beforeThisResume.run();
            Checkpoint position = ((StartAt.StartAtCheckpoint) startAt).checkpoint;
            resumedAt.add(position);
            Held current = held.get(subscriptionId);
            if (current == null) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            if (current.state() == State.RUNNING) {
                throw new SubscriptionAlreadyRunningException(subscriptionId);
            }
            held.put(subscriptionId, current.at(position, State.RUNNING));
            opened(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            Held current = held.get(subscriptionId);
            if (current == null) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            if (current.state() == State.RUNNING) {
                throw new SubscriptionAlreadyRunningException(subscriptionId);
            }
            resumedWhereItWasHeld.incrementAndGet();
            held.put(subscriptionId, current.at(current.position(), State.RUNNING));
            opened(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            held.computeIfPresent(subscriptionId, (id, current) -> current.at(current.position(), State.PAUSED));
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            RuntimeException refused = nextCancelThrows;
            if (refused != null) {
                nextCancelThrows = null;
                throw refused;
            }
            return Mono.defer(() -> cancelCompletesWhen).then(Mono.fromRunnable(() -> {
                cancelled.add(subscriptionId);
                held.remove(subscriptionId);
            }));
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (losesOnTheNextCheckWhetherItRuns.compareAndSet(true, false)) {
                losesTheLiveStartOf(subscriptionId);
            }
            Held current = held.get(subscriptionId);
            return current != null && current.state() == State.RUNNING;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            Held current = held.get(subscriptionId);
            return current != null && current.state() == State.PAUSED;
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void stop() {
        }

        @Override
        public void shutdown() {
            beforeShutdown.run();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            starts.incrementAndGet();
            if (resumeSubscriptionsAutomatically && startResumesPausedSubscriptions) {
                List.copyOf(held.keySet()).forEach(subscriptionId -> {
                    Held current = held.get(subscriptionId);
                    if (current != null && current.state() == State.PAUSED) {
                        held.put(subscriptionId, current.at(current.position(), State.RUNNING));
                        opened(subscriptionId);
                    }
                });
            }
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            listeners.remove(listener);
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }
    }
}
