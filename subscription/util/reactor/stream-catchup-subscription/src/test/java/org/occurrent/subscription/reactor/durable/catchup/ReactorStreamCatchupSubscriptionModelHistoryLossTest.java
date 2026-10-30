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
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.reactor.RepositionableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static CloudEvent liveEvent(String id, long position) {
        return OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("type").build(), position);
    }

    // Events at positions 1 to head, where a read can be blocked or failed
    private static final class ScriptedReader implements UnscopedPositionOrderedReader {
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
    private static final class LosingLiveStartModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {
        private enum State {RUNNING, PAUSED}

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
        volatile Runnable beforeTheNextResumeAtAPosition = () -> {
        };
        volatile Loses loses = Loses.WHEN_THE_TEST_SAYS;
        volatile UnaryOperator<Checkpoint> reportsTheLostPositionAs = UnaryOperator.identity();

        LosingLiveStartModel(boolean restartsFromThePresent) {
            this.restartsFromThePresent = restartsFromThePresent;
        }

        Checkpoint positionOf(String subscriptionId) {
            return held.get(subscriptionId).position();
        }

        void losesTheNextOpen() {
            opensToLose.set(1);
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
            Held lost = held.get(subscriptionId);
            for (HistoryLossListener listener : listeners) {
                if (listener.handleHistoryLoss(subscriptionId, reportsTheLostPositionAs.apply(lost.position())) instanceof HistoryLossHandling.TakeOver takeOver) {
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
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            Checkpoint position = ((StartAt.StartAtCheckpoint) startAt).checkpoint;
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
            return Mono.fromRunnable(() -> {
                cancelled.add(subscriptionId);
                held.remove(subscriptionId);
            });
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
        public void start(boolean resumeSubscriptionsAutomatically) {
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
