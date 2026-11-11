package org.occurrent.subscription.reactor.durable.catchup;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionIdsThrowTest.PutIfAbsentHook;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModelHistoryLossTest.LosingLiveStartModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorStreamCatchupSubscriptionModelHistoryLossTest.ScriptedReader;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionIdsThrowTest.awaitNoReplayInFlight;

// A takeover replay in flight for a subscription whose subscribe failed, or whose cancel was refused, and the handover
// a refused cancel puts back
class ReactorCatchupTakeoverInFlightTest {
    private static final String ID = "sub";

    private final ScriptedReader reader = new ScriptedReader();
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

    @Test
    void a_takeover_registered_before_the_wrapped_subscribe_of_the_handover_threw_hands_over_and_takes_a_later_loss_of_its_live_start_over() {
        // Given a handover whose wrapped subscribe reports the loss of the live start it opened at, on another thread,
        // and then throws while it still holds the subscription paused for the takeover
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try (ErrorsLoggedByTheCatchup logged = new ErrorsLoggedByTheCatchup()) {
            AtomicReference<Sinks.Empty<Void>> reads = new AtomicReference<>();
            wrapped.loses = LosingLiveStartModel.Loses.ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS;
            wrapped.losesTheNextOpen();
            wrapped.beforeTheNextSubscribe = () -> reads.set(reader.blockReads());
            wrapped.nextSubscribeThrows = new IllegalStateException("the wrapped subscribe throws after it reported the loss of its live start");

            // When the subscribe's start fails, the takeover hands over, and the wrapped model then loses the history
            // from the live start that takeover handed over at
            Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            Throwable started = catchThrowable(() -> subscription.waitUntilStarted().block(Duration.ofSeconds(5)));
            reads.get().tryEmitEmpty();
            reader.readsAgain();
            awaitNoReplayInFlight(catchup);
            wrapped.losesTheHistoryFrom(ID, wrapped.positionOf(ID));
            awaitNoReplayInFlight(catchup);

            // Then the catch-up takes that loss over too, instead of leaving it to the wrapped model
            assertAll(
                    () -> assertThat(started).as("what the subscribe's start fails with").hasMessage("the wrapped subscribe throws after it reported the loss of its live start"),
                    () -> assertThat(wrapped.restartedFromThePresent).as("the positions the wrapped model restarted the subscription from on its own").isEmpty(),
                    () -> assertThat(wrapped.takeOversRun).as("how many losses the wrapped model let the catch-up take over").hasValue(2),
                    () -> assertThat(catchup.isRunning(ID)).as("whether the subscription runs").isTrue(),
                    () -> assertThat(logged.errors()).as("what the catch-up logged").noneMatch(error -> error.contains("dead until it is subscribed again"))
                            .anyMatch(error -> error.contains("a takeover replay in flight takes the subscription over"))
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_resume_replays_again_when_a_takeover_registered_before_the_wrapped_subscribe_of_the_handover_threw_fails() {
        // Given a handover whose wrapped subscribe reports the loss of the live start it opened at, on another thread,
        // and then throws while it still holds the subscription paused for the takeover
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            AtomicReference<Sinks.Empty<Void>> reads = new AtomicReference<>();
            wrapped.loses = LosingLiveStartModel.Loses.ON_ANOTHER_THREAD_WHILE_SUBSCRIBE_WAITS;
            wrapped.losesTheNextOpen();
            wrapped.beforeTheNextSubscribe = () -> reads.set(reader.blockReads());
            wrapped.nextSubscribeThrows = new IllegalStateException("the wrapped subscribe throws after it reported the loss of its live start");
            Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            catchThrowable(() -> subscription.waitUntilStarted().block(Duration.ofSeconds(5)));
            Checkpoint lostLiveStart = wrapped.positionOf(ID);

            // When the takeover replay fails, and the subscription is resumed once the reader reads again
            reader.readsAgain();
            reads.get().tryEmitError(new IllegalStateException("the takeover's read fails"));
            awaitNoReplayInFlight(catchup);
            boolean pausedAfterTheFailure = catchup.isPaused(ID);
            Throwable resume = catchThrowable(() -> catchup.resumeSubscription(ID).waitUntilStarted().block(Duration.ofSeconds(5)));
            awaitNoReplayInFlight(catchup);

            // Then the catch-up replays again and resumes the subscription at a new live start, instead of resuming
            // the wrapped model where it lost the history
            assertAll(
                    () -> assertThat(pausedAfterTheFailure).as("whether the subscription was paused once the takeover failed").isTrue(),
                    () -> assertThat(resume).as("what the resume throws").isNull(),
                    () -> assertThat(wrapped.resumedWhereItWasHeld).as("how many times the wrapped model resumed the subscription where it held it").hasValue(0),
                    () -> assertThat(wrapped.heldIds().contains(ID) ? wrapped.positionOf(ID) : null).as("where the wrapped model runs the subscription")
                            .isNotNull().isNotEqualTo(lostLiveStart),
                    () -> assertThat(catchup.isRunning(ID)).as("whether the subscription runs").isTrue()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_subscribe_straight_to_live_is_refused_while_a_takeover_registered_during_a_refused_cancel_is_in_flight() {
        // Given a takeover replay in flight that registered while a refused cancel put the handover back, for a
        // subscription the wrapped model then ended on its own
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            Sinks.Empty<Void> reads = takeoverInFlightAfterARefusedCancel(wrapped, catchup);
            boolean catchingUp = catchup.isCatchingUp(ID);
            Set<String> held = wrapped.heldIds();

            // When a subscribe straight to live comes before that takeover ends, and another one after it
            Throwable straight = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(new StringBasedCheckpoint("straight")),
                    cloudEvent -> Mono.empty()));
            reads.tryEmitEmpty();
            reader.readsAgain();
            awaitNoReplayInFlight(catchup);
            Throwable after = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(new StringBasedCheckpoint("straight")),
                    cloudEvent -> Mono.empty()));

            // Then the first is refused, and the takeover repositions nothing the second subscribed
            assertAll(
                    () -> assertThat(catchingUp).as("whether a replay of the id was in flight before the first subscribe").isTrue(),
                    () -> assertThat(held).as("the ids the wrapped model held before the first subscribe").doesNotContain(ID),
                    () -> assertThat(straight).as("what the first subscribe straight to live throws").isInstanceOf(DuplicateSubscriptionIdException.class),
                    () -> assertThat(after).as("what the second subscribe straight to live throws").isNull(),
                    () -> assertThat(wrapped.heldIds().contains(ID) ? wrapped.positionOf(ID) : null).as("where the wrapped model runs the second subscribe")
                            .isEqualTo(new StringBasedCheckpoint("straight"))
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_subscribe_with_catch_up_refused_while_a_takeover_registered_during_a_refused_cancel_is_in_flight_takes_away_the_record_it_put() {
        // Given a takeover replay in flight with no record of the id, which a refused cancel took away
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            takeoverInFlightAfterARefusedCancel(wrapped, catchup);
            Object recordBefore = recordOf(catchup);

            // When a subscribe with catch-up puts its own record and then finds that replay in flight
            Throwable subscribe = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.empty()));

            // Then it is refused and takes that record away again
            assertAll(
                    () -> assertThat(catchup.isCatchingUp(ID)).as("whether a replay of the id is in flight").isTrue(),
                    () -> assertThat(recordBefore).as("the record of the id before the subscribe").isNull(),
                    () -> assertThat(subscribe).as("what the subscribe with catch-up throws").isInstanceOf(DuplicateSubscriptionIdException.class),
                    () -> assertThat(recordOf(catchup)).as("the record of the id after the refused subscribe").isNull()
            );
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_refused_cancel_that_takes_away_the_record_forgets_the_handover_so_a_later_subscription_straight_to_live_at_that_live_start_is_paused_for_good_when_it_loses_it() {
        // Given a refused cancel of a subscription handed over, whose wrapped model ends the subscription on its own
        // right after that cancel put the handover back
        IntrospectableLosingLiveStartModel wrapped = new IntrospectableLosingLiveStartModel();
        ReactorStreamCatchupSubscriptionModel catchup = catchupOver(wrapped);
        try {
            Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                    cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            subscription.waitUntilStarted().block(Duration.ofSeconds(5));
            Checkpoint liveStart = wrapped.positionOf(ID);
            wrapped.cancelCompletesWhen = Mono.error(new IllegalStateException("the wrapped cancel fails"));
            PutIfAbsentHook.installIn(catchup, "handedOver").afterThePutIfAbsentIn("putBackTheHandover", () -> wrapped.endsTheSubscriptionOnItsOwn(ID));
            Throwable cancel = catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5)));
            wrapped.cancelCompletesWhen = Mono.empty();

            // When a subscribe straight to live opens at that live start, and the wrapped model loses the history from it
            Throwable straight = catchThrowable(() -> catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(liveStart),
                    cloudEvent -> Mono.empty()));
            wrapped.losesTheHistoryFrom(ID, liveStart);
            Throwable resume = catchThrowable(() -> catchup.resumeSubscription(ID));

            // Then the loss pauses that subscription for good, instead of being taken over with nothing to run
            assertAll(
                    () -> assertThat(cancel).as("what the cancel fails with").hasMessage("the wrapped cancel fails"),
                    () -> assertThat(straight).as("what the subscribe straight to live throws").isNull(),
                    () -> assertThat(resume).as("what a resume after the loss throws")
                            .isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining(liveStart.asString()),
                    () -> assertThat(wrapped.resumedWhereItWasHeld).as("how many times the wrapped model resumed the subscription where it held it").hasValue(0)
            );
        } finally {
            catchup.shutdown();
        }
    }

    // Hands a subscription over, then has a cancel of it refused. As that cancel puts the handover back, the wrapped
    // model loses the history from the live start, which registers a takeover replay waiting on the reader, and then
    // ends the subscription on its own. Returns what lets the reader read again.
    private Sinks.Empty<Void> takeoverInFlightAfterARefusedCancel(IntrospectableLosingLiveStartModel wrapped, ReactorStreamCatchupSubscriptionModel catchup) {
        Subscription subscription = catchup.subscribe(ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
        subscription.waitUntilStarted().block(Duration.ofSeconds(5));
        Checkpoint liveStart = wrapped.positionOf(ID);
        Sinks.Empty<Void> reads = reader.blockReads();
        wrapped.cancelCompletesWhen = Mono.error(new IllegalStateException("the wrapped cancel fails"));
        PutIfAbsentHook.installIn(catchup, "handedOver").afterThePutIfAbsentIn("putBackTheHandover", () -> {
            wrapped.losesTheHistoryFrom(ID, liveStart);
            wrapped.endsTheSubscriptionOnItsOwn(ID);
        });
        Throwable cancel = catchThrowable(() -> catchup.cancelSubscription(ID).block(Duration.ofSeconds(5)));
        wrapped.cancelCompletesWhen = Mono.empty();
        assertThat(cancel).as("what the cancel fails with").hasMessage("the wrapped cancel fails");
        return reads;
    }

    private static @Nullable Object recordOf(ReactorStreamCatchupSubscriptionModel catchup) {
        try {
            Field owned = NamedCatchupSupport.class.getDeclaredField("owned");
            owned.setAccessible(true);
            return ((Map<?, ?>) owned.get(catchup.namedSubscriptions)).get(ID);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private ReactorStreamCatchupSubscriptionModel catchupOver(IntrospectableLosingLiveStartModel wrapped) {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        catchup.listenForCatchup(ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        return catchup;
    }
}
