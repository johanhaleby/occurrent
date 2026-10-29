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

import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReads;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
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
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins what {@link ReactorCatchupSubscriptionModel} does with a subscription id it did not create.
 * <p>
 * The dispatcher holds three inner catch-up models and routes each named subscription to one of them, so a
 * per-subscription life-cycle call has to reach the right one. For an id it never created there is no routing decision
 * to look up, and the answer chosen here is to forward rather than to refuse: every inner model delegates to the same
 * wrapped model, so one forward through any of them is that model's own answer. Refusing would break the documented
 * behaviour of the calls that are allowed to be asked about an unknown id, such as cancelling one.
 * <p>
 * The forward has to happen exactly once. Fanning the call out to all three inner models, which is what the blocking
 * {@code CatchupSubscriptionModel} does for its two predicates, would reach the wrapped model three times, and pausing
 * an already paused subscription throws.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupSubscriptionModelOwnershipTest {

    private static final String NEVER_CREATED_HERE = "an-id-this-dispatcher-never-created";

    @Test
    void a_life_cycle_call_for_an_id_it_did_not_create_reaches_the_wrapped_model_exactly_once() {
        RecordingSubscriptionModel wrapped = new RecordingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = dualModeOver(wrapped);

        catchup.isRunning(NEVER_CREATED_HERE);
        catchup.isPaused(NEVER_CREATED_HERE);
        catchup.pauseSubscription(NEVER_CREATED_HERE);
        catchup.resumeSubscription(NEVER_CREATED_HERE);
        catchup.cancelSubscription(NEVER_CREATED_HERE);

        // One call each, not one per inner model: the three inner models share this wrapped model, so a fan-out would
        // ask it the same question three times and pause it twice over.
        assertThat(wrapped.calls).containsExactly(
                "isRunning:" + NEVER_CREATED_HERE,
                "isPaused:" + NEVER_CREATED_HERE,
                "pauseSubscription:" + NEVER_CREATED_HERE,
                "resumeSubscription:" + NEVER_CREATED_HERE,
                "cancelSubscription:" + NEVER_CREATED_HERE);
    }

    @Test
    void it_does_not_refuse_an_id_it_did_not_create() {
        ReactorCatchupSubscriptionModel catchup = dualModeOver(new RecordingSubscriptionModel());

        // The dispatcher answers from the wrapped model rather than throwing on the grounds that it has no routing
        // record for the id. Whether a given call is legal for an unknown id is the wrapped model's contract, not this
        // model's, and cancelling one is explicitly allowed and stops nothing.
        assertThatCode(() -> {
            catchup.isRunning(NEVER_CREATED_HERE);
            catchup.isPaused(NEVER_CREATED_HERE);
            catchup.cancelSubscription(NEVER_CREATED_HERE);
        }).doesNotThrowAnyException();
    }

    @Test
    void an_id_it_did_create_is_answered_by_the_model_it_was_routed_to() {
        RecordingSubscriptionModel wrapped = new RecordingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = dualModeOver(wrapped);

        // A live start hands straight over to the wrapped model, so the routed inner model has no replay of its own to
        // answer from and the question reaches the wrapped model, once, exactly as for an id the dispatcher never saw.
        catchup.subscribe("routed-subscription", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        wrapped.calls.clear();

        catchup.isRunning("routed-subscription");

        assertThat(wrapped.calls).containsExactly("isRunning:routed-subscription");
    }

    @Test
    void a_cancel_the_wrapped_model_refuses_leaves_the_subscription_routed_to_the_model_that_replayed_it() {
        HistoryLosingSubscriptionModel wrapped = new HistoryLosingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = new ReactorCatchupSubscriptionModel(wrapped, new OneEventReader(), new UnusedDcbEventStore(), DcbCriteria.all(), Filter.all());

        Subscription replayed = catchup.subscribe("replayed", AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), __ -> Mono.empty());
        StepVerifier.create(replayed.waitUntilStarted()).expectComplete().verify(Duration.ofSeconds(10));
        wrapped.refusesTheCancel(new IllegalArgumentException("refused by the wrapped model"));
        assertThatThrownBy(() -> catchup.cancelSubscription("replayed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("refused by the wrapped model");
        // Routed to the agnostic model, which is not the stream model the dispatcher falls back to, and with the replay
        // over there is nothing for it to find by asking.
        assertThat(catchup.isCatchingUp("replayed")).isFalse();

        wrapped.losesTheHistoryFrom("replayed", new StringBasedCheckpoint("elsewhere"));

        // Refused only by the model that holds the record of the loss. Any other inner model forwards the resume to the
        // wrapped model, which resumes it.
        assertThatThrownBy(() -> catchup.resumeSubscription("replayed"))
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContaining("elsewhere");
    }

    @Test
    void a_history_loss_reported_while_the_wrapped_model_still_cancels_keeps_the_resume_refused() {
        HistoryLosingSubscriptionModel wrapped = new HistoryLosingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = new ReactorCatchupSubscriptionModel(wrapped, new OneEventReader(), new UnusedDcbEventStore(), DcbCriteria.all(), Filter.all());
        Subscription replayed = catchup.subscribe("replayed", AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), __ -> Mono.empty());
        StepVerifier.create(replayed.waitUntilStarted()).expectComplete().verify(Duration.ofSeconds(10));
        Sinks.Empty<Void> cancelEnds = wrapped.holdsTheCancel();

        // Nothing subscribes to what the cancel returns
        catchup.cancelSubscription("replayed");
        wrapped.losesTheHistoryFrom("replayed", new StringBasedCheckpoint("elsewhere"));

        assertThatThrownBy(() -> catchup.resumeSubscription("replayed"))
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContaining("elsewhere");

        cancelEnds.tryEmitEmpty();
        assertThatCode(() -> catchup.subscribe("replayed", AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.now(), __ -> Mono.empty()))
                .as("a subscribe of the id once the wrapped model has cancelled")
                .doesNotThrowAnyException();
    }

    @Test
    void a_subscribe_from_a_handler_that_a_cancel_runs_is_refused_until_that_cancel_has_ended() {
        HistoryLosingSubscriptionModel wrapped = new HistoryLosingSubscriptionModel();
        ReactorCatchupSubscriptionModel catchup = new ReactorCatchupSubscriptionModel(wrapped, new StalledReader(), new UnusedDcbEventStore(), DcbCriteria.all(), Filter.all());
        Subscription replaying = catchup.subscribe("replaying", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), __ -> Mono.empty());
        AtomicReference<@Nullable RuntimeException> resubscribe = new AtomicReference<>();
        replaying.waitUntilStarted().subscribe(unused -> {
        }, notStarted -> {
            catchup.cancelSubscription("replaying");
            try {
                catchup.subscribe("replaying", AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.now(), __ -> Mono.empty());
            } catch (RuntimeException e) {
                resubscribe.set(e);
            }
        });

        catchup.cancelSubscription("replaying");

        // Routed to another inner model. The wrapped model cancels the id only once this handler returns, which would
        // cancel a subscription made here too.
        assertThat(resubscribe.get()).isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThatCode(() -> catchup.subscribe("replaying", AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.now(), __ -> Mono.empty()))
                .as("a subscribe of the id once the cancel has ended")
                .doesNotThrowAnyException();
    }

    private static ReactorCatchupSubscriptionModel dualModeOver(RecordingSubscriptionModel wrapped) {
        return new ReactorCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader(), new UnusedDcbEventStore(), DcbCriteria.all(), Filter.all());
    }

    // Records every per-subscription life-cycle call it receives, so the test can count the forwards rather than only
    // observe their result.
    private static final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.empty();
        }

        @Override
        // No position, as globalCheckpoint() answers
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            calls.add("subscribe:" + subscriptionId);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            calls.add("stop");
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            calls.add("start");
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            calls.add("isRunning:" + subscriptionId);
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            calls.add("isPaused:" + subscriptionId);
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            calls.add("resumeSubscription:" + subscriptionId);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            calls.add("pauseSubscription:" + subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            calls.add("cancelSubscription:" + subscriptionId);
            return Mono.empty();
        }
    }

    // Reports a lost history to its listeners as a model that loses it does, and pauses the subscription when one takes it over
    private static final class HistoryLosingSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private final List<HistoryLossListener> listeners = new CopyOnWriteArrayList<>();
        private volatile @Nullable RuntimeException cancelRefusal;
        private volatile Mono<Void> cancelEnds = Mono.empty();

        void refusesTheCancel(RuntimeException refusal) {
            cancelRefusal = refusal;
        }

        // A cancel from here on keeps the subscription until the test completes the sink
        Sinks.Empty<Void> holdsTheCancel() {
            Sinks.Empty<Void> held = Sinks.empty();
            cancelEnds = held.asMono();
            return held;
        }

        void losesTheHistoryFrom(String subscriptionId, Checkpoint lostFrom) {
            for (HistoryLossListener listener : listeners) {
                if (listener.handleHistoryLoss(subscriptionId, lostFrom) instanceof HistoryLossHandling.TakeOver takeOver) {
                    paused.add(subscriptionId);
                    takeOver.afterPause().run();
                    return;
                }
            }
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
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
            subscribed.add(subscriptionId);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            listeners.remove(listener);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            throw new AssertionError("No takeover replay is expected, so nothing is resumed at a position");
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribed.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (!subscribed.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.remove(subscriptionId);
            return new StartedSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            RuntimeException refusal = cancelRefusal;
            if (refusal != null) {
                throw refusal;
            }
            return cancelEnds.then(Mono.fromRunnable(() -> {
                subscribed.remove(subscriptionId);
                paused.remove(subscriptionId);
            }));
        }
    }

    private static final class OneEventReader implements PositionOrderedReader, CatchupReads {
        @Override
        public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
            return CatchupReads.unscoped(this).openCatchupReadScope(notBefore);
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = Math.min(range.upToPosition().orElse(1L), 1L);
            return from > to ? Flux.empty() : Flux.just(CloudEventBuilder.v1().withId("e1").withSource(URI.create("urn:test")).withType("type").build());
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(1L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Never reads an event, so a replay stays catching up until it is cancelled
    private static final class StalledReader implements PositionOrderedReader, CatchupReads {
        @Override
        public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
            return CatchupReads.unscoped(this).openCatchupReadScope(notBefore);
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.never();
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(1L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private record StartedSubscription(String id) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return Mono.empty();
        }
    }

    private static final class UnusedPositionOrderedReader implements PositionOrderedReader, CatchupReads {
        @Override
        public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
            return CatchupReads.unscoped(this).openCatchupReadScope(notBefore);
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("readInPositionOrder must not be called when nothing replays"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called when nothing replays"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class UnusedDcbEventStore implements DcbEventStore, DcbCatchupReads {
        @Override
        public Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore) {
            return DcbCatchupReads.unscoped(this).openDcbCatchupReadScope(notBefore);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("read must not be called when nothing replays"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.error(new AssertionError("append must not be called"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.error(new AssertionError("append must not be called"));
        }
    }
}
