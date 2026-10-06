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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelTest {

    @Test
    void cancelling_a_named_subscription_before_its_replay_hands_over_fails_the_started_signal_instead_of_completing_it() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        // The reader never finishes its first window, so the replay is still in flight (no handover) when cancel()
        // runs below, which is exactly the race NamedCatchupSupport.cancelSubscription's "not yet handed over" branch covers.
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new StuckPositionOrderedReader());

        Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        catchup.cancelSubscription("sub");

        StepVerifier.create(subscription.waitUntilStarted())
                .verifyErrorSatisfies(throwable -> assertThat(throwable)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("sub")
                        .hasMessageContaining("was cancelled before it started"));
        assertThat(wrapped.subscribeCalls)
                .as("the id never reached the wrapped model, so cancelling here must not either")
                .isEmpty();
        assertThat(wrapped.cancelCalls)
                .as("cancels the wrapped model received, which can hold what an earlier process stored for this id")
                .containsExactly("sub");
    }

    @Test
    void shutting_down_while_a_replay_is_in_flight_fails_the_started_signal_instead_of_leaving_it_waiting() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        ReleasablePositionOrderedReader reader = new ReleasablePositionOrderedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);

        Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        catchup.shutdown();
        // A replay that shutdown did not stop would finish here and hand over.
        reader.release();

        StepVerifier.create(subscription.waitUntilStarted())
                .expectError(SubscriptionModelShutdownException.class)
                .verify(Duration.ofSeconds(5));
        assertThat(wrapped.subscribeCalls).isEmpty();
    }

    @Test
    void shutting_down_a_subscription_parked_by_a_stop_fails_the_started_signal() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NamedRecordingSubscriptionModel(), new StuckPositionOrderedReader());
        catchup.stop();

        Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        catchup.shutdown();

        StepVerifier.create(subscription.waitUntilStarted())
                .expectError(SubscriptionModelShutdownException.class)
                .verify(Duration.ofSeconds(5));
    }

    // The handover asks the wrapped model whether the id runs only for a pause requested during the replay, and it
    // asks after the handover is recorded but before the id is removed from the replaying subscriptions. Shutting
    // down from there reaches a subscription that has handed over, and the wrapped model's start signal decides it.
    @Test
    void shutting_down_after_the_handover_leaves_the_started_signal_to_the_wrapped_model() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        Sinks.Empty<Void> wrappedStarted = Sinks.empty();
        wrapped.startedSignal = wrappedStarted.asMono();
        ReleasablePositionOrderedReader reader = new ReleasablePositionOrderedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);

        Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
        catchup.pauseSubscription("sub");
        AtomicBoolean shutDownDuringHandover = new AtomicBoolean(false);
        wrapped.whenAskedWhetherRunning = () -> {
            catchup.shutdown();
            shutDownDuringHandover.set(true);
        };
        reader.release();
        wrappedStarted.tryEmitEmpty();

        assertThat(wrapped.subscribeCalls).containsExactly("sub");
        assertThat(shutDownDuringHandover).isTrue();
        StepVerifier.create(subscription.waitUntilStarted())
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    // The wrapped model here accepts a subscribe after its shutdown, so only the catch-up model itself can refuse
    // before the replay delivers history into a model that is shut down.
    @Test
    void subscribing_after_the_model_is_shut_down_throws_subscription_model_shutdown_exception_before_replaying_anything() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new GrowingPositionOrderedReader());
        List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        catchup.shutdown();

        assertThatThrownBy(() -> catchup.subscribe("replaying", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent))))
                .isExactlyInstanceOf(SubscriptionModelShutdownException.class);
        assertThatThrownBy(() -> catchup.subscribe("live", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent))))
                .isExactlyInstanceOf(SubscriptionModelShutdownException.class);
        assertThat(delivered).isEmpty();
        assertThat(wrapped.subscribeCalls).isEmpty();
    }

    @Test
    void subscribing_after_the_model_is_shut_down_does_not_evaluate_a_dynamic_start_position() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NamedRecordingSubscriptionModel(), new GrowingPositionOrderedReader());
        AtomicBoolean evaluated = new AtomicBoolean(false);
        catchup.shutdown();

        assertThatThrownBy(() -> catchup.subscribe("dynamic", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.dynamic(() -> {
                    evaluated.set(true);
                    return StartAt.checkpoint(GlobalCheckpoint.of(0));
                }), __ -> Mono.empty()))
                .isExactlyInstanceOf(SubscriptionModelShutdownException.class);
        assertThat(evaluated).isFalse();
    }

    // The wrapped model is asked whether it already runs the id between the subscribe's first shutdown check and its
    // claim on the id, so shutting down from there runs between the two every time.
    @Test
    void a_shutdown_between_the_shutdown_check_and_the_claim_on_the_id_refuses_the_subscribe_before_replaying_anything() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new GrowingPositionOrderedReader());
        List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        wrapped.whenAskedWhetherRunning = catchup::shutdown;

        assertThatThrownBy(() -> catchup.subscribe("replaying", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent))))
                .isExactlyInstanceOf(SubscriptionModelShutdownException.class);
        assertThat(delivered).isEmpty();
        assertThat(wrapped.subscribeCalls).isEmpty();
    }

    // The contract a recording projection is told, rather than one it reads per delivery. The start arrives before
    // anything this catch-up delivers, the boundary arrives after the history that was already there and before the
    // events written since the catch-up started, and both name the same catch-up.
    @Test
    void tells_a_listener_when_a_catch_up_starts_and_when_its_history_has_been_read() {
        GrowingPositionOrderedReader reader = new GrowingPositionOrderedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NamedRecordingSubscriptionModel(), reader);

        List<String> signals = new CopyOnWriteArrayList<>();
        List<Object> episodes = new CopyOnWriteArrayList<>();
        boolean sendsThem = catchup.listenForCatchup("sub", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("started");
                episodes.add(episode);
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
                episodes.add(episode);
            }
        });
        assertThat(sendsThem).isTrue();

        Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> signals.add("delivered")));
        StepVerifier.create(subscription.waitUntilStarted()).verifyComplete();

        assertThat(signals).containsExactly("started", "delivered", "historyRead", "delivered");
        assertThat(episodes).hasSize(2);
        assertThat(episodes.get(0)).isSameAs(episodes.get(1));
    }

    @Test
    void a_replay_start_fails_loudly_when_the_model_reports_no_resume_token() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        // Without a resume token the handover from the replay to live cannot be guaranteed loss-free, so the catch-up
        // errors instead of replaying. The store is never read, the failure happens before the first replay read.
        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.checkpoint(GlobalCheckpoint.of(0))))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void a_live_start_does_not_require_a_resume_token() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        // A non-replay start goes straight to live through the facade, so it neither needs a resume token nor reads
        // history. The fail-loud rule is scoped to replay starts only.
        StepVerifier.create(catchup.subscribe(Filter.all(), StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_with_a_stream_filter_goes_live_for_a_non_replay_start() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe(StreamSubscriptionFilter.filter(Filter.all()), StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_uses_the_default_filter_when_no_filter_is_given() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader(), Filter.all());

        StepVerifier.create(catchup.subscribe((SubscriptionFilter) null, StartAt.now()))
                .verifyComplete();
    }

    @Test
    void generic_subscribe_without_a_filter_or_default_filter_fails() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe((SubscriptionFilter) null, StartAt.now()))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void generic_subscribe_rejects_a_non_stream_filter() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new NoTokenSubscriptionModel(), new UnusedPositionOrderedReader());

        StepVerifier.create(catchup.subscribe(DcbSubscriptionFilter.filter(DcbCriteria.all()), StartAt.now()))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void a_live_event_sharing_only_its_id_with_a_reconciled_event_is_delivered_and_not_suppressed_on_a_named_subscription() {
        // NamedCatchupSupport.subscribeWithCatchup, which every reactor named stream and DCB subscription actually
        // runs through, as opposed to the cold PositionCatchupPipeline.catchup() PositionCatchupPipelineTest exercises.
        // e1 from producer A is read during the reconciliation phase (bulk head 0, reconcile head 1) and recorded in
        // the dedup cache. A live event sharing only its id, from producer B, must still be delivered.
        // The live change stream stamps every event with its position, which the model's own livePredicate requires
        // (getPosition(cloudEvent) > 0), so the fixture must too, unlike the replayed event above.
        CloudEvent fromB = org.occurrent.cloudevents.OccurrentCloudEventExtension.withPosition(io.cloudevents.core.builder.CloudEventBuilder.v1()
                .withId("e1").withSource(java.net.URI.create("urn:producer:b")).withType("type").build(), 2);
        LiveDeliveringNamedSubscriptionModel wrapped = new LiveDeliveringNamedSubscriptionModel(List.of(fromB));
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new BulkEmptyThenOneReconciledEventReader());

        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId() + "@" + cloudEvent.getSource())));

        StepVerifier.create(subscription.waitUntilStarted()).verifyComplete();
        assertThat(received).containsExactly("e1@urn:test", "e1@urn:producer:b");
    }

    // One event already there and one more written while the history is being read, so the catch-up has a history to
    // read and something to deliver afterwards. The head grows on the second read, which is what the reconciliation
    // sees.
    private static final class GrowingPositionOrderedReader implements PositionOrderedReader {
        private final java.util.concurrent.atomic.AtomicInteger headReads = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = range.upToPosition().orElse(0L);
            return Flux.fromStream(java.util.stream.LongStream.rangeClosed(from, to).boxed()
                    .map(position -> io.cloudevents.core.builder.CloudEventBuilder.v1()
                            .withId("e" + position)
                            .withSource(java.net.URI.create("urn:test"))
                            .withType("type")
                            .build()));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.fromSupplier(() -> headReads.incrementAndGet() == 1 ? 1L : 2L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    private static final class NoTokenSubscriptionModel implements CheckpointAwareSubscriptionModel {
        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }

    private static final class UnusedPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("readInPositionOrder must not be called when the catch-up fails loudly"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("currentPosition must not be called when the catch-up fails loudly"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Head sits ahead of the replay's start position, so there is a window to read, and that window never completes:
    // Flux.never() registers a subscriber and then neither emits nor terminates. subscribe() itself still returns
    // immediately (registering a subscriber is not blocking), so the calling thread is never stuck, and the replay
    // is simply left in flight, exactly as if a slow store had not answered the first page yet.
    private static final class StuckPositionOrderedReader implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.never();
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(10L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Like StuckPositionOrderedReader until release(), after which every window it reads is empty, so the replay
    // finishes and hands over.
    private static final class ReleasablePositionOrderedReader implements PositionOrderedReader {
        private final Sinks.Empty<Void> released = Sinks.empty();

        void release() {
            released.tryEmitEmpty();
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return released.asMono().thenMany(Flux.empty());
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.just(10L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // A named subscription model with a resolvable checkpoint, so a catch-up wrapping it can capture a live token
    // and start replaying. Records every named subscribe/cancel it is asked to do, so a test can assert the wrapped
    // model was never told about a subscription whose replay never handed over.
    private static final class NamedRecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final List<String> cancelCalls = new CopyOnWriteArrayList<>();
        volatile Runnable whenAskedWhetherRunning = () -> {
        };
        volatile Mono<Void> startedSignal = Mono.empty();

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribeCalls.add(subscriptionId);
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return startedSignal;
                }
            };
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            cancelCalls.add(subscriptionId);
            return Mono.empty();
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            whenAskedWhetherRunning.run();
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new AssertionError("resumeSubscription must not be called in this test");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }

    // Bulk head 0, so the bulk read (startPosition, 0] is empty, then reconcile head 1, so reconcile (0, 1] reads
    // one event, "e1" from producer A. Mirrors GrowingPositionOrderedReader's two-read shape with a fixed id instead
    // of one derived from position, so a live event sharing only that id can be built against it.
    private static final class BulkEmptyThenOneReconciledEventReader implements PositionOrderedReader {
        private final java.util.concurrent.atomic.AtomicInteger headReads = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = range.upToPosition().orElse(0L);
            if (from > to) {
                return Flux.empty();
            }
            return Flux.just(io.cloudevents.core.builder.CloudEventBuilder.v1()
                    .withId("e1").withSource(java.net.URI.create("urn:test")).withType("type").build());
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.fromSupplier(() -> headReads.incrementAndGet() == 1 ? 0L : 1L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // A named subscription model with a resolvable checkpoint whose named subscribe(..) delivers a fixed, finite
    // list to the action the catch-up hands it (which wraps NamedCatchupSupport's own dedup cache), synchronously,
    // so the handover's live delivery can be observed deterministically.
    private static final class LiveDeliveringNamedSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private final List<CloudEvent> live;

        private LiveDeliveringNamedSubscriptionModel(List<CloudEvent> live) {
            this.live = live;
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            Flux.fromIterable(live).concatMap(action).subscribe();
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

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new AssertionError("resumeSubscription must not be called in this test");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }
}
