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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

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

    @Test
    @Timeout(30)
    void a_named_catch_up_whose_token_the_model_loses_during_every_replay_fails_after_four_replays_instead_of_replaying_forever() {
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        try {
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));

            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable)
                            .isInstanceOf(CatchupCannotResumeException.class)
                            .hasMessageContaining("sub")
                            .hasMessageContaining("4 replays")
                            .hasMessageMatching("(?s).*replays\\. The longest replay took \\d+ ms .*")
                            .hasMessageContaining("oplog"))
                    .verify(Duration.ofSeconds(20));
            assertThat(received).as("the whole history once for each of the 4 replays")
                    .containsExactly("e1", "e2", "e3", "e1", "e2", "e3", "e1", "e2", "e3", "e1", "e2", "e3");
            assertThat(wrapped.resumeProbes).as("asked once after each replay").hasSize(4);
            assertThat(wrapped.subscribeCalls).as("nothing handed over").isEmpty();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_named_catch_up_that_started_at_the_beginning_and_lost_its_live_start_each_time_is_held_paused_with_its_id_taken_and_a_resume_replays_it_again_and_hands_over() {
        // Given a model that loses the live start in each of the replays
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        try {
            // When
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("4 replays"))
                    .verify(Duration.ofSeconds(20));

            // Then it is held paused with its id taken
            Throwable subscribeAgain = org.assertj.core.api.Assertions.catchThrowable(() -> catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty()));
            assertThat(catchup.isPaused("sub")).as("paused").isTrue();
            assertThat(catchup.isRunning("sub")).as("running").isFalse();
            assertThat(subscribeAgain).as("what a subscribe of the same id throws").isInstanceOf(DuplicateSubscriptionIdException.class);
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();

            // When the model has the live start again and the subscription is resumed
            wrapped.losesEveryToken = false;
            received.clear();
            Subscription resumed = catchup.resumeSubscription("sub");

            // Then it replays again from the beginning and hands over, since that skips nothing
            StepVerifier.create(resumed.waitUntilStarted()).verifyComplete();
            assertThat(received).as("the whole history, replayed again").containsExactly("e1", "e2", "e3");
            assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub");
            assertThat(catchup.isPaused("sub")).as("paused").isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_and_a_start_replay_a_named_catch_up_that_started_at_the_beginning_and_lost_its_live_start_each_time_again_from_the_beginning_and_hand_over() {
        // Given a subscription held paused after a replay from the beginning lost the live start in each of its replays
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        try {
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("4 replays"))
                    .verify(Duration.ofSeconds(20));
            assertThat(catchup.isPaused("sub")).as("paused").isTrue();

            // When the model has the live start again and this model is stopped and started
            wrapped.losesEveryToken = false;
            received.clear();
            catchup.stop();
            catchup.start(true);

            // Then it replays the whole history again and hands over
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub"));
            assertThat(received).as("the whole history, replayed again").containsExactly("e1", "e2", "e3");
            assertThat(catchup.isPaused("sub")).as("paused").isFalse();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_resume_of_a_held_replay_that_a_stop_overtakes_before_it_launches_gets_a_start_that_completes_once_a_start_runs_the_replay_and_hands_over() throws InterruptedException {
        // Given a subscription held paused after a replay from the beginning lost the live start in each of its replays
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        // A second subscribe of the id asks the wrapped model whether it holds the id while it holds the lock of the id, so
        // it keeps a resume of the id waiting for that lock until the test lets it go
        CountDownLatch holdsTheLockOfTheId = new CountDownLatch(1);
        CountDownLatch letGoOfTheLockOfTheId = new CountDownLatch(1);
        AtomicReference<Thread> holder = new AtomicReference<>();
        AtomicReference<Subscription> resumed = new AtomicReference<>();
        AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
        try {
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("4 replays"))
                    .verify(Duration.ofSeconds(20));
            wrapped.losesEveryToken = false;
            received.clear();
            wrapped.whenAskedWhetherRunning = () -> {
                boolean underTheLockOfTheId = StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("underIdLock")));
                if (Thread.currentThread() == holder.get() && underTheLockOfTheId) {
                    holdsTheLockOfTheId.countDown();
                    try {
                        letGoOfTheLockOfTheId.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            Thread holding = new Thread(() -> org.assertj.core.api.Assertions.catchThrowable(() -> catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty())));
            holder.set(holding);
            holding.start();
            assertThat(holdsTheLockOfTheId.await(10, TimeUnit.SECONDS)).as("the lock of the id is held").isTrue();

            // When a resume has passed its check that this model is stopped and waits for the lock of the id, and a stop lands
            Thread resuming = new Thread(() -> resumeFailure.set(org.assertj.core.api.Assertions.catchThrowable(() -> resumed.set(catchup.resumeSubscription("sub")))));
            resuming.start();
            await().atMost(Duration.ofSeconds(10)).until(() -> resuming.getState() == Thread.State.WAITING
                    && Stream.of(resuming.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("underIdLock")));
            catchup.stop();
            letGoOfTheLockOfTheId.countDown();
            resuming.join(10_000);
            holding.join(10_000);

            // Then the resume returned a start that waits for the replay to run, instead of the failure that held the replay
            assertThat(resumeFailure.get()).as("thrown by the resume").isNull();
            assertThat(resumed.get().waitUntilStarted().toFuture().isDone()).as("start of the resume is done before any replay ran").isFalse();
            assertThat(wrapped.subscribeCalls).as("handed over before the start").isEmpty();

            // And it completes once a start runs the replay again and hands over
            catchup.start(true);
            StepVerifier.create(resumed.get().waitUntilStarted()).verifyComplete();
            assertThat(received).as("the whole history, replayed again").containsExactly("e1", "e2", "e3");
            assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub");
        } finally {
            letGoOfTheLockOfTheId.countDown();
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_resume_after_a_named_catch_up_that_started_at_the_beginning_is_held_and_before_the_waiters_of_the_held_replay_are_failed_gets_a_start_that_completes_once_the_replay_run_again_hands_over() {
        // Given a model that loses the live start in each of the replays, and a resume made where the hold is logged, which
        // comes after the hold and before the waiters of the held replay are told it failed
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        Sinks.Empty<Void> wrappedStarted = Sinks.empty();
        Sinks.Empty<Void> letTheReplayGo = Sinks.empty();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        AtomicBoolean asked = new AtomicBoolean();
        AtomicReference<Subscription> resumed = new AtomicReference<>();
        AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
        Logger supportLogger = (Logger) LoggerFactory.getLogger(NamedCatchupSupport.class);
        AppenderBase<ILoggingEvent> resumesWhereTheHoldIsLogged = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("held paused, and resuming it or start(true) on this model replays again from the beginning") && asked.compareAndSet(false, true)) {
                    wrapped.losesEveryToken = false;
                    wrapped.startedSignal = wrappedStarted.asMono();
                    resumeFailure.set(org.assertj.core.api.Assertions.catchThrowable(() -> resumed.set(catchup.resumeSubscription("sub"))));
                }
            }
        };
        resumesWhereTheHoldIsLogged.start();
        supportLogger.addAppender(resumesWhereTheHoldIsLogged);
        try {
            // When, with the replays let go only once the subscribe has returned the start of the replay it made
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> letTheReplayGo.asMono());
            letTheReplayGo.tryEmitEmpty();
            await().atMost(Duration.ofSeconds(20)).until(() -> resumed.get() != null || resumeFailure.get() != null);
            assertThat(resumeFailure.get()).as("thrown by the resume").isNull();
            // The start of the resume waits for the start signal of the wrapped model, held back until below, so only a
            // failure can end it before then
            Mono<String> firstStartToEnd = Mono.firstWithSignal(
                    subscription.waitUntilStarted().thenReturn("the held replay started").onErrorReturn("the held replay failed"),
                    resumed.get().waitUntilStarted().thenReturn("the resume started").onErrorReturn("the resume failed"));

            // Then the failure reaches the waiters of the held replay and not the start of the resume
            StepVerifier.create(firstStartToEnd).expectNext("the held replay failed").expectComplete().verify(Duration.ofSeconds(5));
            wrappedStarted.tryEmitEmpty();
            StepVerifier.create(resumed.get().waitUntilStarted()).expectComplete().verify(Duration.ofSeconds(5));
            assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub");
        } finally {
            supportLogger.detachAppender(resumesWhereTheHoldIsLogged);
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_resume_made_while_a_subscribe_still_runs_after_its_named_catch_up_was_held_leaves_the_start_the_subscribe_returns_failing_with_the_held_replay() {
        // Given a model that loses the live start in each of the replays, whose replay runs on the thread of the subscribe,
        // and a resume made where the hold is logged, which is before the subscribe returns
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        AtomicBoolean asked = new AtomicBoolean();
        AtomicReference<Subscription> resumed = new AtomicReference<>();
        AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
        Logger supportLogger = (Logger) LoggerFactory.getLogger(NamedCatchupSupport.class);
        AppenderBase<ILoggingEvent> resumesWhereTheHoldIsLogged = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("held paused, and resuming it or start(true) on this model replays again from the beginning") && asked.compareAndSet(false, true)) {
                    wrapped.losesEveryToken = false;
                    resumeFailure.set(org.assertj.core.api.Assertions.catchThrowable(() -> resumed.set(catchup.resumeSubscription("sub"))));
                }
            }
        };
        resumesWhereTheHoldIsLogged.start();
        supportLogger.addAppender(resumesWhereTheHoldIsLogged);
        try {
            // When
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());

            // Then the resume ran before the subscribe returned, and handed over
            assertThat(asked).as("resumed before the subscribe returned").isTrue();
            assertThat(resumeFailure.get()).as("thrown by the resume").isNull();
            StepVerifier.create(resumed.get().waitUntilStarted()).expectComplete().verify(Duration.ofSeconds(5));
            assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub");

            // And the subscribe returned the start of the replay it made, which was held
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("4 replays"))
                    .verify(Duration.ofSeconds(5));
        } finally {
            supportLogger.detachAppender(resumesWhereTheHoldIsLogged);
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_resume_made_while_a_resume_still_runs_after_its_replay_was_held_again_leaves_the_start_the_first_resume_returns_failing_with_the_held_replay() {
        // Given a subscription held after a replay that lost the live start in each run, and a model that loses it again
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        AtomicInteger holds = new AtomicInteger();
        AtomicReference<Subscription> resumedFromTheHold = new AtomicReference<>();
        AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
        Logger supportLogger = (Logger) LoggerFactory.getLogger(NamedCatchupSupport.class);
        // A resume made where the second hold is logged, which is before the first resume returns
        AppenderBase<ILoggingEvent> resumesWhereTheSecondHoldIsLogged = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("held paused, and resuming it or start(true) on this model replays again from the beginning") && holds.incrementAndGet() == 2) {
                    wrapped.losesEveryToken = false;
                    resumeFailure.set(org.assertj.core.api.Assertions.catchThrowable(() -> resumedFromTheHold.set(catchup.resumeSubscription("sub"))));
                }
            }
        };
        resumesWhereTheSecondHoldIsLogged.start();
        supportLogger.addAppender(resumesWhereTheSecondHoldIsLogged);
        try {
            catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());
            await().atMost(Duration.ofSeconds(20)).until(() -> holds.get() == 1);

            // When it is resumed and the replay is held again
            Subscription firstResume = catchup.resumeSubscription("sub");

            // Then the resume made where that hold is logged handed over
            assertThat(holds).as("holds logged").hasValue(2);
            assertThat(resumeFailure.get()).as("thrown by the resume").isNull();
            StepVerifier.create(resumedFromTheHold.get().waitUntilStarted()).expectComplete().verify(Duration.ofSeconds(5));
            assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub");

            // And the first resume returned the start of the replay it ran, which was held
            StepVerifier.create(firstResume.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("4 replays"))
                    .verify(Duration.ofSeconds(5));
        } finally {
            supportLogger.detachAppender(resumesWhereTheSecondHoldIsLogged);
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_history_loss_that_ends_a_replay_a_stop_and_a_start_replaced_holds_nothing_and_leaves_the_state_the_id_and_the_start_to_the_replay_that_runs_since() {
        // Given a first replay that reads one event, whose action stops the model and starts it again so a second replay
        // runs, and that then fails with a lost live start, which is before the subscribe has registered the first replay
        // to be disposed. The second replay reads nothing more, so it is the one in flight.
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        AtomicInteger reads = new AtomicInteger();
        AtomicReference<ReactorStreamCatchupSubscriptionModel> model = new AtomicReference<>();
        UnscopedPositionOrderedReader reader = new ThreeEventReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                if (reads.incrementAndGet() == 1) {
                    return super.readInPositionOrder(filter, range).take(1)
                            .concatWith(Flux.error(() -> HistoryLossMessages.liveStartLost("The live start was lost.", GlobalCheckpoint.of(3), true)));
                }
                return Flux.never();
            }
        };
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        model.set(catchup);
        AtomicBoolean replaced = new AtomicBoolean();
        try (ErrorsLoggedByTheCatchup errors = new ErrorsLoggedByTheCatchup()) {
            // When
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
                if (replaced.compareAndSet(false, true)) {
                    model.get().stop();
                    model.get().start(true);
                }
                return Mono.empty();
            });

            // Then the failure of the first replay holds nothing, and records nothing
            assertThat(replaced).as("the first replay was replaced").isTrue();
            assertThat(reads.get()).as("reads, which a second replay made").isGreaterThan(1);
            assertThat(errors.errors()).as("errors logged").noneMatch(message -> message.contains("held paused"));
            assertThat(catchup.isPaused("sub")).as("paused").isFalse();
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();

            // And the id is still taken, since the second replay owns it
            Throwable subscribeAgain = org.assertj.core.api.Assertions.catchThrowable(() -> catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty()));
            assertThat(subscribeAgain).as("what a subscribe of the same id throws").isInstanceOf(DuplicateSubscriptionIdException.class);

            // And the start the subscribe returned is not failed while the second replay is in flight
            StepVerifier.create(subscription.waitUntilStarted()).expectSubscription().expectNoEvent(Duration.ofMillis(100)).thenCancel().verify(Duration.ofSeconds(5));
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_resume_that_read_no_loss_before_it_waited_for_the_lock_of_the_id_while_a_replay_from_above_the_beginning_recorded_its_loss_refuses_instead_of_returning_the_failed_start() throws InterruptedException {
        // Given a replay in flight from above the beginning, whose live start the model has when it starts, and loses by the end
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.answersTrueToTheFirstProbes = 1;
        ReleasablePositionOrderedReader reader = new ReleasablePositionOrderedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        Checkpoint stored = GlobalCheckpoint.of(1, new StringBasedCheckpoint("stored live start"), 1, 3);
        // A second subscribe of the id asks the wrapped model whether it holds the id while it holds the lock of the id, so
        // it keeps the hold of the loss and the resume waiting for that lock until the test lets it go
        CountDownLatch holdsTheLockOfTheId = new CountDownLatch(1);
        CountDownLatch letGoOfTheLockOfTheId = new CountDownLatch(1);
        AtomicReference<Thread> holder = new AtomicReference<>();
        AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
        AtomicReference<Subscription> resumed = new AtomicReference<>();
        try {
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(stored), cloudEvent -> Mono.empty());
            wrapped.whenAskedWhetherRunning = () -> {
                boolean underTheLockOfTheId = StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("underIdLock")));
                if (Thread.currentThread() == holder.get() && underTheLockOfTheId) {
                    holdsTheLockOfTheId.countDown();
                    try {
                        letGoOfTheLockOfTheId.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            Thread holding = new Thread(() -> org.assertj.core.api.Assertions.catchThrowable(() -> catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(stored), cloudEvent -> Mono.empty())));
            holder.set(holding);
            holding.start();
            assertThat(holdsTheLockOfTheId.await(10, TimeUnit.SECONDS)).as("the lock of the id is held").isTrue();

            // When the replay loses its live start and waits for the lock of the id to record it
            // The release runs the rest of the replay on the thread that calls it
            Thread releasing = new Thread(reader::release);
            releasing.start();
            await().atMost(Duration.ofSeconds(10)).until(() -> Thread.getAllStackTraces().entrySet().stream()
                    .anyMatch(entry -> entry.getKey().getState() == Thread.State.WAITING
                            && Stream.of(entry.getValue()).anyMatch(frame -> frame.getMethodName().equals("holdForLostLiveStart"))
                            && Stream.of(entry.getValue()).anyMatch(frame -> frame.getMethodName().equals("underIdLock"))));
            // And a resume that has read no loss queues for the lock after it, which hands the lock on in the order of the queue
            Thread resuming = new Thread(() -> resumeFailure.set(org.assertj.core.api.Assertions.catchThrowable(() -> resumed.set(catchup.resumeSubscription("sub")))));
            resuming.start();
            await().atMost(Duration.ofSeconds(10)).until(() -> resuming.getState() == Thread.State.WAITING
                    && Stream.of(resuming.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("underIdLock")));
            letGoOfTheLockOfTheId.countDown();
            resuming.join(10_000);
            holding.join(10_000);
            releasing.join(10_000);

            // Then the resume refuses with the loss, instead of returning the start that failed with it
            assertThat(resumeFailure.get()).as("thrown by the resume").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("stored live start");
            assertThat(resumed.get()).as("returned by the resume").isNull();
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class))
                    .verify(Duration.ofSeconds(10));
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();
        } finally {
            letGoOfTheLockOfTheId.countDown();
            reader.release();
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_named_catch_up_that_started_above_the_beginning_and_lost_its_live_start_is_held_paused_and_recorded_so_a_resume_refuses_and_a_start_replays_nothing_until_it_is_cancelled() {
        // Given a stored position above 0 whose live start the model still has when the replay starts, and loses during it
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.answersTrueToTheFirstProbes = 1;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        Checkpoint stored = GlobalCheckpoint.of(1, new StringBasedCheckpoint("stored live start"), 1, 3);
        try {
            // When
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(stored), cloudEvent -> Mono.fromRunnable(() -> received.add(cloudEvent.getId())));
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("again from position 1"))
                    .verify(Duration.ofSeconds(20));

            // Then it is held paused
            assertThat(received).as("the events after the stored position").containsExactly("e2", "e3");
            assertThat(catchup.isPaused("sub")).as("paused").isTrue();
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();

            // When it is resumed or started, although the model has the live start again
            wrapped.answersTrueToTheFirstProbes = Integer.MAX_VALUE;
            received.clear();
            Throwable resume = org.assertj.core.api.Assertions.catchThrowable(() -> catchup.resumeSubscription("sub"));
            Throwable start = org.assertj.core.api.Assertions.catchThrowable(() -> catchup.start(true));

            // Then the resume refuses, and nothing replays
            assertThat(resume).as("what the resume throws").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("sub");
            assertThat(start).as("what start(true) throws").isNull();
            assertThat(received).as("events delivered by a replay again").isEmpty();
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();
            assertThat(catchup.isPaused("sub")).as("paused").isTrue();

            // When it is cancelled
            catchup.cancelSubscription("sub").block(Duration.ofSeconds(10));
            Subscription again = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> Mono.empty());

            // Then the id subscribes again and hands over
            StepVerifier.create(again.waitUntilStarted()).verifyComplete();
            assertThat(wrapped.subscribeCalls).as("handed over").containsExactly("sub");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void refuses_a_resume_of_a_named_catch_up_whose_replay_from_above_the_beginning_lost_its_live_start_without_naming_that_live_start_as_where_it_stopped() {
        // Given
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.answersTrueToTheFirstProbes = 1;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        Checkpoint liveStart = new StringBasedCheckpoint("stored live start");
        try {
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(1, liveStart, 1, 3)), cloudEvent -> Mono.empty());
            StepVerifier.create(subscription.waitUntilStarted()).expectError(CatchupCannotResumeException.class).verify(Duration.ofSeconds(20));
            wrapped.answersTrueToTheFirstProbes = Integer.MAX_VALUE;

            // When
            Throwable resume = org.assertj.core.api.Assertions.catchThrowable(() -> catchup.resumeSubscription("sub"));

            // Then
            assertThat(resume).as("what the resume throws").isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessage(HistoryLossMessages.liveStartLostOnResume("sub", liveStart).getMessage())
                    .hasMessageNotContaining("where it stopped");
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_named_start_after_a_bare_position_above_0_is_refused_with_the_advice_to_cancel_first_and_hands_nothing_over() {
        // Given
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new UnusedPositionOrderedReader());
        try {
            // When
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(GlobalCheckpoint.of(1)), cloudEvent -> Mono.empty());

            // Then
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable)
                            .isInstanceOf(CatchupCannotResumeException.class)
                            .hasMessageContaining("catch-up subscription sub after position 1")
                            .hasMessageContaining("cancel it first, which deletes that checkpoint")
                            .hasMessageContaining("EventMetadata.getCheckpoint()"))
                    .verify(Duration.ofSeconds(10));
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();
        } finally {
            catchup.shutdown();
        }
    }

    @Test
    void a_named_start_from_a_stored_live_start_the_model_no_longer_has_above_the_beginning_is_refused_with_the_advice_to_cancel_first() {
        // Given a stored checkpoint whose replay began above the beginning, and a model that has lost its live start
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel();
        wrapped.losesEveryToken = true;
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ThreeEventReader());
        Checkpoint stored = GlobalCheckpoint.of(3, new StringBasedCheckpoint("aged out live start"), 1, 3);
        try {
            // When
            Subscription subscription = catchup.subscribe("sub", StreamSubscriptionFilter.filter(Filter.all()),
                    StartAt.checkpoint(stored), cloudEvent -> Mono.empty());

            // Then
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorSatisfies(throwable -> assertThat(throwable)
                            .isInstanceOf(CatchupCannotResumeException.class)
                            .hasMessageContaining("catch-up subscription sub again from position 1")
                            .hasMessageContaining("cancel it first, which deletes that checkpoint")
                            .hasMessageContaining("from the beginning again"))
                    .verify(Duration.ofSeconds(10));
            assertThat(wrapped.subscribeCalls).as("handed over").isEmpty();
        } finally {
            catchup.shutdown();
        }
    }

    // Three events at positions 1 to 3, and a head that stays at 3
    private static class ThreeEventReader implements UnscopedPositionOrderedReader {
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
            return Mono.just(3L);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // One event already there and one more written while the history is being read, so the catch-up has a history to
    // read and something to deliver afterwards. The head grows on the second read, which is what the reconciliation
    // sees.
    private static final class GrowingPositionOrderedReader implements UnscopedPositionOrderedReader {
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
        // No position, as globalCheckpoint() answers
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.empty();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.empty();
        }
    }

    private static final class UnusedPositionOrderedReader implements UnscopedPositionOrderedReader {
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
    private static final class StuckPositionOrderedReader implements UnscopedPositionOrderedReader {
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
    private static final class ReleasablePositionOrderedReader implements UnscopedPositionOrderedReader {
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
        final List<Checkpoint> resumeProbes = new CopyOnWriteArrayList<>();
        volatile Runnable whenAskedWhetherRunning = () -> {
        };
        volatile Mono<Void> startedSignal = Mono.empty();
        volatile boolean losesEveryToken = false;
        // canResumeFrom answers true to this many probes and false to every one after them
        volatile int answersTrueToTheFirstProbes = Integer.MAX_VALUE;

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("token"));
        }

        @Override
        public Mono<Boolean> canResumeFrom(Checkpoint checkpoint) {
            return Mono.fromSupplier(() -> {
                resumeProbes.add(checkpoint);
                return !losesEveryToken && resumeProbes.size() <= answersTrueToTheFirstProbes;
            });
        }

        @Override
        // The position never moves, so it is the one at the call
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
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
    private static final class BulkEmptyThenOneReconciledEventReader implements UnscopedPositionOrderedReader {
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
        // The position never moves, so it is the one at the call
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return globalCheckpoint();
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
