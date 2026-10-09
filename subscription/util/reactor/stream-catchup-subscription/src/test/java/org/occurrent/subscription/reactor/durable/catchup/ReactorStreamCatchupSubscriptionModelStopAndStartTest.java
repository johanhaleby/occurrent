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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A subscription whose replay {@code stop()} interrupted, or that was made while the model was stopped, is paused
 * until {@code start(true)} or {@code resumeSubscription(id)} runs its replay again from where it started.
 * {@code start(false)} keeps it paused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupSubscriptionModelStopAndStartTest {

    private static final String SUBSCRIPTION_ID = "sub";

    @Test
    void start_false_keeps_a_replay_that_was_running_at_stop_paused_until_it_is_resumed() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = subscribe(catchup, delivered);

        catchup.stop();
        catchup.start(false);
        reader.release();

        assertThat(delivered).as("events delivered after start(false) without resumeSubscription").isEmpty();
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("isPaused(sub) after stop then start(false)").isTrue();
        assertThat(catchup.isRunning(SUBSCRIPTION_ID)).as("isRunning(sub) after stop then start(false)").isFalse();
        assertThat(wrapped.subscribeCalls).isEmpty();

        catchup.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delivered).containsExactly("e1", "e2", "e3");
        assertThat(wrapped.subscribeCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(outcomeOf(subscription)).isEqualTo("complete");
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void start_false_keeps_a_subscription_made_while_the_model_was_stopped_paused_until_it_is_resumed() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        reader.release();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> delivered = new CopyOnWriteArrayList<>();
        catchup.stop();
        Subscription subscription = subscribe(catchup, delivered);

        catchup.start(false);

        assertThat(delivered).isEmpty();
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(outcomeOf(subscription)).isEqualTo("waiting");

        catchup.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delivered).containsExactly("e1", "e2", "e3");
        assertThat(wrapped.subscribeCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(outcomeOf(subscription)).isEqualTo("complete");
    }

    @Test
    void start_true_runs_a_replay_that_stop_interrupted_again_from_where_it_started_and_undoes_a_pause() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = subscribe(catchup, delivered);
        catchup.pauseSubscription(SUBSCRIPTION_ID);

        catchup.stop();
        catchup.start(true);
        reader.release();

        assertThat(delivered).containsExactly("e1", "e2", "e3");
        assertThat(wrapped.subscribeCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).isFalse();
        assertThat(catchup.isRunning(SUBSCRIPTION_ID)).isTrue();
        assertThat(outcomeOf(subscription)).isEqualTo("complete");
    }

    @Test
    void resuming_a_paused_replay_while_the_model_is_stopped_starts_the_model_without_resuming_the_others() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        reader.release();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> delivered = new CopyOnWriteArrayList<>();
        List<String> deliveredToOther = new CopyOnWriteArrayList<>();
        catchup.stop();
        subscribe(catchup, delivered);
        catchup.subscribe("other", StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> deliveredToOther.add(cloudEvent.getId())));

        catchup.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(wrapped.startCalls).containsExactly(false);
        assertThat(delivered).containsExactly("e1", "e2", "e3");
        assertThat(deliveredToOther).isEmpty();
        assertThat(catchup.isPaused("other")).isTrue();
    }

    @Test
    void pausing_a_replay_that_stop_interrupted_throws_since_it_is_already_paused() {
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(new WrappedModel(), new GatedReader());
        subscribe(catchup, new CopyOnWriteArrayList<>());
        catchup.stop();
        catchup.start(false);

        assertThatThrownBy(() -> catchup.pauseSubscription(SUBSCRIPTION_ID))
                .isExactlyInstanceOf(SubscriptionNotRunningException.class);
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_cancelled_replay_that_stop_interrupted_is_not_run_again() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = subscribe(catchup, delivered);
        catchup.stop();

        catchup.cancelSubscription(SUBSCRIPTION_ID);
        catchup.start(true);
        reader.release();

        assertThat(delivered).isEmpty();
        assertThat(wrapped.subscribeCalls).isEmpty();
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).isFalse();
        assertThat(outcomeOf(subscription)).isEqualTo("error");
        assertThatThrownBy(() -> catchup.resumeSubscription(SUBSCRIPTION_ID)).isExactlyInstanceOf(UnknownSubscriptionException.class);
    }

    // The history read ends before the reconciliation read, so a stop from the listener comes after every replayed
    // event was delivered and before the handover.
    @Test
    void a_stop_after_the_history_is_read_keeps_the_subscription_paused_through_start_false() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        reader.release();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        AtomicReference<Boolean> stopOnHistoryRead = new AtomicReference<>(true);
        catchup.listenForCatchup(SUBSCRIPTION_ID, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
            }

            @Override
            public void historyRead(Object episode) {
                if (stopOnHistoryRead.getAndSet(false)) {
                    catchup.stop();
                }
            }
        });
        List<String> delivered = new CopyOnWriteArrayList<>();

        Subscription subscription = subscribe(catchup, delivered);
        catchup.start(false);

        assertThat(delivered).containsExactly("e1", "e2", "e3");
        assertThat(wrapped.subscribeCalls).isEmpty();
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(outcomeOf(subscription)).isEqualTo("waiting");

        catchup.resumeSubscription(SUBSCRIPTION_ID);

        // Run again from where it started, so the replayed events are delivered a second time
        assertThat(delivered).containsExactly("e1", "e2", "e3", "e1", "e2", "e3");
        assertThat(wrapped.subscribeCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(outcomeOf(subscription)).isEqualTo("complete");
    }

    // The wrapped model's subscribe(..) runs while the handover holds the subscription's lock, and it returns only once
    // a stop on another thread is waiting for that lock. The handover went first, so the wrapped model owns the
    // subscription.
    @Test
    void a_stop_that_waits_for_the_handover_leaves_the_subscription_to_the_wrapped_model() throws InterruptedException {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> delivered = new CopyOnWriteArrayList<>();
        Thread stopping = new Thread(catchup::stop);
        wrapped.whileSubscribing = () -> {
            stopping.start();
            awaitBlocked(stopping);
        };
        Subscription subscription = subscribe(catchup, delivered);

        reader.release();
        stopping.join(5_000);
        catchup.start(false);

        assertThat(stopping.isAlive()).isFalse();
        assertThat(wrapped.subscribeCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(wrapped.stopCalls).hasSize(1);
        assertThat(delivered).containsExactly("e1", "e2", "e3");
        assertThat(catchup.isCatchingUp(SUBSCRIPTION_ID)).isFalse();
        assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("answered by the wrapped model, which stop() paused").isTrue();
        assertThat(outcomeOf(subscription)).isEqualTo("complete");
    }

    /**
     * Whatever sequence of calls ran before the replay hands over, the subscription is paused exactly when a pause or
     * a stop came after the last {@code start(true)} or resume. It is running exactly when neither did and the model
     * is started. Its replay delivers only when it was not cancelled and no stop came after the last
     * {@code start(true)} or resume, and a replay run again reads from where the subscription started, so nothing is
     * skipped.
     */
    @Test
    void every_sequence_of_life_cycle_calls_pauses_and_runs_the_replay_as_the_contract_says() {
        List<List<Call>> sequences = new ArrayList<>();
        sequences.add(List.of());
        for (int length = 1; length <= 4; length++) {
            List<List<Call>> longer = new ArrayList<>();
            for (List<Call> shorter : sequences) {
                if (shorter.size() == length - 1 && !shorter.contains(Call.CANCEL)) {
                    for (Call call : Call.values()) {
                        List<Call> sequence = new ArrayList<>(shorter);
                        sequence.add(call);
                        longer.add(sequence);
                    }
                }
            }
            sequences.addAll(longer);
        }

        int checked = 0;
        for (boolean subscribedWhileStopped : new boolean[]{false, true}) {
            for (List<Call> sequence : sequences) {
                check(subscribedWhileStopped, sequence);
                checked++;
            }
        }
        assertThat(checked).isEqualTo(2 * (1 + 6 + 5 * 6 + 25 * 6 + 125 * 6));
    }

    private static void check(boolean subscribedWhileStopped, List<Call> sequence) {
        String description = (subscribedWhileStopped ? "stop, subscribe" : "subscribe") + ", " + sequence;
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        Expected expected = new Expected();
        if (subscribedWhileStopped) {
            catchup.stop();
            expected.stop();
        }
        List<String> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = subscribe(catchup, delivered);

        for (Call call : sequence) {
            switch (call) {
                case PAUSE -> {
                    if (expected.parked || expected.pausePending) {
                        assertThatThrownBy(() -> catchup.pauseSubscription(SUBSCRIPTION_ID)).as(description).isExactlyInstanceOf(SubscriptionNotRunningException.class);
                    } else {
                        catchup.pauseSubscription(SUBSCRIPTION_ID);
                        expected.pausePending = true;
                    }
                }
                case RESUME -> {
                    if (expected.parked || expected.pausePending) {
                        catchup.resumeSubscription(SUBSCRIPTION_ID);
                        expected.resume();
                    } else {
                        assertThatThrownBy(() -> catchup.resumeSubscription(SUBSCRIPTION_ID)).as(description).isExactlyInstanceOf(SubscriptionAlreadyRunningException.class);
                    }
                }
                case STOP -> {
                    catchup.stop();
                    expected.stop();
                }
                case START_RESUMING -> {
                    catchup.start(true);
                    expected.startResuming();
                }
                case START_WITHOUT_RESUMING -> {
                    catchup.start(false);
                    expected.stopped = false;
                }
                case CANCEL -> {
                    catchup.cancelSubscription(SUBSCRIPTION_ID);
                    expected.cancelled = true;
                }
            }
            assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("isPaused after " + description + ", at " + call).isEqualTo(expected.paused());
            assertThat(catchup.isRunning(SUBSCRIPTION_ID)).as("isRunning after " + description + ", at " + call).isEqualTo(expected.running());
        }

        reader.release();

        if (expected.cancelled || expected.parked) {
            assertThat(delivered).as("delivered after " + description).isEmpty();
            assertThat(wrapped.subscribeCalls).as("handovers after " + description).isEmpty();
            assertThat(outcomeOf(subscription)).as("started signal after " + description).isEqualTo(expected.cancelled ? "error" : "waiting");
        } else {
            assertThat(delivered).as("delivered after " + description).containsExactly("e1", "e2", "e3");
            assertThat(wrapped.subscribeCalls).as("handovers after " + description).containsExactly(SUBSCRIPTION_ID);
            assertThat(outcomeOf(subscription)).as("started signal after " + description).isEqualTo("complete");
            assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("isPaused once handed over after " + description).isEqualTo(expected.pausePending);
        }
    }

    private enum Call {
        PAUSE, RESUME, STOP, START_RESUMING, START_WITHOUT_RESUMING, CANCEL
    }

    // What the life-cycle contract says about one subscription whose replay has not handed over
    private static final class Expected {
        boolean stopped = false;
        // Waiting for start(true) or a resume to run its replay
        boolean parked = false;
        // Paused during the replay, which hands over paused
        boolean pausePending = false;
        boolean cancelled = false;

        void stop() {
            stopped = true;
            parked = true;
        }

        void startResuming() {
            stopped = false;
            parked = false;
            pausePending = false;
        }

        // Resuming while stopped starts the model first, without resuming anything else
        void resume() {
            stopped = false;
            parked = false;
            pausePending = false;
        }

        boolean paused() {
            return !cancelled && (parked || pausePending);
        }

        boolean running() {
            return !cancelled && !stopped && !parked && !pausePending;
        }
    }

    private static Subscription subscribe(ReactorStreamCatchupSubscriptionModel catchup, List<String> delivered) {
        return catchup.subscribe(SUBSCRIPTION_ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));
    }

    // Every step here runs on the calling thread, so the outcome is known once the call that causes it returns
    private static String outcomeOf(Subscription subscription) {
        AtomicReference<String> outcome = new AtomicReference<>("waiting");
        subscription.waitUntilStarted().subscribe(unused -> {
        }, error -> outcome.set("error"), () -> outcome.set("complete"));
        return outcome.get();
    }

    private static void awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (thread.getState() != Thread.State.BLOCKED) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the stop never waited for the handover, its thread is " + thread.getState());
            }
            Thread.onSpinWait();
        }
    }

    // Three events at positions 1 to 3. Every read waits until release(), and a read started after that returns at once.
    private static final class GatedReader implements PositionOrderedReader {
        private final Sinks.Empty<Void> released = Sinks.empty();

        void release() {
            released.tryEmitEmpty();
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = Math.min(range.upToPosition().orElse(3L), 3L);
            return released.asMono().thenMany(Flux.fromStream(LongStream.rangeClosed(from, to).boxed()
                    .map(position -> CloudEventBuilder.v1()
                            .withId("e" + position)
                            .withSource(URI.create("urn:test"))
                            .withType("type")
                            .build())));
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

    // A named subscription model with a resolvable checkpoint. stop() pauses what it runs and start(true) resumes it, as
    // the life-cycle contract says.
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final List<Boolean> startCalls = new CopyOnWriteArrayList<>();
        final List<String> stopCalls = new CopyOnWriteArrayList<>();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        volatile Runnable whileSubscribing = () -> {
        };

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
            whileSubscribing.run();
            subscribeCalls.add(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            subscribeCalls.remove(subscriptionId);
            paused.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public void stop() {
            stopCalls.add("stop");
            paused.addAll(subscribeCalls);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            startCalls.add(resumeSubscriptionsAutomatically);
            if (resumeSubscriptionsAutomatically) {
                paused.clear();
            }
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscribeCalls.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (!subscribeCalls.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.remove(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            if (!subscribeCalls.contains(subscriptionId)) {
                throw new UnknownSubscriptionException(subscriptionId);
            }
            paused.add(subscriptionId);
        }

        private static Subscription handle(String subscriptionId) {
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
