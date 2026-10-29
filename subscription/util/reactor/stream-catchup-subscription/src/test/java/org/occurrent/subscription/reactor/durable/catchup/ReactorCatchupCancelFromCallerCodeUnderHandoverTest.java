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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wrapped model runs the caller's code while it pauses a subscription, since cancelling the action it runs
 * does. The catch-up model pauses the subscription a replay hands over when a pause was asked for during that replay,
 * and it does so on the replay's thread. A cancel that this caller code makes through the catch-up model, while
 * another thread cancels the same subscription, must not wait for that other thread forever.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupCancelFromCallerCodeUnderHandoverTest {

    private static final String SUBSCRIPTION_ID = "sub";
    private static final Duration DEADLINE = Duration.ofSeconds(5);

    @Test
    void a_cancel_from_the_wrapped_models_pause_during_a_handover_returns_while_another_thread_cancels_the_same_subscription() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        AtomicReference<Thread> otherCancelThread = new AtomicReference<>();
        CompletableFuture<Void> otherCancelReturned = new CompletableFuture<>();
        AtomicReference<@Nullable Throwable> otherCancelFailure = new AtomicReference<>();
        AtomicReference<@Nullable Throwable> ownCancelFailure = new AtomicReference<>();
        AtomicReference<@Nullable Throwable> hookFailure = new AtomicReference<>();
        wrapped.whilePausing = () -> {
            try {
                Thread other = daemon("cancel-on-another-thread", () -> {
                    try {
                        catchup.cancelSubscription(SUBSCRIPTION_ID).block(DEADLINE);
                    } catch (Throwable t) {
                        otherCancelFailure.set(t);
                    } finally {
                        otherCancelReturned.complete(null);
                    }
                });
                otherCancelThread.set(other);
                other.start();
                awaitBlocked(other);
                try {
                    catchup.cancelSubscription(SUBSCRIPTION_ID).block(DEADLINE);
                } catch (Throwable t) {
                    ownCancelFailure.set(t);
                }
            } catch (Throwable t) {
                hookFailure.set(t);
            }
        };
        catchup.subscribe(SUBSCRIPTION_ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> Mono.empty());
        catchup.pauseSubscription(SUBSCRIPTION_ID);

        // The release runs the rest of the replay on this thread, which is not the one that subscribed. The replay
        // is asynchronous, so the refusal a launch on the subscribing thread sets doesn't cover its handover.
        CompletableFuture<Void> replayReturned = new CompletableFuture<>();
        Thread replay = daemon("replay", () -> {
            try {
                reader.release();
            } finally {
                replayReturned.complete(null);
            }
        });
        replay.start();

        awaitBoth(replayReturned, otherCancelReturned, replay, otherCancelThread);
        assertThat(hookFailure.get()).as("failure in the pause of the wrapped model").isNull();
        assertThat(wrapped.pauseCalls).as("pauses of the wrapped model").containsExactly(SUBSCRIPTION_ID);
        assertThat(ownCancelFailure.get()).as("cancel made by the pause of the wrapped model")
                .satisfiesAnyOf(failure -> assertThat(failure).isNull(), failure -> assertThat(failure).isInstanceOf(IllegalStateException.class));
        assertThat(otherCancelFailure.get()).as("cancel made by the other thread")
                .satisfiesAnyOf(failure -> assertThat(failure).isNull(), failure -> assertThat(failure).isInstanceOf(IllegalStateException.class));
    }

    private static Thread daemon(String name, Runnable work) {
        Thread thread = new Thread(work, name);
        thread.setDaemon(true);
        return thread;
    }

    private static void awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + DEADLINE.toNanos();
        while (thread.getState() != Thread.State.BLOCKED) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the thread never waited for a monitor, its state is " + thread.getState());
            }
            Thread.onSpinWait();
        }
    }

    // A thread that never returns is a deadlock, which must fail here rather than hang the build
    private static void awaitBoth(CompletableFuture<Void> replayReturned, CompletableFuture<Void> otherCancelReturned, Thread replay, AtomicReference<Thread> otherCancelThread) {
        try {
            CompletableFuture.allOf(replayReturned, otherCancelReturned).get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("A cancel did not return within " + DEADLINE.toSeconds() + " seconds. The replay thread (cancel from the pause of the wrapped model) returned: "
                    + replayReturned.isDone() + ", " + describe(replay) + ". The other thread's cancel returned: " + otherCancelReturned.isDone() + ", " + describe(otherCancelThread.get()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    private static String describe(@Nullable Thread thread) {
        if (thread == null) {
            return "no thread";
        }
        return "thread " + thread.getName() + " is " + thread.getState() + " at " + Arrays.stream(thread.getStackTrace()).limit(4).toList();
    }

    // Three events at positions 1 to 3. Every read waits until release(), which runs the rest of the replay on its caller
    private static final class GatedReader implements UnscopedPositionOrderedReader {
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

    // A named subscription model with a resolvable checkpoint, whose pause runs whilePausing before it pauses, as the
    // caller's cancel hook runs when a reactor model closes the run it pauses
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final List<String> pauseCalls = new CopyOnWriteArrayList<>();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        volatile Runnable whilePausing = () -> {
        };

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(TOKEN);
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(TOKEN);
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.error(new AssertionError("The cold primitive must not be used by the named catch-up path"));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
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
            paused.addAll(subscribeCalls);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
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
            paused.remove(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pauseCalls.add(subscriptionId);
            whilePausing.run();
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
