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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wrapped model runs the caller's code while it pauses a subscription, since pausing cancels the Mono of the action
 * it runs. The catch-up model pauses the subscription a replay hands over when a pause was asked for during that
 * replay, and it does so on the replay's thread holding the subscription's state monitor. A cancel from that code,
 * while another thread cancels the same subscription, would wait for that other thread forever, so the live action
 * holds the event the wrapped model delivers until the pause has returned.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupCancelFromCallerCodeUnderHandoverTest {

    private static final String SUBSCRIPTION_ID = "sub";
    private static final String LIVE_EVENT_ID = "live";
    private static final Duration DEADLINE = Duration.ofSeconds(5);

    @Test
    void the_pause_of_a_handover_cancels_none_of_the_callers_code_and_a_resume_delivers_the_event_it_held() {
        WrappedModel wrapped = new WrappedModel();
        GatedReader reader = new GatedReader();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, reader);
        List<String> received = new CopyOnWriteArrayList<>();
        AtomicBoolean hookRanWhilePausing = new AtomicBoolean();
        List<Throwable> hookFailures = new CopyOnWriteArrayList<>();
        Runnable cancelFromThisAndAnotherThread = () -> {
            if (wrapped.pausing.get()) {
                hookRanWhilePausing.set(true);
            }
            CompletableFuture<Void> otherCancelReturned = new CompletableFuture<>();
            daemon("cancel-on-another-thread", () -> {
                try {
                    catchup.cancelSubscription(SUBSCRIPTION_ID).block(DEADLINE);
                    otherCancelReturned.complete(null);
                } catch (Throwable t) {
                    otherCancelReturned.completeExceptionally(t);
                }
            }).start();
            try {
                catchup.cancelSubscription(SUBSCRIPTION_ID).block(DEADLINE);
                otherCancelReturned.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                hookFailures.add(t);
            }
        };
        catchup.subscribe(SUBSCRIPTION_ID, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                cloudEvent -> {
                    received.add(cloudEvent.getId());
                    return LIVE_EVENT_ID.equals(cloudEvent.getId()) ? Mono.<Void>never().doOnCancel(cancelFromThisAndAnotherThread) : Mono.empty();
                });
        catchup.pauseSubscription(SUBSCRIPTION_ID);

        // The release runs the rest of the replay, its handover and the pause of the wrapped model on this thread
        CompletableFuture<Void> replayReturned = new CompletableFuture<>();
        Thread replay = daemon("replay", () -> {
            reader.release();
            replayReturned.complete(null);
        });
        replay.start();
        await(replayReturned, "The replay that hands over and pauses", replay);

        assertThat(wrapped.pauseCalls).as("pauses of the wrapped model").containsExactly(SUBSCRIPTION_ID);
        assertThat(hookRanWhilePausing).as("caller code the pause of the wrapped model ran").isFalse();
        assertThat(received).as("events the action was given before the resume").containsExactly("e1", "e2", "e3");

        catchup.resumeSubscription(SUBSCRIPTION_ID);
        awaitReceived(received, LIVE_EVENT_ID);

        CompletableFuture<Void> cancelReturned = new CompletableFuture<>();
        Thread cancelling = daemon("cancel", () -> {
            catchup.cancelSubscription(SUBSCRIPTION_ID).block(DEADLINE);
            cancelReturned.complete(null);
        });
        cancelling.start();
        await(cancelReturned, "The cancel whose wrapped cancel runs the cancel hook", cancelling);
        assertThat(hookFailures).as("failures of the cancels the hook made").isEmpty();
        assertThat(received).as("events the action was given").containsExactly("e1", "e2", "e3", LIVE_EVENT_ID);
    }

    private static Thread daemon(String name, Runnable work) {
        Thread thread = new Thread(work, name);
        thread.setDaemon(true);
        return thread;
    }

    // A thread that never returns is a deadlock, which must fail here rather than hang the build
    private static void await(CompletableFuture<Void> returned, String what, Thread thread) {
        try {
            returned.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(what + " did not return within " + DEADLINE.toSeconds() + " seconds, " + describe(thread));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    private static void awaitReceived(List<String> received, String eventId) {
        long deadline = System.nanoTime() + DEADLINE.toNanos();
        while (!received.contains(eventId)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("The action was never given " + eventId + ", it was given " + received);
            }
            Thread.onSpinWait();
        }
    }

    private static String describe(Thread thread) {
        return "thread " + thread.getName() + " is " + thread.getState() + " at " + Arrays.stream(thread.getStackTrace()).limit(6).toList();
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
                    .map(position -> event("e" + position))));
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

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("type")
                .build();
    }

    // A named subscription model that, like ReactorMongoSubscriptionModel, gives a subscription's action an event on a
    // thread of its own, and pauses or cancels the subscription by cancelling the Mono that action returned. Its
    // subscribe and resume return once that thread has the Mono, so the pause that follows always finds it.
    private static final class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
        final List<String> pauseCalls = new CopyOnWriteArrayList<>();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        final AtomicBoolean pausing = new AtomicBoolean();
        private final Map<String, Function<CloudEvent, Mono<Void>>> actions = new ConcurrentHashMap<>();
        private final Map<String, Disposable> running = new ConcurrentHashMap<>();

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
            actions.put(subscriptionId, action);
            deliverLiveEvent(subscriptionId);
            return handle(subscriptionId);
        }

        private void deliverLiveEvent(String subscriptionId) {
            CountDownLatch delivered = new CountDownLatch(1);
            daemon("delivery", () -> {
                running.put(subscriptionId, actions.get(subscriptionId).apply(OccurrentCloudEventExtension.withPosition(event(LIVE_EVENT_ID), 4)).subscribe());
                delivered.countDown();
            }).start();
            try {
                if (!delivered.await(DEADLINE.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new AssertionError("The delivery thread never gave the action the live event");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            subscribeCalls.remove(subscriptionId);
            paused.remove(subscriptionId);
            cancelRunning(subscriptionId);
            return Mono.empty();
        }

        private void cancelRunning(String subscriptionId) {
            Disposable inFlight = running.remove(subscriptionId);
            if (inFlight != null) {
                inFlight.dispose();
            }
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
            deliverLiveEvent(subscriptionId);
            return handle(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            pauseCalls.add(subscriptionId);
            pausing.set(true);
            try {
                cancelRunning(subscriptionId);
            } finally {
                pausing.set(false);
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
