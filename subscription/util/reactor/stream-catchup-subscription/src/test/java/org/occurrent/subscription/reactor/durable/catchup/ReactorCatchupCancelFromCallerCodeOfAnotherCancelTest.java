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

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
 * Cancelling a subscription whose replay is in flight runs the caller's code, since disposing the replay cancels the
 * Mono its action returned and failing the start of the subscription runs its waiters. When that code cancels another
 * subscription through the catch-up model, while another thread cancels that subscription and runs code that cancels
 * the first one, neither cancel may wait for the other forever.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupCancelFromCallerCodeOfAnotherCancelTest {

    private static final Duration DEADLINE = Duration.ofSeconds(5);

    @Test
    void two_cancels_return_when_each_cancels_the_other_subscription_from_the_cancel_hook_of_its_action() {
        assertBothCancelsReturn((catchup, id, cancelOther) ->
                catchup.subscribe(id, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                        cloudEvent -> Mono.<Void>never().doOnCancel(cancelOther::run)));
    }

    @Test
    void two_cancels_return_when_each_cancels_the_other_subscription_from_a_waiter_of_its_start() {
        assertBothCancelsReturn((catchup, id, cancelOther) ->
                catchup.subscribe(id, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(GlobalCheckpoint.of(0)),
                                cloudEvent -> Mono.never())
                        .waitUntilStarted()
                        .subscribe(unused -> {
                        }, failure -> cancelOther.run()));
    }

    private interface SubscribingWithCallerCode {
        void subscribe(ReactorStreamCatchupSubscriptionModel catchup, String id, Runnable cancelOther);
    }

    private static void assertBothCancelsReturn(SubscribingWithCallerCode subscribing) {
        StubWrappedModel wrapped = new StubWrappedModel();
        ReactorStreamCatchupSubscriptionModel catchup = new ReactorStreamCatchupSubscriptionModel(wrapped, new ImmediateReader());
        CountDownLatch bothInCallerCode = new CountDownLatch(2);
        List<Throwable> callerCodeFailures = new CopyOnWriteArrayList<>();
        for (String[] ids : new String[][]{{"x", "y"}, {"y", "x"}}) {
            String other = ids[1];
            AtomicBoolean ran = new AtomicBoolean();
            subscribing.subscribe(catchup, ids[0], () -> {
                if (!ran.compareAndSet(false, true)) {
                    return;
                }
                try {
                    bothInCallerCode.countDown();
                    if (!bothInCallerCode.await(DEADLINE.toMillis(), TimeUnit.MILLISECONDS)) {
                        throw new AssertionError("The cancel of " + other + " never ran its caller code");
                    }
                    catchup.cancelSubscription(other).block(DEADLINE);
                } catch (Throwable t) {
                    callerCodeFailures.add(t);
                }
            });
        }

        CompletableFuture<Void> cancelOfX = new CompletableFuture<>();
        CompletableFuture<Void> cancelOfY = new CompletableFuture<>();
        Thread cancellingX = daemon("cancel-x", cancelOfX, () -> catchup.cancelSubscription("x").block(DEADLINE));
        Thread cancellingY = daemon("cancel-y", cancelOfY, () -> catchup.cancelSubscription("y").block(DEADLINE));
        cancellingX.start();
        cancellingY.start();

        awaitBoth(cancelOfX, cancelOfY, cancellingX, cancellingY);
        assertThat(bothInCallerCode.getCount()).as("cancels that ran caller code").isZero();
        assertThat(callerCodeFailures).as("failures in the caller code").isEmpty();
        assertThat(wrapped.cancelCalls).as("cancels of the wrapped model").contains("x", "y");
    }

    private static Thread daemon(String name, CompletableFuture<Void> returned, Runnable work) {
        Thread thread = new Thread(() -> {
            try {
                work.run();
                returned.complete(null);
            } catch (Throwable t) {
                returned.completeExceptionally(t);
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    // A thread that never returns is a deadlock, which must fail here rather than hang the build
    private static void awaitBoth(CompletableFuture<Void> cancelOfX, CompletableFuture<Void> cancelOfY, Thread cancellingX, Thread cancellingY) {
        try {
            CompletableFuture.allOf(cancelOfX, cancelOfY).get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("A cancel did not return within " + DEADLINE.toSeconds() + " seconds. The cancel of x returned: " + cancelOfX.isDone() + ", " + describe(cancellingX)
                    + ". The cancel of y returned: " + cancelOfY.isDone() + ", " + describe(cancellingY));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    private static String describe(Thread thread) {
        return "thread " + thread.getName() + " is " + thread.getState() + " at " + Arrays.stream(thread.getStackTrace()).limit(4).toList();
    }

    // Three events at positions 1 to 3, which the replay reads at once, so it waits in the action of the first
    private static final class ImmediateReader implements UnscopedPositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            long from = range.afterPosition().orElse(0L) + 1;
            long to = Math.min(range.upToPosition().orElse(3L), 3L);
            return Flux.fromStream(LongStream.rangeClosed(from, to).boxed()
                    .map(position -> CloudEventBuilder.v1()
                            .withId("e" + position)
                            .withSource(URI.create("urn:test"))
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

    // A named subscription model that holds nothing, since no replay in these tests reaches the handover
    private static final class StubWrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        static final Checkpoint TOKEN = new StringBasedCheckpoint("token");
        final List<String> cancelCalls = new CopyOnWriteArrayList<>();

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
            throw new AssertionError("A replay that waits in its action must not hand over");
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
            return true;
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
            throw new AssertionError("Nothing is paused");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new AssertionError("Nothing is running");
        }
    }
}
