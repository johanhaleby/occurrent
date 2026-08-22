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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A completing replay and a lifecycle call for the same id want this model's monitor at the same time, and both
 * have to finish.
 * <p>
 * The model takes its own monitor from {@code subscribe}, from {@code launchReplay}'s registering put and its
 * completion check, and from {@code cancelSubscription}, {@code pauseSubscription}, {@code resumeSubscription} and
 * {@code start}. A replay's completion runs on {@code boundedElastic} and takes the monitor there, so these race
 * that completion against a lifecycle call and assert both return rather than hang. The blocking twin has the same
 * test as {@code a_completing_replay_and_a_concurrent_cancel_plus_resubscribe_never_deadlock_on_the_models_monitor}.
 */
class CatchupThenPushSubscriptionModelDeadlockTest {

    @Test
    void a_completing_replay_and_a_concurrent_cancel_plus_resubscribe_never_deadlock() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1")), 1);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);

        CountDownLatch parkedOnOnlyEvent = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            parkedOnOnlyEvent.countDown();
            awaitLatch(releaseFold);
        }));
        assertThat(parkedOnOnlyEvent.await(5, TimeUnit.SECONDS)).isTrue();

        CountDownLatch bothReady = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);

        // Unblocks the parked fold on its boundedElastic thread, which then runs markCaughtUp, flips live, and
        // reaches completeIfStillOwned, each of which takes the model's monitor.
        Thread releaseTrigger = new Thread(() -> {
            bothReady.countDown();
            awaitLatch(go);
            releaseFold.countDown();
        }, "release-trigger");

        // Takes the SAME monitor, first for cancelSubscription's removal, then again inside launchReplay's
        // registering put, racing directly against the completing replay above.
        Thread cancelAndResubscribe = new Thread(() -> {
            bothReady.countDown();
            awaitLatch(go);
            model.cancelSubscription("sub");
            model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.empty());
        }, "cancel-and-resubscribe");

        releaseTrigger.start();
        cancelAndResubscribe.start();
        assertThat(bothReady.await(5, TimeUnit.SECONDS)).isTrue();
        go.countDown();

        releaseTrigger.join(5_000);
        cancelAndResubscribe.join(5_000);

        assertThat(releaseTrigger.isAlive())
                .as("no deadlock between the replay's completion and a concurrent cancelSubscription/subscribe "
                        + "racing for the model's monitor")
                .isFalse();
        assertThat(cancelAndResubscribe.isAlive())
                .as("no deadlock between the replay's completion and a concurrent cancelSubscription/subscribe "
                        + "racing for the model's monitor")
                .isFalse();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (model.isCatchingUp("sub") && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(model.isCatchingUp("sub")).as("the winning replay, old or new, eventually finishes catching up")
                .isFalse();
    }

    @Test
    void a_completing_replay_and_a_concurrent_pause_never_deadlock() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(cloudEvent("1")), 1);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);

        CountDownLatch parkedOnOnlyEvent = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            parkedOnOnlyEvent.countDown();
            awaitLatch(releaseFold);
        }));
        assertThat(parkedOnOnlyEvent.await(5, TimeUnit.SECONDS)).isTrue();

        CountDownLatch bothReady = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);

        Thread releaseTrigger = new Thread(() -> {
            bothReady.countDown();
            awaitLatch(go);
            releaseFold.countDown();
        }, "release-trigger");

        // pauseSubscription is synchronized on the same monitor and, while a replay is registered for the id,
        // only touches pauseRequestedDuringReplay, so it must never block behind the replay's own completion for
        // longer than the monitor hand-off itself.
        Thread pausing = new Thread(() -> {
            bothReady.countDown();
            awaitLatch(go);
            model.pauseSubscription("sub");
        }, "pausing");

        releaseTrigger.start();
        pausing.start();
        assertThat(bothReady.await(5, TimeUnit.SECONDS)).isTrue();
        go.countDown();

        releaseTrigger.join(5_000);
        pausing.join(5_000);

        assertThat(releaseTrigger.isAlive())
                .as("no deadlock between the replay's completion and a concurrent pauseSubscription racing for "
                        + "the model's monitor")
                .isFalse();
        assertThat(pausing.isAlive())
                .as("no deadlock between the replay's completion and a concurrent pauseSubscription racing for "
                        + "the model's monitor")
                .isFalse();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the latch", e);
        }
    }

    private static PositionOrderedReader reader(Supplier<Flux<CloudEvent>> flux, long head) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(flux::get);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(head);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Created")
                .build();
    }
}
