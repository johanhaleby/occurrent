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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Fresh-context verification test for reviewer CLAIM 2 (BLOCKER): the reactor
 * {@code subscription/push/reactor} {@link CatchupThenPushSubscriptionModel} is claimed to lack the ownership
 * guards ({@code BlockingHandover}'s {@code self}/{@code ownLaunch} identity checks and
 * {@code completeIfStillOwned}) that the blocking twin has. An old replay (A) still folding when
 * {@code cancelSubscription} runs, followed immediately by a fresh {@code subscribe} that installs a new replay
 * (B) under the same id, can complete late and, purely by removing map entries keyed by subscription id rather
 * than by the replay's own identity, evict B's bookkeeping and write B's completion marker for a history B never
 * actually finished folding.
 * <p>
 * This test forces exactly that interleaving and checks the state the reviewer says gets corrupted: the
 * catch-up-complete marker, {@code isCatchingUp}, and a pause requested against the new replay, all observed
 * while B is still demonstrably blocked mid-fold on its own first event. If CLAIM 2 is real, A's late completion
 * corrupts all three before B has done anything.
 */
class CatchupThenPushSubscriptionModelOwnershipVerificationTest {

    @Test
    void an_old_replays_late_completion_does_not_evict_the_new_replays_bookkeeping_or_write_its_marker() throws Exception {
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        PositionOrderedReader reader = reader(() -> Flux.just(
                cloudEvent("1", "Created"), cloudEvent("2", "Updated"), cloudEvent("3", "Updated")), 3);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);

        // Replay A parks on its last event ("3"), which is what drives ReactiveHandover.catchUp(..) into its
        // genuine success path (record marker, go live) once released, regardless of what happens to "sub" while
        // parked.
        CountDownLatch oldReplayParkedOnLastEvent = new CountDownLatch(1);
        CountDownLatch releaseOldReplay = new CountDownLatch(1);
        List<String> oldReplayFolded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            oldReplayFolded.add(ce.getId());
            if (ce.getId().equals("3")) {
                oldReplayParkedOnLastEvent.countDown();
                awaitLatch(releaseOldReplay);
            }
        }));
        assertThat(oldReplayParkedOnLastEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancelling does not stop the old replay's boundedElastic thread, only the registration and whatever map
        // entries it owns at this moment. The old replay is left running, blocked, entirely unaware of the cancel.
        model.cancelSubscription("sub");

        // Replay B parks on its first event, well before it could possibly have caught up.
        CountDownLatch newReplayParkedOnFirstEvent = new CountDownLatch(1);
        CountDownLatch releaseNewReplay = new CountDownLatch(1);
        List<String> newReplayFolded = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), ce -> Mono.fromRunnable(() -> {
            newReplayFolded.add(ce.getId());
            newReplayParkedOnFirstEvent.countDown();
            awaitLatch(releaseNewReplay);
        }));
        assertThat(newReplayParkedOnFirstEvent.await(5, TimeUnit.SECONDS)).isTrue();

        // Registered while "sub" maps to B's replay signal, so this is a pause meant for B, applied once B's own
        // catch-up finishes.
        model.pauseSubscription("sub");
        assertThat(model.isPaused("sub")).isTrue();

        // A's late, stale completion runs now, well after "sub" moved on to B.
        releaseOldReplay.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(oldReplayFolded).containsExactly("1", "2", "3"));

        // B is still genuinely blocked on its own first event the whole time; if any of the assertions below
        // observe "sub" as caught up, paused-for-real, or marked, that is exclusively A's doing. A's remaining
        // work after its last fold returns (record marker, flip live, run the completion callback) is pure
        // in-process reactive plumbing with nothing to wait on externally, so a generous fixed wait is enough for
        // it to have settled one way or the other.
        Thread.sleep(1500);
        assertThat(newReplayFolded).as("B has not progressed past its own first event").containsExactly("1");

        assertThat(marker.read("sub").hasElement().block())
                .as("the old replay's late completion must not mark \"sub\" caught up for a history the "
                        + "projection currently registered under that id (B) never finished folding")
                .isFalse();

        assertThat(model.isCatchingUp("sub"))
                .as("B is still genuinely replaying (parked on its own first event), isCatchingUp must not have "
                        + "been flipped to false by A's unrelated, late completion")
                .isTrue();

        assertThat(feed.isPaused("sub"))
                .as("the pause requested against B must not be applied by A's completion; B has not caught up")
                .isFalse();
        assertThat(model.isPaused("sub")).as("still pending, not lost").isTrue();

        releaseNewReplay.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(newReplayFolded).containsExactly("1", "2", "3"));
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

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .build();
    }
}
