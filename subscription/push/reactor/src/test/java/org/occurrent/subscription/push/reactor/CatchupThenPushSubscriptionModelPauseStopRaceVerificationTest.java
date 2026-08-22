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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fresh-context verification test for reviewer CLAIM 7 (MAJOR): the reactor
 * {@code CatchupThenPushSubscriptionModel.applyPendingPauseIfAny(String)} is claimed to lack the
 * {@code liveFeed.isRunning(subscriptionId)} guard the blocking twin's own {@code applyPendingPauseIfAny} has
 * (compare {@code subscription/push/blocking/CatchupThenPushSubscriptionModel#applyPendingPauseIfAny}, which
 * checks {@code liveFeed.isRunning(subscriptionId)} before calling {@code pauseSubscription}). If a pause was
 * requested during a replay, the replay reaches its own success completion, but {@code stop()} lands before the
 * completion callback actually runs, {@code liveFeed.pauseSubscription(id)} is claimed to throw
 * {@code SubscriptionNotRunningException} from inside that callback, which runs as the {@code onNext} consumer of
 * {@code catchupDone.subscribe(...)} in {@code launchReplay}. {@code replayDone.tryEmitValue(caughtUp)}, the line
 * immediately after {@code applyPendingPauseIfAny(...)} in that same lambda, is claimed to never run as a result,
 * so {@code waitUntilStarted()} never observes completion.
 * <p>
 * The race window is made deterministic with a {@link CheckpointStorage} whose {@code save(...)} blocks: by the
 * time it is parked there, the replay has already folded its one historical event and committed to the success
 * path (past every {@code keepReplaying()} check), and is strictly before {@code ReactiveHandover}'s own
 * {@code catchupDone.tryEmitValue(true)} that synchronously triggers this model's completion callback.
 * <p>
 * This test forces exactly that interleaving (pause during replay, then {@code stop()} while parked in the
 * marker write) and asserts the CORRECT behavior the reviewer proposes: {@code waitUntilStarted()} still
 * completes. If CLAIM 7 is real, this assertion fails: verified with a bounded {@link StepVerifier} timeout so a
 * genuine hang fails the test rather than blocking the build forever.
 */
class CatchupThenPushSubscriptionModelPauseStopRaceVerificationTest {

    @Test
    void a_pause_requested_during_replay_racing_stop_before_the_completion_callback_does_not_hang_waitUntilStarted() throws Exception {
        CloudEvent event1 = cloudEvent("1", "Created");

        CountDownLatch markEntered = new CountDownLatch(1);
        CountDownLatch releaseMark = new CountDownLatch(1);
        CheckpointStorage marker = blockingOnSaveCheckpointStorage(markEntered, releaseMark);

        PositionOrderedReader reader = reader(() -> Flux.just(event1), 1);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);

        SubscriptionHandle subscription = model.subscribe("sub", null, StartAt.subscriptionModelDefault(),
                ce -> Mono.fromRunnable(() -> {
                }));

        // The replay's own marker write is what we park on, strictly after it folded the one historical event and
        // committed to the success path.
        assertThat(markEntered.await(5, TimeUnit.SECONDS))
                .as("the replay must have folded its history and be recording the catch-up-complete marker")
                .isTrue();

        // Registered while replayingSubscriptions still maps "sub" to this replay (it has not completed yet), so
        // this is a pause meant to be applied once the replay's own completion callback runs.
        model.pauseSubscription("sub");
        assertThat(model.isPaused("sub")).isTrue();

        // stop() lands here: before the completion callback (which only runs once the marker write below is
        // released) has had any chance to execute, but the replay has already decided it will succeed.
        model.stop();

        releaseMark.countDown();

        StepVerifier.create(subscription.waitUntilStarted())
                .as("applyPendingPauseIfAny(..) must not silently swallow the completion signal even if pausing "
                        + "a stopped live feed is refused")
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

    private static CheckpointStorage blockingOnSaveCheckpointStorage(CountDownLatch entered, CountDownLatch release) {
        AtomicReference<Checkpoint> stored = new AtomicReference<>();
        return new CheckpointStorage() {
            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return Mono.justOrEmpty(stored.get());
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return Mono.fromCallable(() -> {
                    entered.countDown();
                    awaitLatch(release);
                    stored.set(checkpoint);
                    return checkpoint;
                });
            }

            @Override
            public boolean evaluatesWriteConditionsFor(String subscriptionId) {
                return false;
            }

            @Override
            public Mono<Long> writeVersion(String subscriptionId) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.empty();
            }
        };
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

    private static PositionOrderedReader reader(java.util.function.Supplier<Flux<CloudEvent>> flux, long head) {
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
