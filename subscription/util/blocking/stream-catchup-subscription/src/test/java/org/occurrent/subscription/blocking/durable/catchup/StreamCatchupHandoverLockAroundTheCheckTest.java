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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A replay decides under the handover lock that it may let an event through, or store a position, and records the
 * action or the store as under way before it lets go of the lock. A cancel from another thread marks the replay
 * cancelled under the same lock, so it comes either before that decision or after it. When it comes after, the cancel
 * returns only once the action or the store has returned, and the subscriber's action isn't called and no position is
 * stored once the cancel has returned. The lock isn't fair, so a replay can take it back for the next event before the
 * waiting cancel does, and the tests don't count the actions for that reason.
 * <p>
 * Each test holds the replay inside the lock, right after its decision. The first event's action, and the first store,
 * wait until the cancel waits for them, and then return. The cancel waits up to a second for an action, so the action
 * returns well inside that.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupHandoverLockAroundTheCheckTest {

    private static final String ID = "subscription";

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final CountDownLatch heldAfterTheCheck = new CountDownLatch(1);
    private final CountDownLatch releaseTheReplay = new CountDownLatch(1);
    private final AtomicBoolean held = new AtomicBoolean();
    private final AtomicInteger safeChecks = new AtomicInteger();
    private volatile Thread replayThread;
    private volatile Thread cancelThread;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void an_event_store_with_two_events() {
        eventStore = new InMemoryEventStore().withoutStreamPosition();
        eventStore.write("stream", List.of(event("event1"), event("event2")));
    }

    @AfterEach
    void release_the_replay_and_shut_down() {
        releaseTheReplay.countDown();
        if (catchup != null) {
            catchup.shutdown();
        }
    }

    @Test
    void a_cancel_that_comes_once_the_replay_has_decided_to_let_an_event_through_returns_only_after_that_event_has_been_through_the_action() throws InterruptedException {
        // Given the replay has decided to let the first event through and is held before it registers that it did
        catchup = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected boolean shouldKeepReplaying(String subscriptionId) {
                boolean keepReplaying = super.shouldKeepReplaying(subscriptionId);
                if (keepReplaying && StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("deliverIfStillWanted"))) && held.compareAndSet(false, true)) {
                    holdTheReplay();
                }
                return keepReplaying;
            }
        };
        AtomicInteger actionsStarted = new AtomicInteger();
        AtomicBoolean firstActionReturned = new AtomicBoolean();
        catchup.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
            actionsStarted.incrementAndGet();
            if (event.getId().equals("event1")) {
                waitUntilTheCancelIsIn("awaitTheAdmittedActions");
                firstActionReturned.set(true);
            }
        });
        assertThat(heldAfterTheCheck.await(5, TimeUnit.SECONDS)).as("the replay is held after it decided to let the first event through").isTrue();

        // When
        AtomicBoolean firstActionReturnedWhenTheCancelReturned = new AtomicBoolean();
        AtomicInteger actionsStartedWhenTheCancelReturned = new AtomicInteger(-1);
        Thread cancel = cancelThread = new Thread(() -> {
            catchup.cancelSubscription(ID);
            firstActionReturnedWhenTheCancelReturned.set(firstActionReturned.get());
            actionsStartedWhenTheCancelReturned.set(actionsStarted.get());
        });
        cancel.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING || cancel.getState() == Thread.State.TIMED_WAITING);
        boolean cancelWaited = cancel.isAlive();
        releaseTheReplay.countDown();
        cancel.join(5000);
        replayThread.join(5000);

        // Then
        assertAll(
                () -> assertThat(cancelWaited).as("the cancel is still waiting while the replay is held after its decision").isTrue(),
                () -> assertThat(cancel.isAlive()).as("the cancel returned once the replay was released").isFalse(),
                () -> assertThat(firstActionReturnedWhenTheCancelReturned.get()).as("the action had returned for the first event, which the replay decided to let through, when the cancel returned").isTrue(),
                () -> assertThat(replayThread.isAlive()).as("the replay has ended").isFalse(),
                () -> assertThat(actionsStarted.get() - actionsStartedWhenTheCancelReturned.get()).as("the calls the action began after the cancel had returned").isZero());
    }

    @Test
    void a_cancel_that_comes_once_the_replay_has_decided_to_store_a_position_returns_only_after_that_position_has_been_stored_and_deleted() throws InterruptedException {
        // Given the replay has decided to store the position of the first event and is held before it registers that it will
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        catchup = new StreamCatchupSubscriptionModel(live, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1))) {
            @Override
            protected boolean isSafeToPersistFor(String subscriptionId) {
                boolean safe = super.isSafeToPersistFor(subscriptionId);
                if (safe && safeChecks.incrementAndGet() == 2 && held.compareAndSet(false, true)) {
                    holdTheReplay();
                }
                return safe;
            }
        };
        catchup.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(heldAfterTheCheck.await(5, TimeUnit.SECONDS)).as("the replay is held after it decided to store a position").isTrue();

        // When
        AtomicReference<List<String>> operationsWhenTheCancelReturned = new AtomicReference<>(List.of());
        Thread cancel = cancelThread = new Thread(() -> {
            catchup.cancelSubscription(ID);
            operationsWhenTheCancelReturned.set(storage.operations());
        });
        cancel.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING || cancel.getState() == Thread.State.TIMED_WAITING);
        boolean cancelWaited = cancel.isAlive();
        releaseTheReplay.countDown();
        cancel.join(5000);
        replayThread.join(5000);

        // Then
        List<String> operations = operationsWhenTheCancelReturned.get();
        assertAll(
                () -> assertThat(cancelWaited).as("the cancel is still waiting while the replay is held after its decision").isTrue(),
                () -> assertThat(cancel.isAlive()).as("the cancel returned once the replay was released").isFalse(),
                () -> assertThat(operations).as("the operations on the storage when the cancel returned, the position of the first event stored and then every stored position deleted")
                        .startsWith("save").endsWith("delete"),
                () -> assertThat(replayThread.isAlive()).as("the replay has ended").isFalse(),
                () -> assertThat(storage.operations().size() - operations.size()).as("the operations on the storage after the cancel had returned").isZero());
    }

    // Gives up after 5 seconds and returns, so a cancel that never gets there fails on the assertions and not on this wait
    private void waitUntilTheCancelIsIn(String... methods) {
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                Thread cancel = cancelThread;
                if (cancel == null) {
                    return false;
                }
                if (!cancel.isAlive()) {
                    return true;
                }
                List<String> frames = Arrays.stream(cancel.getStackTrace()).map(StackTraceElement::getMethodName).toList();
                return frames.containsAll(List.of(methods));
            });
        } catch (ConditionTimeoutException ignored) {
        }
    }

    private void holdTheReplay() {
        replayThread = Thread.currentThread();
        heldAfterTheCheck.countDown();
        try {
            releaseTheReplay.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private class RecordingCheckpointStorage extends InMemoryCheckpointStorage {
        private final List<String> operations = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean firstSaveSeen = new AtomicBoolean();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (Thread.currentThread() == replayThread && firstSaveSeen.compareAndSet(false, true)) {
                waitUntilTheCancelIsIn("cancelRunningCatchup", "awaitUninterruptibly");
            }
            Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
            operations.add("save");
            return saved;
        }

        @Override
        public void delete(String subscriptionId) {
            super.delete(subscriptionId);
            operations.add("delete");
        }

        List<String> operations() {
            synchronized (operations) {
                return List.copyOf(operations);
            }
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").withTime(OffsetDateTime.now()).build();
    }
}
