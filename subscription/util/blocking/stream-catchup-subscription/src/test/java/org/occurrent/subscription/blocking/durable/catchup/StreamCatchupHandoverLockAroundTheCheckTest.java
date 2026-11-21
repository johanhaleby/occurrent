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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A replay decides that it may let an event through, or store a position, and registers that it did, under the
 * handover lock. A cancel takes the same lock, so one that comes between the decision and the registration waits for
 * the replay to register, and so for the action or the store, instead of returning ahead of what the replay then does.
 * The replay is held right after it has decided, which is inside the lock only while the lock is taken.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StreamCatchupHandoverLockAroundTheCheckTest {

    private static final String ID = "subscription";

    private final KeepsTheLiveActionSubscriptionModel live = new KeepsTheLiveActionSubscriptionModel();
    private final CountDownLatch heldAfterTheCheck = new CountDownLatch(1);
    private final CountDownLatch releaseTheReplay = new CountDownLatch(1);
    private final AtomicBoolean held = new AtomicBoolean();
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
        AtomicInteger actionCalls = new AtomicInteger();
        catchup.subscribe(ID, StartAtTime.beginningOfTime(), event -> actionCalls.incrementAndGet());
        assertThat(heldAfterTheCheck.await(5, TimeUnit.SECONDS)).as("the replay is held after it decided to let the first event through").isTrue();

        // When
        Thread cancel = new Thread(() -> catchup.cancelSubscription(ID));
        cancel.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING);
        boolean cancelWaited = cancel.isAlive();
        releaseTheReplay.countDown();
        cancel.join(5000);

        // Then
        assertAll(
                () -> assertThat(cancelWaited).as("the cancel is still waiting while the replay is held after its decision").isTrue(),
                () -> assertThat(cancel.isAlive()).as("the cancel returned once the replay was released").isFalse(),
                () -> assertThat(actionCalls.get()).as("the calls the action had had when the cancel returned, the second event is not delivered").isEqualTo(1));
    }

    @Test
    void a_cancel_that_comes_once_the_replay_has_decided_to_store_a_position_returns_only_after_that_position_has_been_stored_and_deleted() throws InterruptedException {
        // Given the replay has decided to store the position of the first event and is held before it registers that it will
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        catchup = new StreamCatchupSubscriptionModel(live, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1))) {
            @Override
            protected boolean isSafeToPersistFor(String subscriptionId) {
                boolean safe = super.isSafeToPersistFor(subscriptionId);
                if (safe && held.compareAndSet(false, true)) {
                    holdTheReplay();
                }
                return safe;
            }
        };
        catchup.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(heldAfterTheCheck.await(5, TimeUnit.SECONDS)).as("the replay is held after it decided to store a position").isTrue();

        // When
        Thread cancel = new Thread(() -> catchup.cancelSubscription(ID));
        cancel.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING);
        boolean cancelWaited = cancel.isAlive();
        releaseTheReplay.countDown();
        cancel.join(5000);

        // Then
        assertAll(
                () -> assertThat(cancelWaited).as("the cancel is still waiting while the replay is held after its decision").isTrue(),
                () -> assertThat(cancel.isAlive()).as("the cancel returned once the replay was released").isFalse(),
                () -> assertThat(storage.read(ID)).as("the position stored for the cancelled subscription").isNull());
    }

    private void holdTheReplay() {
        heldAfterTheCheck.countDown();
        try {
            releaseTheReplay.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").withTime(OffsetDateTime.now()).build();
    }
}
