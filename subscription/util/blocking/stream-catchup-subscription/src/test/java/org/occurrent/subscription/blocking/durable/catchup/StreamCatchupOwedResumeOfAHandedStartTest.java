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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code start(true)} an action handed over leaves the resume of a paused subscription whose checkpoint read doesn't
 * answer until after its turn, and owes the subscription that resume for as long as it is the same registration that
 * nobody has paused or cancelled since. The read is held past both the wait for it before the turn and the wait of the
 * lifecycle thread for the resume, so the resume that runs once the read answers must leave a subscription cancelled and
 * subscribed again, or paused, as it is, and must park its replay when a {@code stopReplay()} came after the turn.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupOwedResumeOfAHandedStartTest {

    private static final String A = "a";
    private static final String X = "x";
    private static final String Y = "y";
    private static final long BOUND_SECONDS = 25;
    private static final Duration LONG_ENOUGH_FOR_A_RESUME_TO_ACT = Duration.ofSeconds(2);
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private final AtomicBoolean aStartedTheModel = new AtomicBoolean();
    private final AtomicBoolean xResumesFromTheBeginning = new AtomicBoolean();
    private final CopyOnWriteArrayList<String> idsDeliveredToX = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemory;
    private RepositionableHookedModel wrapped;
    private InMemoryEventStore eventStore;
    private StorageThatHoldsTheFirstReadOfX storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_with_a_paused_x_and_an_a_whose_action_hands_a_start_over() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new RepositionableHookedModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory);
        storage = new StorageThatHoldsTheFirstReadOfX();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        for (int i = 0; i < 3; i++) {
            eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        }
        model.subscribe(A, null, StartAt.now(), event -> {
            if (aStartedTheModel.compareAndSet(false, true)) {
                model.start(true);
            }
        });
    }

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        storage.release.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_a_handed_start_left_until_its_read_answered_does_not_resume_a_subscription_cancelled_and_subscribed_again_and_paused_since_its_turn() throws Exception {
        // Given the read of x made by a start an action handed over is held past both waits, and the resume of x is owed
        givenAPausedX(StartAt.dynamic(() -> null));
        givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed();
        CopyOnWriteArrayList<String> deliveredToTheReplacement = new CopyOnWriteArrayList<>();

        // When x is cancelled, subscribed again, and the replacement is paused, and then the read answers
        model.cancelSubscription(X);
        model.subscribe(X, null, StartAt.now(), event -> deliveredToTheReplacement.add(event.getId()));
        model.pauseSubscription(X);
        storage.release.countDown();
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));

        // Then the replacement stays paused, gets nothing, and the live delegate resumed nothing
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(model.isPaused(X)).as("whether the replacement of x is paused once the resume of the start it replaced had its read answered").isTrue());
        assertThat(model.isRunning(X)).as("whether the replacement of x runs once the resume of the start it replaced had its read answered").isFalse();
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
        assertThat(deliveredToTheReplacement).as("the events delivered to the replacement of x").isEmpty();
        assertThat(idsDeliveredToX).as("the events delivered to the cancelled x").isEmpty();
    }

    @Test
    void a_resume_a_handed_start_left_until_its_read_answered_does_not_resume_a_subscription_paused_again_since_its_turn_and_the_pause_returns_normally() throws Exception {
        // Given the read of x made by a start an action handed over is held past both waits, and the resume of x is owed
        givenAPausedX(StartAt.dynamic(() -> null));
        givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed();

        // When x, which the live delegate holds paused until that resume, is paused, and then the read answers
        assertThatCode(() -> model.pauseSubscription(X)).as("what the pause of x threw while the live delegate holds x paused until the owed resume").doesNotThrowAnyException();
        storage.release.countDown();

        // Then x stays paused and the live delegate resumed nothing
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(model.isPaused(X)).as("whether x is paused once the resume of the start it was paused after had its read answered").isTrue());
        assertThat(model.isRunning(X)).as("whether x runs once the resume of the start it was paused after had its read answered").isFalse();
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
        assertThat(idsDeliveredToX).as("the events delivered to x").isEmpty();
    }

    @Test
    void a_resume_a_handed_start_left_until_its_read_answered_still_resumes_a_subscription_whose_pause_threw_something_other_than_not_running_and_changed_nothing() throws Exception {
        // Given the read of x made by a start an action handed over is held past both waits, and the resume of x is owed
        givenAPausedX(StartAt.dynamic(() -> null));
        givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed();
        IllegalStateException refused = new IllegalStateException("the live delegate failed the pause of x");
        CopyOnWriteArrayList<String> pausesThatReachedTheLiveDelegate = new CopyOnWriteArrayList<>();
        wrapped.beforePause = id -> {
            pausesThatReachedTheLiveDelegate.add(id);
            throw refused;
        };

        // When x is paused and the live delegate throws, and then the read answers
        assertThatThrownBy(() -> model.pauseSubscription(X)).as("what the pause of x threw while the live delegate failed it").isSameAs(refused);
        assertThat(pausesThatReachedTheLiveDelegate).as("the subscriptions whose pause reached the live delegate while the resume of x was owed").containsExactly(X);
        storage.release.countDown();

        // Then x is resumed by the start, as it is when the start comes before a pause that failed and changed nothing
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.isRunning(X)).as("whether x runs once the resume of the start had its read answered, after a pause that changed nothing").isTrue());
        assertThat(model.isPaused(X)).as("whether x is paused once the resume of the start had its read answered, after a pause that changed nothing").isFalse();
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").contains(X);
    }

    @Test
    void a_resume_a_handed_start_left_until_its_read_answered_parks_its_replay_when_a_stopReplay_and_a_resumeReplay_came_after_the_turn() throws Exception {
        // Given x has a stored position behind the history, so a resume of it replays, and the start's read of x is held past both waits
        givenAPausedXWhoseResumeReplays();
        storage.save(X, STORED);
        givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed();

        // When the replay is stopped and resumed after the turn, and then the read answers
        model.stopReplay();
        model.resumeReplay();
        storage.release.countDown();

        // Then the replay of x parks itself and delivers nothing
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.hasParkedReplay(X)).as("whether the replay the owed resume of x started parked itself").isTrue());
        assertThat(idsDeliveredToX).as("the events delivered to x by a replay that is parked").isEmpty();

        // And a resume of x runs the replay
        model.resumeSubscription(X);
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(idsDeliveredToX).as("the events delivered to x once its own resume ran the parked replay").hasSize(2));
    }

    @Test
    void a_resume_a_handed_start_left_until_its_read_answered_does_not_resume_a_subscription_subscribed_again_and_paused_after_a_cancel_that_ended_before_the_turn_did() throws Exception {
        // Given x is paused, and y is paused with a read that answers at once
        givenAPausedX(StartAt.dynamic(() -> null));
        model.subscribe(Y, null, StartAt.dynamic(() -> null), event -> {
        });
        model.pauseSubscription(Y);

        // And the cancel of x is under way in the live delegate when the start's turn lists the subscriptions it resumes
        CountDownLatch cancelMayEnd = new CountDownLatch(1);
        CountDownLatch cancelOfXEntered = new CountDownLatch(1);
        wrapped.beforeCancel = id -> {
            cancelOfXEntered.countDown();
            HookedSubscriptionModel.awaitLatch(cancelMayEnd);
        };
        CompletableFuture<Void> cancelOfX = CompletableFuture.runAsync(() -> model.cancelSubscription(X), daemonExecutor("cancel-of-x"));
        assertThat(cancelOfXEntered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the cancel of x is under way in the live delegate").isTrue();

        // And that cancel ends while the turn resumes y, which is after the turn listed x and before it ends
        wrapped.beforeResume = () -> {
            cancelMayEnd.countDown();
            try {
                cancelOfX.get(BOUND_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("the cancel of x did not end while the turn resumed", e);
            }
        };
        CopyOnWriteArrayList<String> deliveredToTheReplacement = new CopyOnWriteArrayList<>();

        // When the start's turn ends with the resume of x owed, and x is subscribed again and paused, and then the read answers
        givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed(false);
        assertThat(cancelOfX).as("the cancel of x that ended before the turn did").isCompleted();
        model.subscribePaused(X, null, StartAtTime.beginningOfTime(), event -> deliveredToTheReplacement.add(event.getId()));
        storage.release.countDown();
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));

        // Then the replacement stays paused, gets nothing, and the live delegate resumed nothing but y
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(model.isPaused(X)).as("whether the replacement of x is paused once the resume of the start it replaced had its read answered").isTrue());
        assertThat(model.isRunning(X)).as("whether the replacement of x runs once the resume of the start it replaced had its read answered").isFalse();
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
        assertThat(deliveredToTheReplacement).as("the events delivered to the replacement of x").isEmpty();
    }

    private void givenAPausedX(StartAt startAt) {
        model.subscribe(X, null, startAt, event -> idsDeliveredToX.add(event.getId()));
        model.pauseSubscription(X);
    }

    // X starts live, so it gets no history when it is subscribed. A resume of it replays from its stored position
    private void givenAPausedXWhoseResumeReplays() {
        givenAPausedX(StartAt.dynamic(() -> xResumesFromTheBeginning.get() ? StartAtTime.beginningOfTime() : null));
        xResumesFromTheBeginning.set(true);
    }

    private void givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed() throws Exception {
        givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed(true);
    }

    private void givenTheStartIsHandedOverAndTheReadOfXIsHeldWhileTheResumeIsOwed(boolean xIsStillPaused) throws Exception {
        storage.holds.set(true);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start the action of a handed over read the storage for x").isTrue();
        // The turn has ended once the resume is owed, which is after the start's wait for its reads, and that wait is
        // the one the wait of the lifecycle thread ends with
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(theResumeOwedFor(X)).as("whether the start owes x the resume it left until after its turn").isTrue());
        if (xIsStillPaused) {
            assertThat(model.isPaused(X)).as("whether x is paused before its read answered").isTrue();
        }
    }

    private boolean theResumeOwedFor(String subscriptionId) throws Exception {
        Field owedResumes = AbstractCatchupSubscriptionModel.class.getDeclaredField("owedResumes");
        owedResumes.setAccessible(true);
        return ((Map<?, ?>) owedResumes.get(model)).containsKey(subscriptionId);
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static final class StorageThatHoldsTheFirstReadOfX extends InMemoryCheckpointStorage {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean holds = new AtomicBoolean();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (X.equals(subscriptionId) && holds.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    release.await(80, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.read(subscriptionId);
        }
    }

    // A live delegate a catch-up can reposition, so a subscription whose replay has ended resumes in it
    private static final class RepositionableHookedModel extends HookedSubscriptionModel implements RepositionableSubscriptions {
        RepositionableHookedModel(InMemorySubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }
    }
}
