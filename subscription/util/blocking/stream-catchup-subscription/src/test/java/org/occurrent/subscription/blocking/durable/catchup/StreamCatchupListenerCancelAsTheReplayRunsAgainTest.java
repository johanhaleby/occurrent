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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.cloudEvent;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.daemonExecutor;

/**
 * A replay of x runs again once the live model has lost the live start it goes live from, and tells the
 * {@link CatchupListener} of x on the thread that runs it again that it starts again. A cancel of x made there, after
 * which the listener throws, leaves no replay of x held, so a later {@code start(true)} has nothing of x to run again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupListenerCancelAsTheReplayRunsAgainTest {

    private static final String X = "x";
    private static final long BOUND_SECONDS = 25;
    private static final int HISTORY = 3;
    private static final Duration LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER = Duration.ofSeconds(2);

    private final InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel();
    private final InMemoryEventStore eventStore = new InMemoryEventStore(inMemory);
    private final CopyOnWriteArrayList<String> idsDeliveredToX = new CopyOnWriteArrayList<>();
    private final AtomicInteger catchupStarts = new AtomicInteger();
    private final AtomicReference<Throwable> cancelThrew = new AtomicReference<>();
    private @Nullable StreamCatchupSubscriptionModel model;

    @AfterEach
    void shut_down() throws Exception {
        CompletableFuture.runAsync(() -> {
            if (model != null) {
                model.shutdown();
            }
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_cancel_made_by_the_listener_of_x_that_then_throws_when_the_replay_runs_again_before_its_handover_ends_the_replay_so_a_later_start_runs_nothing() {
        // Given a live model without the history back to the first live start it is asked for, so the replay of x runs again before it hands over
        HistoryLosingLiveModel liveModel = new HistoryLosingLiveModel().withoutHistoryBack(new StringBasedCheckpoint("live-start-1"));

        // When x is subscribed from the beginning of time, and its listener cancels it when it is told that the replay runs again and then throws
        StreamCatchupSubscriptionModel model = givenAModelOverWhoseListenerOfXCancelsAndThrowsOnTheSecondTell(liveModel);
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> idsDeliveredToX.add(event.getId()));

        // Then the cancel returned, the first replay delivered the history, and no replay of x is held or run
        assertThatTheCancelReturnedOnTheSecondTell();
        assertThatXIsGoneAndGetsNothingAfterTheFirstReplay(model);

        // And a later start that resumes subscriptions runs no replay of x
        model.start(true);
        assertThatXIsGoneAndGetsNothingAfterTheFirstReplay(model);
    }

    @Test
    void a_cancel_made_by_the_listener_of_x_that_then_throws_when_the_replay_runs_again_on_a_takeover_of_the_lost_live_start_drops_the_replay_so_a_later_start_runs_nothing() {
        // Given a live model that loses the first live start it opens, which the catch-up takes over by running the replay again
        LiveStartLosingSubscriptionModel liveModel = new LiveStartLosingSubscriptionModel(inMemory, false, true, () -> {
        });

        // When x is subscribed from the beginning of time, and its listener cancels it when it is told that the replay runs again and then throws
        StreamCatchupSubscriptionModel model = givenAModelOverWhoseListenerOfXCancelsAndThrowsOnTheSecondTell(liveModel);
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> idsDeliveredToX.add(event.getId()));

        // Then the cancel returned, the first replay delivered the history, and no replay of x is held or run
        assertThatTheCancelReturnedOnTheSecondTell();
        assertThatXIsGoneAndGetsNothingAfterTheFirstReplay(model);

        // And a later start that resumes subscriptions runs no replay of x
        model.start(true);
        assertThatXIsGoneAndGetsNothingAfterTheFirstReplay(model);
    }

    private StreamCatchupSubscriptionModel givenAModelOverWhoseListenerOfXCancelsAndThrowsOnTheSecondTell(CheckpointAwareSubscriptionModel liveModel) {
        for (int i = 0; i < HISTORY; i++) {
            write();
        }
        StreamCatchupSubscriptionModel made = new StreamCatchupSubscriptionModel(liveModel, eventStore, new CatchupSubscriptionModelConfig(100));
        model = made;
        made.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (catchupStarts.incrementAndGet() == 2) {
                    try {
                        made.cancelSubscription(X);
                    } catch (Throwable e) {
                        cancelThrew.set(e);
                    }
                    throw new IllegalStateException("The listener of x failed");
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        return made;
    }

    private void assertThatTheCancelReturnedOnTheSecondTell() {
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(catchupStarts).as("the times the listener of x was told that a replay starts").hasValue(2));
        assertThat(cancelThrew.get()).as("what the cancel of x made by the listener threw").isNull();
    }

    private void assertThatXIsGoneAndGetsNothingAfterTheFirstReplay(StreamCatchupSubscriptionModel model) {
        write();
        await().during(LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(model.hasParkedReplay(X)).as("whether a replay of the cancelled x is parked").isFalse();
            assertThat(model.isPaused(X)).as("whether the cancelled x reads as paused").isFalse();
            assertThat(model.isRunning(X)).as("whether the cancelled x reads as running").isFalse();
            assertThat(idsDeliveredToX).as("the events delivered to x, which only its first replay delivered").hasSize(HISTORY);
            assertThat(catchupStarts).as("the times the listener of x was told that a replay starts").hasValue(2);
        });
    }

    private void write() {
        CloudEvent event = cloudEvent();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
    }
}
