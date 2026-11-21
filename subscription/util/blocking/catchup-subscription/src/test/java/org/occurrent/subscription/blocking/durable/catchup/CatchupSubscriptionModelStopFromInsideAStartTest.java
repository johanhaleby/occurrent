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
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code stop()} made on the thread of a {@code start(true)}, either from a {@code StartAt.dynamic} function the
 * start asks for a subscription the live delegate holds paused, or from inside the wrapped model's own {@code start(..)}.
 * No serial order of a start and a stop leaves the wrapped model running while the catch-up children are stopped, so
 * once the start returned each child must be stopped and the wrapped model not running, as when the stop came right
 * after the start. A later start must then behave as it does after that order. A stop followed by a start from the same
 * function is the other serial order, and leaves the model started.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelStopFromInsideAStartTest {

    private InMemorySubscriptionModel inMemory;
    private HookedLiveSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;
    private ExecutorService executor;
    private final AtomicBoolean acted = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> actionThrew = new AtomicReference<>();
    private final List<String> delivered = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedLiveSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new CatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
        executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "call-on-a-thread");
            thread.setDaemon(true);
            return thread;
        });
    }

    @AfterEach
    void shut_down() {
        try {
            executor.shutdownNow();
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_stop_called_from_a_start_at_function_the_start_asks_leaves_each_child_stopped_and_the_wrapped_model_not_running_once_the_start_returns() throws Exception {
        // Given
        givenAStoppedModelWhoseHeldPausedSubscriptionHasAStartAtFunctionThatRuns(model::stop);

        // When
        start();

        // Then
        assertThat(acted).as("whether the start asked the function, which called the catch-up model's stop").isTrue();
        assertThat(actionThrew.get()).as("what the stop called from the function threw").isNull();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs exactly when the children are not stopped, stopped being %s", stoppedOfEachChild()).isEqualTo(!allChildrenStopped());
        assertThat(stoppedOfEachChild()).as("whether each child is stopped once the start returned").containsOnly(true);
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the start returned").isFalse();
        assertThat(wrapped.starts.get()).as("the starts the wrapped model got after a stop that came before it").isZero();
    }

    @Test
    @Timeout(40)
    void a_start_after_a_stop_called_from_a_start_at_function_replays_the_history_of_a_new_subscription_and_goes_live() throws Exception {
        // Given
        givenAStoppedModelWhoseHeldPausedSubscriptionHasAStartAtFunctionThatRuns(model::stop);
        start();
        assertThat(acted).as("whether the first start asked the function, which called the catch-up model's stop").isTrue();
        String history = write();
        model.subscribe("late", null, StartAtTime.beginningOfTime(), event -> delivered.add(event.getId()));

        // When
        start();

        // Then
        assertThatTheHistoryIsReplayedAndTheSubscriptionIsLive(history);
    }

    @Test
    @Timeout(40)
    void a_stop_called_from_inside_the_wrapped_start_leaves_each_child_stopped_and_the_wrapped_model_not_running_once_the_start_returns() throws Exception {
        // Given
        givenAStoppedModelWhoseWrappedStartRuns(model::stop);

        // When
        start();

        // Then
        assertThat(acted).as("whether the wrapped start ran the stop of the catch-up model").isTrue();
        assertThat(actionThrew.get()).as("what the stop called from inside the wrapped start threw").isNull();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs exactly when the children are not stopped, stopped being %s", stoppedOfEachChild()).isEqualTo(!allChildrenStopped());
        assertThat(stoppedOfEachChild()).as("whether each child is stopped once the start returned").containsOnly(true);
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the start returned").isFalse();
    }

    @Test
    @Timeout(40)
    void a_start_after_a_stop_called_from_inside_the_wrapped_start_replays_the_history_of_a_new_subscription_and_goes_live() throws Exception {
        // Given
        givenAStoppedModelWhoseWrappedStartRuns(model::stop);
        start();
        assertThat(acted).as("whether the wrapped start ran the stop of the catch-up model").isTrue();
        String history = write();
        model.subscribe("late", null, StartAtTime.beginningOfTime(), event -> delivered.add(event.getId()));

        // When
        start();

        // Then
        assertThatTheHistoryIsReplayedAndTheSubscriptionIsLive(history);
    }

    @Test
    @Timeout(40)
    void a_stop_followed_by_a_start_from_a_start_at_function_the_start_asks_leaves_each_child_started_and_the_wrapped_model_running() throws Exception {
        // Given
        givenAStoppedModelWhoseHeldPausedSubscriptionHasAStartAtFunctionThatRuns(() -> {
            model.stop();
            model.start(true);
        });

        // When
        start();

        // Then
        assertThat(acted).as("whether the start asked the function, which called stop and then start").isTrue();
        assertThat(actionThrew.get()).as("what the stop and the start called from the function threw").isNull();
        assertThat(stoppedOfEachChild()).as("whether each child is stopped once the start returned").containsOnly(false);
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the start returned").isTrue();
    }

    @Test
    @Timeout(40)
    void a_stop_called_while_the_start_lists_the_subscriptions_the_wrapped_model_holds_paused_leaves_each_child_stopped_and_those_subscriptions_not_resumed() throws Exception {
        // Given
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        model = new CatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory), new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        AtomicBoolean armed = new AtomicBoolean();
        for (String id : List.of("first", "second")) {
            // A non-null position and a stored one make the start resume this subscription itself, one at a time, after the wrapped start
            model.subscribe(id, null, StartAt.dynamic(() -> armed.get() && wrapped.starts.get() == 0 ? StartAtTime.beginningOfTime() : null), event -> {
            });
            model.pauseSubscription(id);
            storage.save(id, new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, new StringBasedCheckpoint("token-1"))));
        }
        model.stop();
        armed.set(true);
        wrapped.beforeIsPaused = () -> {
            if (wrapped.starts.get() > 0 && acted.compareAndSet(false, true)) {
                runCatching(model::stop);
            }
        };

        // When
        start();

        // Then
        assertThat(acted).as("whether the start asked the wrapped model about a paused subscription after its start, which called the catch-up model's stop").isTrue();
        assertThat(actionThrew.get()).as("what the stop called while the start listed the paused subscriptions threw").isNull();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs exactly when the children are not stopped, stopped being %s", stoppedOfEachChild()).isEqualTo(!allChildrenStopped());
        assertThat(stoppedOfEachChild()).as("whether each child is stopped once the start returned").containsOnly(true);
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the start returned").isFalse();
        assertThat(model.isRunning("first")).as("whether first runs once the start returned").isFalse();
        assertThat(model.isRunning("second")).as("whether second runs once the start returned").isFalse();
    }

    private void givenAStoppedModelWhoseHeldPausedSubscriptionHasAStartAtFunctionThatRuns(Runnable onTheFirstAsk) throws Exception {
        AtomicBoolean armed = new AtomicBoolean();
        model.subscribe("subscription", null, StartAt.dynamic(() -> {
            if (armed.get() && acted.compareAndSet(false, true)) {
                runCatching(onTheFirstAsk);
            }
            return null;
        }), event -> {
        });
        model.pauseSubscription("subscription");
        assertThat(model.isPaused("subscription")).as("whether the subscription is paused before the start").isTrue();
        model.stop();
        assertThat(stoppedOfEachChild()).as("whether each child is stopped before the start").containsOnly(true);
        armed.set(true);
    }

    private void givenAStoppedModelWhoseWrappedStartRuns(Runnable onTheFirstStart) throws Exception {
        model.stop();
        assertThat(stoppedOfEachChild()).as("whether each child is stopped before the start").containsOnly(true);
        wrapped.beforeStart = () -> {
            if (acted.compareAndSet(false, true)) {
                runCatching(onTheFirstStart);
            }
        };
    }

    private void runCatching(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            actionThrew.set(e);
        }
    }

    private void start() throws Exception {
        Future<?> start = executor.submit(() -> model.start(true));
        start.get(10, TimeUnit.SECONDS);
    }

    private String write() {
        CloudEvent event = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
        eventStore.write(UUID.randomUUID().toString(), List.of(event));
        return event.getId();
    }

    private void assertThatTheHistoryIsReplayedAndTheSubscriptionIsLive(String history) throws Exception {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(delivered).as("the events delivered to the new subscription after the later start").contains(history));
        String live = write();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(delivered).as("the events delivered to the new subscription once it is live").contains(live));
        assertThat(stoppedOfEachChild()).as("whether each child is stopped after the later start").containsOnly(false);
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs after the later start").isTrue();
    }

    private boolean allChildrenStopped() throws Exception {
        return stoppedOfEachChild().stream().allMatch(stopped -> stopped);
    }

    // The children are private, and each keeps its own stopped flag
    private List<Boolean> stoppedOfEachChild() throws Exception {
        return Stream.of("streamCatchupSubscriptionModel", "agnosticCatchupSubscriptionModel").map(name -> {
            try {
                Field field = CatchupSubscriptionModel.class.getDeclaredField(name);
                field.setAccessible(true);
                return ((AbstractCatchupSubscriptionModel) field.get(model)).stopped;
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    // Runs a hook of the test before its own start(..) and before it reports a subscription paused, and otherwise passes on to the in-memory model
    private static final class HookedLiveSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        final AtomicInteger starts = new AtomicInteger();
        volatile Runnable beforeStart = () -> {
        };
        volatile Runnable beforeIsPaused = () -> {
        };

        private HookedLiveSubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("in-memory-global-position");
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            starts.incrementAndGet();
            beforeStart.run();
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            beforeIsPaused.run();
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
