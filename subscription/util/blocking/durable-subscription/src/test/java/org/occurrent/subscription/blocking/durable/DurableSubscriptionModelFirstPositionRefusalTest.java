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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A subscription registered with the model default and no stored checkpoint is either recorded from the wrapped
 * model's {@code globalCheckpoint()} before anything is delivered, or refused when that answer is {@code null}.
 * Issue #852 is what these tests close: without the refusal, a fresh subscription over a wrapped model that cannot
 * answer starts from wherever the feed is, and a crash after a failed first delivery starts over from wherever the
 * feed has reached by then, so the failed event is never seen again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionRefusalTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_subscription_that_cannot_record_a_first_position_is_refused_rather_than_losing_its_first_event_to_a_restart() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        List<String> deliveredBeforeRestart = new ArrayList<>();

        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> {
            deliveredBeforeRestart.add(cloudEvent.getId());
            throw new IllegalStateException("first delivery fails");
        }));

        // Either arm keeps the same promise, no first event is silently lost. A model that refuses has nothing to
        // lose. One that starts anyway, which is what 0.33.0 did, must survive the sequence below, where the first
        // delivery fails and the process dies before any checkpoint is saved.
        if (refusal == null) {
            feed.publish(cloudEvent("event-1"));
            assertThat(deliveredBeforeRestart).containsExactly("event-1");
            durable.shutdown();

            DurableSubscriptionModel restarted = new DurableSubscriptionModel(feed, storage);
            List<String> deliveredAfterRestart = new ArrayList<>();
            restarted.subscribe(SUBSCRIPTION_ID, cloudEvent -> deliveredAfterRestart.add(cloudEvent.getId()));

            assertThat(deliveredAfterRestart).contains("event-1");
        } else {
            assertThat(refusal).isInstanceOf(IllegalStateException.class).hasMessageContaining("answered nothing");
        }
    }

    @Test
    void subscribing_with_the_model_default_is_refused_when_nothing_is_stored_and_the_position_source_cannot_answer() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(InMemoryFeed.class.getName())
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing");

        assertThat(feed.subscriptions).isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_refused_subscription_id_is_left_free_so_subscribing_again_works_once_the_position_source_answers() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).isInstanceOf(IllegalStateException.class);

        feed.answersCurrentPosition = true;
        List<String> delivered = new ArrayList<>();
        durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId()));
        feed.publish(cloudEvent("event-1"));

        assertThat(delivered).containsExactly("event-1");
    }

    /**
     * The wrapped model here accepts the id and evaluates the start position on a thread of its own while the
     * subscribe asks it for the global checkpoint, the way the MongoDB models do once their subscribe has returned.
     * The evaluation has to wait, and once the subscribe is refused it has to fail rather than start at the present
     * with nothing stored.
     */
    @Test
    void a_start_position_evaluated_while_the_first_position_is_recorded_starts_nothing_once_the_subscribe_is_refused() {
        EvaluatesWhileAskedForTheGlobalCheckpoint wrapped = new EvaluatesWhileAskedForTheGlobalCheckpoint();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("answered nothing");

        assertThat(wrapped.evaluated).as("the start position the wrapped model evaluated")
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .havingCause()
                .withMessageContaining("was refused before its start position was recorded");
        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds").isEmpty();
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    /**
     * The wrapped model here waits on cancel for the thread that evaluates the start position, the way a model with a
     * thread per subscription can, and that evaluation waits while the first position is recorded.
     */
    @Test
    void a_refused_subscribe_returns_when_the_wrapped_models_cancel_waits_for_the_thread_evaluating_the_start_position() {
        EvaluatesWhileAskedForTheGlobalCheckpoint wrapped = new EvaluatesWhileAskedForTheGlobalCheckpoint();
        wrapped.cancelWaitsForTheEvaluation = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }), runnable -> Thread.ofPlatform().daemon().start(runnable));

        try {
            assertThat(subscribing).as("the refused subscribe")
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableThat()
                    .havingCause()
                    .isInstanceOf(IllegalStateException.class)
                    .withMessageContaining("answered nothing");
        } finally {
            wrapped.interruptTheEvaluation();
        }
        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds").isEmpty();
    }

    @Test
    void a_subscription_the_wrapped_model_failed_to_cancel_after_a_refused_subscribe_is_cancelled_by_cancel_subscription() {
        EvaluatesWhileAskedForTheGlobalCheckpoint wrapped = new EvaluatesWhileAskedForTheGlobalCheckpoint();
        wrapped.cancelsThatFail.set(1);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertThat(refusal).isInstanceOf(IllegalStateException.class).hasMessageContaining("answered nothing");
        assertThat(refusal.getSuppressed()).as("what the refusal says about the failed cancel").singleElement().satisfies(suppressed -> {
            assertThat(suppressed).hasMessageContaining("may still hold it")
                    .hasMessageContaining("cancelSubscription(\"" + SUBSCRIPTION_ID + "\")");
            assertThat(suppressed.getCause()).hasMessage("the cancel fails");
        });
        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds after the failed cancel").containsExactly(SUBSCRIPTION_ID);

        durable.cancelSubscription(SUBSCRIPTION_ID);

        assertThat(wrapped.subscriptions).as("the subscriptions the wrapped model holds after cancelSubscription").isEmpty();
    }

    @Test
    void cancel_subscription_after_a_failed_cancel_keeps_the_checkpoint_an_earlier_run_stored_so_subscribing_again_resumes_from_it() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        wrapped.cancelsThatFail.set(1);
        AtomicInteger readsThatFail = new AtomicInteger(1);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public @Nullable Checkpoint read(String subscriptionId) {
                if (readsThatFail.getAndDecrement() > 0) {
                    throw new IllegalStateException("the read fails");
                }
                return super.read(subscriptionId);
            }
        };
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("earlier run"));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));
        assertThat(refusal).hasMessage("the read fails");
        assertThat(refusal.getSuppressed()).as("what the refusal says about the failed cancel").singleElement()
                .satisfies(suppressed -> assertThat(suppressed).hasMessageContaining("keeps the checkpoint stored"));

        durable.cancelSubscription(SUBSCRIPTION_ID);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(wrapped.evaluateStartPosition(SUBSCRIPTION_ID)).as("where the subscription starts")
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("earlier run"));
    }

    /**
     * The action is still running when {@code cancelSubscription} cancels the subscription, and returns afterwards.
     */
    @Test
    void a_subscription_the_wrapped_model_failed_to_cancel_stores_no_checkpoint_once_cancel_subscription_has_returned() throws InterruptedException {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated();
        wrapped.cancelsThatFail.set(1);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        CountDownLatch actionRuns = new CountDownLatch(1);
        CountDownLatch actionMayReturn = new CountDownLatch(1);
        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            actionRuns.countDown();
            try {
                actionMayReturn.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertThat(refusal.getSuppressed()).as("what the refusal says about the failed cancel").hasSize(1);
        Thread delivery = Thread.ofPlatform().start(() -> wrapped.deliver(SUBSCRIPTION_ID,
                new CheckpointAwareCloudEvent(cloudEvent("event-1"), new StringBasedCheckpoint("1"))));
        assertThat(actionRuns.await(5, TimeUnit.SECONDS)).as("whether the action runs").isTrue();

        durable.cancelSubscription(SUBSCRIPTION_ID);
        actionMayReturn.countDown();
        delivery.join();

        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored after cancelSubscription").isFalse();
    }

    /**
     * The wrapped model here evaluates the start position and holds the subscription before its subscribe throws.
     */
    @Test
    void a_wrapped_model_whose_subscribe_throws_after_it_got_the_start_position_is_left_holding_no_subscription() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.subscribeThrowsAfterHoldingTheSubscription = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).hasMessage("the subscribe fails");

        assertThat(feed.subscriptions).as("the subscriptions the wrapped model holds").isEmpty();
        assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("the position the evaluation stored").isEqualTo("0");
    }

    /**
     * The wrapped model here evaluates the start position before it refuses an id it already holds.
     */
    @Test
    void a_duplicate_the_wrapped_model_refuses_after_it_got_the_start_position_leaves_the_running_subscription_alone() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.refusesAHeldIdAfterEvaluating = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).isInstanceOf(DuplicateSubscriptionIdException.class);

        assertThat(feed.subscriptions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
    }

    @Test
    void a_dynamic_start_position_resolving_to_the_model_default_is_refused_the_same_way() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("answered nothing");
    }

    @Test
    void the_first_position_is_recorded_before_anything_is_delivered_when_the_position_source_answers() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(storage.read(SUBSCRIPTION_ID).asString()).isEqualTo("0");
    }

    @Test
    void a_restart_after_a_failed_first_delivery_resumes_from_the_recorded_position() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
            throw new IllegalStateException("first delivery fails");
        });
        feed.publish(cloudEvent("event-1"));
        durable.shutdown();

        DurableSubscriptionModel restarted = new DurableSubscriptionModel(feed, storage);
        List<String> deliveredAfterRestart = new ArrayList<>();
        restarted.subscribe(SUBSCRIPTION_ID, cloudEvent -> deliveredAfterRestart.add(cloudEvent.getId()));

        assertThat(deliveredAfterRestart).containsExactly("event-1");
    }

    @Test
    void a_stored_checkpoint_is_taken_without_asking_the_position_source() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("0"));
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        List<String> delivered = new ArrayList<>();

        durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId()));
        feed.publish(cloudEvent("event-1"));

        assertThat(delivered).containsExactly("event-1");
        assertThat(feed.globalCheckpointCalls).isZero();
    }

    @Test
    void a_start_position_of_your_own_is_never_refused_and_records_nothing() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(feed.subscriptions).containsKey(SUBSCRIPTION_ID);
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_dynamic_start_position_opting_out_hands_the_subscription_to_the_wrapped_model_unchanged() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        StartAt optOut = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? null : StartAt.now());

        durable.subscribe(SUBSCRIPTION_ID, null, optOut, __ -> {
        });

        assertThat(feed.subscriptions).containsKey(SUBSCRIPTION_ID);
        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void the_override_starts_a_subscription_the_position_source_cannot_answer_for_and_records_nothing() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        List<String> delivered = new ArrayList<>();

        durable.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId()));

        assertThat(storage.exists(SUBSCRIPTION_ID)).isFalse();
        feed.publish(cloudEvent("event-1"));
        assertThat(delivered).containsExactly("event-1");
    }

    @Test
    void the_override_keeps_the_loss_window_it_accepts_so_a_restart_after_a_failed_first_delivery_starts_from_the_feed() {
        InMemoryFeed feed = new InMemoryFeed();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModelConfig config = new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, config);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
            throw new IllegalStateException("first delivery fails");
        });
        feed.publish(cloudEvent("event-1"));
        durable.shutdown();

        DurableSubscriptionModel restarted = new DurableSubscriptionModel(feed, storage, config);
        List<String> deliveredAfterRestart = new ArrayList<>();
        restarted.subscribe(SUBSCRIPTION_ID, cloudEvent -> deliveredAfterRestart.add(cloudEvent.getId()));
        feed.publish(cloudEvent("event-2"));

        assertThat(deliveredAfterRestart).containsExactly("event-2");
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
    }

    /**
     * A feed with change-stream mechanics reduced to what these tests need. The model default and
     * {@link StartAt#now()} both mean the end of what has been published so far, a checkpoint means everything
     * after that position, and each subscription is delivered to synchronously. A delivery whose action throws is
     * not retried and the subscription's position advances past the event anyway, which is the state a crash right
     * after the failure leaves behind. The published events and nothing else survive {@link #shutdown()}, the way
     * a database outlives a process.
     */
    private static final class InMemoryFeed implements CheckpointAwareSubscriptionModel {
        final List<CloudEvent> published = new ArrayList<>();
        final Map<String, FeedSubscription> subscriptions = new LinkedHashMap<>();
        boolean answersCurrentPosition = false;
        boolean subscribeThrowsAfterHoldingTheSubscription = false;
        boolean refusesAHeldIdAfterEvaluating = false;
        int globalCheckpointCalls = 0;

        void publish(CloudEvent cloudEvent) {
            published.add(cloudEvent);
            List.copyOf(subscriptions.values()).forEach(this::deliverPending);
        }

        private void deliverPending(FeedSubscription subscription) {
            while (subscription.position < published.size()) {
                CloudEvent next = published.get(subscription.position);
                subscription.position++;
                try {
                    subscription.action.accept(new CheckpointAwareCloudEvent(next, new StringBasedCheckpoint(Integer.toString(subscription.position))));
                } catch (RuntimeException ignored) {
                }
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.isDynamic() ? startAt.get(new SubscriptionModelContext(InMemoryFeed.class)) : startAt;
            int position = resolved instanceof StartAt.StartAtCheckpoint startAtCheckpoint
                    ? Integer.parseInt(startAtCheckpoint.checkpoint.asString())
                    : published.size();
            if (refusesAHeldIdAfterEvaluating && subscriptions.containsKey(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            FeedSubscription subscription = new FeedSubscription(position, action);
            subscriptions.put(subscriptionId, subscription);
            deliverPending(subscription);
            if (subscribeThrowsAfterHoldingTheSubscription) {
                throw new IllegalStateException("the subscribe fails");
            }
            return dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            globalCheckpointCalls++;
            return answersCurrentPosition ? new StringBasedCheckpoint(Integer.toString(published.size())) : null;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
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
            return subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        private static Subscription dummySubscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        private static final class FeedSubscription {
            int position;
            final Consumer<CloudEvent> action;

            private FeedSubscription(int position, Consumer<CloudEvent> action) {
                this.position = position;
                this.action = action;
            }
        }
    }

    /**
     * Evaluates the start position of a subscription on a thread of its own once it is asked for the global
     * checkpoint, and answers {@code null} only once that evaluation waits or has finished, so the evaluation always
     * comes while the first position is recorded. The first {@code cancelsThatFail} cancels throw, and with
     * {@code cancelWaitsForTheEvaluation} set a cancel waits for the evaluating thread to end.
     */
    private static final class EvaluatesWhileAskedForTheGlobalCheckpoint implements CheckpointAwareSubscriptionModel {
        final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
        final CompletableFuture<@Nullable StartAt> evaluated = new CompletableFuture<>();
        final AtomicInteger cancelsThatFail = new AtomicInteger();
        volatile boolean cancelWaitsForTheEvaluation;
        private final CountDownLatch askedForTheGlobalCheckpoint = new CountDownLatch(1);
        private volatile boolean evaluating;
        private volatile @Nullable Thread evaluator;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.add(subscriptionId);
            evaluator = Thread.ofPlatform().start(() -> {
                try {
                    askedForTheGlobalCheckpoint.await();
                    evaluating = true;
                    evaluated.complete(startAt.get(new SubscriptionModelContext(EvaluatesWhileAskedForTheGlobalCheckpoint.class)));
                } catch (Throwable t) {
                    evaluated.completeExceptionally(t);
                }
            });
            return InMemoryFeed.dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            askedForTheGlobalCheckpoint.countDown();
            long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (!evaluationWaitsOrIsDone() && System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            return null;
        }

        private boolean evaluationWaitsOrIsDone() {
            Thread thread = evaluator;
            if (thread == null || !evaluating) {
                return false;
            }
            Thread.State state = thread.getState();
            return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING || state == Thread.State.TERMINATED;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
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
            return subscriptions.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return InMemoryFeed.dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            if (cancelsThatFail.getAndDecrement() > 0) {
                throw new IllegalStateException("the cancel fails");
            }
            subscriptions.remove(subscriptionId);
            Thread thread = evaluator;
            if (cancelWaitsForTheEvaluation && thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void interruptTheEvaluation() {
            Thread thread = evaluator;
            if (thread != null) {
                thread.interrupt();
            }
        }
    }

    /**
     * Holds the action and the start position of each subscription without evaluating it, so a test delivers events
     * and evaluates the start position itself. The first {@code cancelsThatFail} cancels throw.
     */
    private static final class HoldsTheStartPositionUnevaluated implements CheckpointAwareSubscriptionModel {
        final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        final Map<String, StartAt> startAts = new ConcurrentHashMap<>();
        final AtomicInteger cancelsThatFail = new AtomicInteger();
        volatile @Nullable Checkpoint globalCheckpoint;

        void deliver(String subscriptionId, CloudEvent cloudEvent) {
            actions.get(subscriptionId).accept(cloudEvent);
        }

        StartAt evaluateStartPosition(String subscriptionId) {
            return startAts.get(subscriptionId).get(new SubscriptionModelContext(HoldsTheStartPositionUnevaluated.class));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (actions.putIfAbsent(subscriptionId, action) != null) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            startAts.put(subscriptionId, startAt);
            return InMemoryFeed.dummySubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            if (cancelsThatFail.getAndDecrement() > 0) {
                throw new IllegalStateException("the cancel fails");
            }
            actions.remove(subscriptionId);
            startAts.remove(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public void shutdown() {
            actions.clear();
            startAts.clear();
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
            return actions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return InMemoryFeed.dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }
    }
}
