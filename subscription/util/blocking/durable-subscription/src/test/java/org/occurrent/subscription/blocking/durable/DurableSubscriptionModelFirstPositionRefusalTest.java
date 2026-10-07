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
import java.util.concurrent.CopyOnWriteArrayList;
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
                .withMessageContaining("failed before this evaluation got its start position");
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
    void a_wrapped_model_whose_subscribe_throws_after_it_got_the_start_position_keeps_its_subscription_and_the_exception_says_so() {
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.subscribeThrowsAfterHoldingTheSubscription = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertThat(thrown).hasMessage("the subscribe fails");
        assertThat(thrown.getSuppressed()).as("what the exception says about the subscription the wrapped model may hold").singleElement()
                .satisfies(suppressed -> assertThat(suppressed).hasMessageContaining("may still hold a subscription")
                        .hasMessageContaining("getWrappedSubscriptionModel().cancelSubscription(\"" + SUBSCRIPTION_ID + "\")"));
        assertThat(feed.subscriptions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
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

        Throwable duplicate = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertThat(duplicate).isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(duplicate.getSuppressed()).as("what the duplicate says about the subscription the wrapped model holds").isEmpty();
        assertThat(feed.subscriptions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
    }

    @Test
    void a_wrapped_model_whose_subscribe_throws_before_it_evaluated_the_start_position_adds_nothing_to_the_exception() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                throw new IllegalArgumentException("unsupported subscription");
            }
        };
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertThat(thrown).hasMessage("unsupported subscription");
        assertThat(thrown.getSuppressed()).as("what the exception says about a subscription the wrapped model may hold").isEmpty();
    }

    /**
     * The wrapped model here holds the subscription, then evaluates the start position inside its subscribe and passes
     * on what the evaluation throws.
     */
    @Test
    void a_wrapped_model_that_passes_on_what_its_evaluation_threw_keeps_its_subscription_until_the_wrapped_model_cancels_it() {
        HoldsTheStartPositionUnevaluated wrapped = new EvaluatesOnceItHoldsTheSubscription();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertThat(refusal).hasMessageContaining("answered nothing");
        assertThat(refusal.getSuppressed()).as("what the refusal says about the subscription the wrapped model may hold").singleElement()
                .satisfies(suppressed -> assertThat(suppressed).hasMessageContaining("may still hold a subscription"));
        assertThat(wrapped.actions).as("the subscriptions the wrapped model holds after the refusal").containsOnlyKeys(SUBSCRIPTION_ID);

        durable.getWrappedSubscriptionModel().cancelSubscription(SUBSCRIPTION_ID);
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        Throwable subscribingAgain = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));

        assertThat(subscribingAgain).as("subscribing again once the model can answer").isNull();
    }

    /**
     * The wrapped model here can answer a position, holds the subscription and throws without evaluating the start
     * position.
     */
    @Test
    void an_evaluation_after_a_subscribe_whose_wrapped_model_threw_before_evaluating_gets_no_start_position() {
        HoldsTheSubscriptionThenThrows wrapped = new HoldsTheSubscriptionThenThrows(false);
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).hasMessage("the subscribe fails");

        Throwable evaluation = catchThrowable(() -> wrapped.evaluateStartPosition(SUBSCRIPTION_ID));

        assertThat(evaluation).isInstanceOf(IllegalStateException.class).hasMessageContaining("failed before this evaluation got its start position");
        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored").isFalse();
    }

    /**
     * The wrapped model here holds the subscription and evaluates the start position, which records the position it
     * answers, before its subscribe throws. The held subscription then delivers an event.
     */
    @Test
    void an_evaluation_after_a_subscribe_whose_wrapped_model_threw_after_evaluating_starts_from_the_checkpoint_the_held_subscription_stored_since() {
        HoldsTheSubscriptionThenThrows wrapped = new HoldsTheSubscriptionThenThrows(true);
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).hasMessage("the subscribe fails");
        wrapped.deliver(SUBSCRIPTION_ID, new CheckpointAwareCloudEvent(cloudEvent("event-1"), new StringBasedCheckpoint("after event-1")));

        StartAt evaluation = wrapped.evaluateStartPosition(SUBSCRIPTION_ID);

        assertThat(evaluation).as("where the evaluation starts")
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("after event-1"));
        assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("the checkpoint stored").isEqualTo("after event-1");
    }

    /**
     * The wrapped model here holds the subscription and evaluates the start position while it can't answer a position,
     * which gets it the model default, before its subscribe throws. It answers a position afterwards.
     */
    @Test
    void an_evaluation_after_a_subscribe_whose_wrapped_model_threw_after_its_evaluation_recorded_nothing_records_the_position_the_wrapped_model_answers_by_then() {
        HoldsTheSubscriptionThenThrows wrapped = new HoldsTheSubscriptionThenThrows(true);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).hasMessage("the subscribe fails");
        assertThat(wrapped.evaluation).as("where the evaluation inside subscribe starts").isInstanceOf(StartAt.Default.class);
        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored before the wrapped model answers").isFalse();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");

        StartAt evaluation = wrapped.evaluateStartPosition(SUBSCRIPTION_ID);

        assertThat(evaluation).as("where the later evaluation starts")
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("present"));
        assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("the checkpoint stored").isEqualTo("present");
    }

    /**
     * The wrapped model here holds the subscription, starts evaluating the start position on a thread of its own, and
     * throws while that evaluation still reads the checkpoint an earlier run stored.
     */
    @Test
    void cancelling_on_the_wrapped_model_after_a_subscribe_that_threw_while_its_evaluation_was_recording_keeps_the_checkpoint_an_earlier_run_stored() throws InterruptedException {
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch readMayReturn = new CountDownLatch(1);
        AtomicInteger readsThatWait = new AtomicInteger(1);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public @Nullable Checkpoint read(String subscriptionId) {
                if (readsThatWait.getAndDecrement() > 0) {
                    readStarted.countDown();
                    try {
                        readMayReturn.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.read(subscriptionId);
            }
        };
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("earlier run"));
        AtomicInteger subscribesThatThrow = new AtomicInteger(1);
        List<Thread> evaluations = new CopyOnWriteArrayList<>();
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                Subscription subscription = super.subscribe(subscriptionId, filter, startAt, action);
                if (subscribesThatThrow.getAndDecrement() <= 0) {
                    return subscription;
                }
                evaluations.add(Thread.ofPlatform().start(() -> catchThrowable(() -> evaluate(startAt))));
                try {
                    readStarted.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("timed out waiting for the start position");
            }
        };
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);

        Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));
        readMayReturn.countDown();
        evaluations.getFirst().join();

        assertThat(thrown).hasMessage("timed out waiting for the start position");
        assertThat(thrown.getSuppressed()).as("what the exception says about the subscription the wrapped model may hold").singleElement()
                .satisfies(suppressed -> assertThat(suppressed).hasMessageContaining("may still hold a subscription"));
        assertThat(wrapped.actions).as("the subscriptions the wrapped model holds after the subscribe threw").containsOnlyKeys(SUBSCRIPTION_ID);

        durable.getWrappedSubscriptionModel().cancelSubscription(SUBSCRIPTION_ID);
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(wrapped.evaluateStartPosition(SUBSCRIPTION_ID)).as("where the subscription starts")
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("earlier run"));
    }

    /**
     * The wrapped model here evaluates the start position before it refuses a filter it can't apply. Two durable models
     * share it, so the second one doesn't know the first one holds the id.
     */
    @Test
    void a_second_durable_models_subscribe_that_the_wrapped_model_refuses_after_it_got_the_start_position_leaves_the_first_ones_subscription_alone() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                evaluate(startAt);
                if (filter != null) {
                    throw new IllegalArgumentException("unsupported filter");
                }
                return super.subscribe(subscriptionId, filter, startAt, action);
            }
        };
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel first = new DurableSubscriptionModel(wrapped, storage);
        DurableSubscriptionModel second = new DurableSubscriptionModel(wrapped, storage);
        first.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThatThrownBy(() -> second.subscribe(SUBSCRIPTION_ID, new SubscriptionFilter() {
        }, StartAt.subscriptionModelDefault(), __ -> {
        })).hasMessage("unsupported filter");

        assertThat(wrapped.actions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
    }

    /**
     * The id is subscribed straight on the wrapped model, which evaluates the start position before it refuses an id it
     * already holds, and refuses it with something other than {@link DuplicateSubscriptionIdException}.
     */
    @Test
    void a_refusal_other_than_a_duplicate_after_the_wrapped_model_got_the_start_position_leaves_a_subscription_made_straight_on_the_wrapped_model_alone() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                evaluate(startAt);
                if (actions.containsKey(subscriptionId)) {
                    throw new IllegalArgumentException("Subscription " + subscriptionId + " is already registered");
                }
                return super.subscribe(subscriptionId, filter, startAt, action);
            }
        };
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        wrapped.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).hasMessageContaining("already registered");

        assertThat(wrapped.actions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
    }

    /**
     * The wrapped model here evaluates the start position before it refuses an id it already holds, and refuses it
     * with something other than {@link DuplicateSubscriptionIdException}.
     */
    @Test
    void a_refusal_other_than_a_duplicate_after_the_wrapped_model_got_the_start_position_leaves_the_running_subscription_alone() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                evaluate(startAt);
                if (actions.containsKey(subscriptionId)) {
                    throw new IllegalArgumentException("Subscription " + subscriptionId + " is already registered");
                }
                return super.subscribe(subscriptionId, filter, startAt, action);
            }
        };
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).hasMessageContaining("already registered");

        assertThat(wrapped.actions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
    }

    /**
     * The wrapped model here evaluates the start position before it refuses a filter it can't apply, and only then
     * would refuse an id it already holds.
     */
    @Test
    void a_filter_the_wrapped_model_refuses_after_it_got_the_start_position_leaves_the_running_subscription_alone() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                evaluate(startAt);
                if (filter != null) {
                    throw new IllegalArgumentException("unsupported filter");
                }
                return super.subscribe(subscriptionId, filter, startAt, action);
            }
        };
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, new SubscriptionFilter() {
        }, StartAt.subscriptionModelDefault(), __ -> {
        })).hasMessage("unsupported filter");

        assertThat(wrapped.actions).as("the subscriptions the wrapped model holds").containsOnlyKeys(SUBSCRIPTION_ID);
    }

    /**
     * The wrapped model here evaluates the start position on a thread of its own before its subscribe returns, the
     * way the MongoDB models can, and evaluates it again later when that evaluation throws.
     */
    @Test
    void an_evaluation_that_throws_before_the_wrapped_subscribe_returns_leaves_the_subscribe_to_record_the_position() {
        HoldsTheStartPositionUnevaluated wrapped = new HoldsTheStartPositionUnevaluated() {
            @Override
            public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
                Subscription subscription = super.subscribe(subscriptionId, filter, startAt, action);
                Thread evaluation = Thread.ofPlatform().start(() -> catchThrowable(() -> evaluate(startAt)));
                try {
                    evaluation.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                globalCheckpoint = new StringBasedCheckpoint("present");
                return subscription;
            }
        };
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        CompletableFuture<StartAt> evaluatedAgain = CompletableFuture.supplyAsync(() -> wrapped.evaluateStartPosition(SUBSCRIPTION_ID), Runnable::run);

        assertThat(evaluatedAgain).as("where the subscription starts").succeedsWithin(Duration.ofSeconds(5))
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("present"));
    }

    /**
     * The wrapped model here drops the subscription when the cancel after the refusal throws, so it accepts the id
     * again while the refused subscribe's run can still deliver.
     */
    @Test
    void a_run_a_failed_cancel_left_behind_stores_no_checkpoint_once_a_later_subscribe_replaced_it() {
        CancelDropsTheSubscriptionAndThrowsOnce wrapped = new CancelDropsTheSubscriptionAndThrowsOnce();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));
        assertThat(refusal.getSuppressed()).as("what the refusal says about the failed cancel").hasSize(1);
        Consumer<CloudEvent> leftBehind = wrapped.cancelledActions.getFirst();
        wrapped.globalCheckpoint = new StringBasedCheckpoint("present");
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });
        durable.cancelSubscription(SUBSCRIPTION_ID);

        leftBehind.accept(new CheckpointAwareCloudEvent(cloudEvent("event-1"), new StringBasedCheckpoint("1")));

        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored after the run left behind delivered").isFalse();
    }

    @Test
    void a_run_a_failed_cancel_left_behind_stores_no_checkpoint_once_a_subscribe_that_opts_out_replaced_it() {
        CancelDropsTheSubscriptionAndThrowsOnce wrapped = new CancelDropsTheSubscriptionAndThrowsOnce();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        Throwable refusal = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }));
        assertThat(refusal.getSuppressed()).as("what the refusal says about the failed cancel").hasSize(1);
        Consumer<CloudEvent> leftBehind = wrapped.cancelledActions.getFirst();
        StartAt optOut = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? null : StartAt.now());
        durable.subscribe(SUBSCRIPTION_ID, null, optOut, __ -> {
        });

        leftBehind.accept(new CheckpointAwareCloudEvent(cloudEvent("event-1"), new StringBasedCheckpoint("1")));

        assertThat(storage.exists(SUBSCRIPTION_ID)).as("whether a checkpoint is stored after the run left behind delivered").isFalse();
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
     * and evaluates the start position itself. The first {@code cancelsThatFail} cancels throw. A test overrides
     * {@code subscribe} or {@code cancelSubscription} for a model that does more.
     */
    private static class HoldsTheStartPositionUnevaluated implements CheckpointAwareSubscriptionModel {
        final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        final Map<String, StartAt> startAts = new ConcurrentHashMap<>();
        final AtomicInteger cancelsThatFail = new AtomicInteger();
        volatile @Nullable Checkpoint globalCheckpoint;

        void deliver(String subscriptionId, CloudEvent cloudEvent) {
            actions.get(subscriptionId).accept(cloudEvent);
        }

        StartAt evaluateStartPosition(String subscriptionId) {
            return evaluate(startAts.get(subscriptionId));
        }

        StartAt evaluate(StartAt startAt) {
            return startAt.get(new SubscriptionModelContext(HoldsTheStartPositionUnevaluated.class));
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

    /**
     * Holds the subscription, then evaluates its start position inside {@code subscribe} and passes on what the
     * evaluation throws, still holding the subscription.
     */
    private static final class EvaluatesOnceItHoldsTheSubscription extends HoldsTheStartPositionUnevaluated {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Subscription subscription = super.subscribe(subscriptionId, filter, startAt, action);
            evaluate(startAt);
            return subscription;
        }
    }

    /**
     * Holds the subscription, then throws from {@code subscribe}. Evaluates the start position in between when
     * {@code evaluatesFirst}, and keeps what it got in {@code evaluation}.
     */
    private static final class HoldsTheSubscriptionThenThrows extends HoldsTheStartPositionUnevaluated {
        private final boolean evaluatesFirst;
        volatile @Nullable StartAt evaluation;

        HoldsTheSubscriptionThenThrows(boolean evaluatesFirst) {
            this.evaluatesFirst = evaluatesFirst;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            super.subscribe(subscriptionId, filter, startAt, action);
            if (evaluatesFirst) {
                evaluation = evaluate(startAt);
            }
            throw new IllegalStateException("the subscribe fails");
        }
    }

    /**
     * Drops the subscription on every cancel and keeps its action in {@code cancelledActions}. The first cancel throws
     * once it has dropped the subscription.
     */
    private static final class CancelDropsTheSubscriptionAndThrowsOnce extends HoldsTheStartPositionUnevaluated {
        final List<Consumer<CloudEvent>> cancelledActions = new CopyOnWriteArrayList<>();
        private final AtomicInteger cancelsThatFailAfterDropping = new AtomicInteger(1);

        @Override
        public void cancelSubscription(String subscriptionId) {
            Consumer<CloudEvent> action = actions.get(subscriptionId);
            if (action != null) {
                cancelledActions.add(action);
            }
            super.cancelSubscription(subscriptionId);
            if (cancelsThatFailAfterDropping.getAndDecrement() > 0) {
                throw new IllegalStateException("the cancel fails after dropping the subscription");
            }
        }
    }
}
