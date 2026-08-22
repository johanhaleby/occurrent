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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpointSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.*;

/**
 * Pins that registering a subscription and starting one are separate events: nothing reaches the wrapped model until
 * the subscription is started, and everything the wrapped model was going to be told is told then instead. Uses a
 * hand-rolled recording model rather than a real one, since what matters is exactly what the wrapped model is asked to
 * do and when.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ManualStartSubscriptionModelTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void registering_a_subscription_does_not_reach_the_wrapped_model() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        List<CloudEvent> received = new CopyOnWriteArrayList<>();

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), received::add);
        delegate.feed(cloudEvent("1"));

        assertThat(delegate.subscribeCalls).isEmpty();
        assertThat(received).isEmpty();
        assertThat(subscription.id()).isEqualTo(SUBSCRIPTION_ID);
        // Answers false rather than true. The subscription has not started and nothing here will start it until the
        // caller asks, so claiming it started would be a lie a caller relying on the answer could act on.
        assertThat(subscription.waitUntilStarted(ofSeconds(1))).isFalse();
    }

    @Test
    void a_deferred_registration_answers_false_and_the_handle_from_starting_it_answers_true() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);

        Subscription deferred = model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        assertThat(deferred.waitUntilStarted(ofSeconds(1))).isFalse();

        Subscription started = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(started.waitUntilStarted(ofSeconds(1))).isTrue();
    }

    @Test
    void a_registered_subscription_reports_itself_paused_so_that_starting_everything_finds_it() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();
        assertThat(model.isRunning()).isFalse();
        assertThat(model.subscriptionIds()).containsExactly(SUBSCRIPTION_ID);
    }

    @Test
    void introspection_stops_at_this_model_rather_than_unwrapping_past_the_registered_subscriptions() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(IntrospectableSubscriptions.findIn(model)).containsSame(model);
    }

    @Test
    void starting_a_subscription_passes_the_wrapped_model_exactly_what_registration_was_given() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        SubscriptionFilter filter = new SubscriptionFilter() {
        };
        StartAt startAt = StartAt.now();
        Consumer<CloudEvent> action = __ -> {
        };

        model.subscribe(SUBSCRIPTION_ID, filter, startAt, action);
        Subscription subscription = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.subscribeCalls).hasSize(1);
        SubscribeCall call = delegate.subscribeCalls.getFirst();
        assertThat(call.subscriptionId()).isEqualTo(SUBSCRIPTION_ID);
        assertThat(call.filter()).isSameAs(filter);
        assertThat(call.startAt()).isSameAs(startAt);
        assertThat(call.action()).isSameAs(action);
        assertThat(subscription).isSameAs(delegate.subscriptions.get(SUBSCRIPTION_ID));
    }

    @Test
    void a_started_subscription_receives_events() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        List<CloudEvent> received = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), received::add);

        model.resumeSubscription(SUBSCRIPTION_ID);
        delegate.feed(cloudEvent("1"));

        assertThat(received).extracting(CloudEvent::getId).containsExactly("1");
    }

    @Test
    void a_model_that_records_no_position_never_resolves_the_start_position_while_a_subscription_is_only_registered() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        AtomicInteger resolutions = new AtomicInteger();
        StartAt countingStartAt = StartAt.dynamic(() -> {
            resolutions.incrementAndGet();
            return StartAt.now();
        });

        model.subscribe(SUBSCRIPTION_ID, null, countingStartAt, __ -> {
        });
        model.isPaused(SUBSCRIPTION_ID);
        model.subscriptionIds();

        assertThat(resolutions).hasValue(0);
    }

    @Test
    void starting_a_subscription_on_a_stopped_wrapped_model_also_resumes_it_so_it_actually_delivers() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        delegate.parkOnSubscribe = true;
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.resumeCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(delegate.paused).isEmpty();
    }

    @Test
    void a_subscription_that_fails_to_start_stays_registered_and_can_be_started_again() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        delegate.failNextSubscribe = true;
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        Throwable failure = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));

        assertThat(failure).isInstanceOf(IllegalStateException.class);
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(model.resumeSubscription(SUBSCRIPTION_ID)).isNotNull();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void registering_the_same_id_twice_is_rejected() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is already defined");
    }

    @Test
    void starting_the_same_subscription_twice_is_rejected() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID)).isInstanceOf(IllegalArgumentException.class);
        assertThat(delegate.subscribeCalls).hasSize(1);
    }

    @Test
    void resuming_a_subscription_another_thread_is_already_starting_reports_that_it_is_already_running() throws InterruptedException {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.subscribeEntered = insideSubscribe;
        delegate.holdSubscribeUntil = releaseSubscribe;
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.execute(() -> model.resumeSubscription(SUBSCRIPTION_ID));
            assertThat(insideSubscribe.await(10, TimeUnit.SECONDS)).isTrue();

            Throwable thrown = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));

            assertThat(thrown)
                    .as("the wrapped model has not been handed the id yet, so asking it would answer that it has no "
                            + "such subscription, which is the one answer that sends a caller looking elsewhere")
                    .isInstanceOf(SubscriptionAlreadyRunningException.class);
        } finally {
            releaseSubscribe.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void several_threads_starting_one_subscription_produce_a_single_subscribe() throws InterruptedException {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        int threads = 8;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger succeeded = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.execute(() -> {
                    try {
                        startGate.await();
                        model.resumeSubscription(SUBSCRIPTION_ID);
                        succeeded.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (RuntimeException ignored) {
                        // Losing the race is the expected outcome for all but one thread.
                    } finally {
                        done.countDown();
                    }
                });
            }
            startGate.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(delegate.subscribeCalls).hasSize(1);
        assertThat(succeeded).hasValue(1);
    }

    @Test
    void pausing_a_subscription_that_has_not_been_started_is_rejected() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThatThrownBy(() -> model.pauseSubscription(SUBSCRIPTION_ID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has not been started");
    }

    @Test
    void a_started_subscription_answers_from_the_wrapped_model_rather_than_from_here() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        model.resumeSubscription(SUBSCRIPTION_ID);

        // Something below can pause a subscription without telling this model, for example on losing a lease.
        delegate.paused.add(SUBSCRIPTION_ID);

        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void cancelling_a_registered_subscription_forgets_it_and_frees_the_id() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        model.cancelSubscription(SUBSCRIPTION_ID);

        assertThat(model.subscriptionIds()).isEmpty();
        assertThat(delegate.cancelCalls).containsExactly(SUBSCRIPTION_ID);
        assertThat(catchThrowable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        }))).isNull();
    }

    @Test
    void stopping_makes_a_later_registration_wait_to_be_started_again() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.start(false);

        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(delegate.subscribeCalls).isEmpty();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThat(delegate.stopCalls).isEqualTo(1);
    }

    @Test
    void resuming_a_subscription_on_a_stopped_model_reopens_it_for_later_registrations() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.start(false);
        model.subscribe("first", null, StartAt.now(), __ -> {
        });
        model.stop();

        model.resumeSubscription("first");
        model.subscribe("second", null, StartAt.now(), __ -> {
        });

        assertThat(model.isRunning())
                .as("the wrapped model reports itself running again after a resume, and this one must not disagree "
                        + "while one of its own subscriptions is delivering")
                .isTrue();
        assertThat(delegate.subscribeCalls).extracting(SubscribeCall::subscriptionId).containsExactly("first", "second");
    }

    @Test
    void resuming_a_subscription_that_was_never_started_leaves_the_rest_waiting() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe("first", null, StartAt.now(), __ -> {
        });

        model.resumeSubscription("first");
        model.subscribe("second", null, StartAt.now(), __ -> {
        });

        assertThat(model.isRunning())
                .as("starting one subscription is not the same as starting a model that has never been started")
                .isFalse();
        assertThat(delegate.subscribeCalls).extracting(SubscribeCall::subscriptionId).containsExactly("first");
        assertThat(model.isPaused("second")).isTrue();
    }

    @Test
    void starting_the_model_starts_every_registered_subscription_in_registration_order() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe("first", null, StartAt.now(), __ -> {
        });
        model.subscribe("second", null, StartAt.now(), __ -> {
        });

        model.start(true);

        assertThat(delegate.subscribeCalls).extracting(SubscribeCall::subscriptionId).containsExactly("first", "second");
        assertThat(model.isRunning()).isTrue();
    }

    @Test
    void a_subscription_registered_while_the_model_is_being_started_is_still_started() throws Exception {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        CountDownLatch registrationInProgress = new CountDownLatch(1);
        CountDownLatch startReturned = new CountDownLatch(1);
        // Capturing the position to pin happens after the id is claimed but before the deferred registration is
        // stored, so parking here holds the registering thread in the window a concurrent start(true) can run in.
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            registrationInProgress.countDown();
            awaitOrFail(startReturned);
            return new StringCheckpoint("at-registration");
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> registering = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
            }));
            assertThat(registrationInProgress.await(10, TimeUnit.SECONDS)).isTrue();
            model.start(true);
            startReturned.countDown();
            registering.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertThat(model.isRunning(SUBSCRIPTION_ID))
                .as("start(true) starts everything registered, including a registration in progress while it ran")
                .isTrue();
        assertThat(delegate.subscribeCalls).extracting(SubscribeCall::subscriptionId).containsExactly(SUBSCRIPTION_ID);
    }

    @Test
    void a_subscription_registered_while_the_model_is_being_stopped_still_starts_from_where_it_was_registered() throws Exception {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        CountDownLatch registrationInProgress = new CountDownLatch(1);
        CountDownLatch stopReturned = new CountDownLatch(1);
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            registrationInProgress.countDown();
            awaitOrFail(stopReturned);
            return new StringCheckpoint("at-registration");
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);
        model.start(false);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> registering = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
            }));
            assertThat(registrationInProgress.await(10, TimeUnit.SECONDS))
                    .as("this registration asks for the model default, so its position is read whatever the state "
                            + "looked like on the way in")
                    .isTrue();
            model.stop();
            stopReturned.countDown();
            registering.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void pinning_the_start_position_uses_if_absent() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> new StringCheckpoint("at-registration");
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThat(storage.conditions.get(SUBSCRIPTION_ID)).isEqualTo(CheckpointWriteCondition.ifAbsent());
    }

    @Test
    void a_position_source_that_answers_nothing_refuses_the_registration_rather_than_recording_none() {
        // Answering null is the source reporting a problem it cannot resolve, so there is no position to hold this
        // registration to. Letting it through would start the subscription from wherever the feed has reached once it
        // is started, skipping everything written while it waited, which is what recording at registration is for.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, () -> null, storage);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing")
                .hasMessageContaining("stoppedByDefault(SubscriptionModel)");

        assertThat(storage.checkpoints).isEmpty();
        assertThat(delegate.subscribeCalls).isEmpty();
        assertThat(model.subscriptionIds())
                .as("the id is left free, so registering again once the source can answer is what a node does")
                .doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void a_position_source_that_answers_nothing_still_registers_a_subscription_that_has_a_stored_checkpoint() {
        // That checkpoint is where the subscription starts, and nothing would have been recorded over it anyway, so
        // there is nothing here for a source that cannot answer to cost. A model on a database that never answers,
        // an Atlas cluster prohibiting hostInfo say, would otherwise stop registering every subscription it has
        // already run once.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringCheckpoint("from-a-previous-run"), CheckpointWriteCondition.any());
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, () -> null, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("from-a-previous-run");
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_position_source_that_answers_nothing_leaves_a_registration_naming_its_own_position_alone() {
        // Nothing is recorded for such a registration in the first place, so there is no position for the source to
        // fail to supply, and a replay the caller asked for is not something to refuse.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, () -> null, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(storage.checkpoints).isEmpty();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_position_source_that_fails_reaches_the_caller_as_the_failure_it_threw() {
        // There is an original failure here, so it is what the caller gets. Wrapping it would bury the reason the
        // position could not be read behind a name that says something else.
        RuntimeException unreachable = new IllegalStateException("the position source is unreachable");
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, () -> {
            throw unreachable;
        }, storage);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).isSameAs(unreachable);

        assertThat(storage.checkpoints).isEmpty();
        assertThat(delegate.subscribeCalls).isEmpty();
        assertThat(model.subscriptionIds()).doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void starting_the_model_again_resumes_a_subscription_the_wrapped_model_paused_on_its_own() {
        // The wrapped model reports itself running throughout, since only one of its subscriptions was paused, not the
        // whole model. Guarding start(true) on delegate.isRunning() (removed) skipped calling delegate.start(..)
        // here, and this subscription is Live rather than Deferred in this model's own registry, so the
        // resumeSubscriptionsAutomatically loop below never reached it either. It stayed paused forever.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        model.start(true);
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();

        // Something below paused it without telling this model, for example on losing a lease, while the wrapped
        // model as a whole stays running.
        delegate.paused.add(SUBSCRIPTION_ID);
        assertThat(delegate.isRunning()).isTrue();

        model.start(true);

        assertThat(delegate.resumeCalls).contains(SUBSCRIPTION_ID);
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void starting_the_model_without_resuming_leaves_registrations_alone_but_lets_later_ones_through() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe("first", null, StartAt.now(), __ -> {
        });

        model.start(false);
        model.subscribe("second", null, StartAt.now(), __ -> {
        });

        assertThat(model.isPaused("first")).isTrue();
        assertThat(delegate.subscribeCalls).extracting(SubscribeCall::subscriptionId).containsExactly("second");
    }

    @Test
    void a_subscription_that_cannot_start_leaves_the_ones_after_it_registered() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe("first", null, StartAt.now(), __ -> {
        });
        model.subscribe("second", null, StartAt.now(), __ -> {
        });
        model.subscribe("third", null, StartAt.now(), __ -> {
        });
        delegate.failSubscribeFor = "second";

        assertThatThrownBy(() -> model.start(true)).isInstanceOf(IllegalStateException.class);

        assertThat(model.isRunning("first")).isTrue();
        assertThat(model.isPaused("second")).isTrue();
        assertThat(model.isPaused("third")).isTrue();
    }

    @Test
    void shutting_down_discards_registrations_that_were_never_started() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        model.shutdown();

        assertThat(delegate.shutdownCalls).isEqualTo(1);
        assertThat(model.subscriptionIds()).isEmpty();
        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        })).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void the_start_position_is_written_to_checkpoint_storage_as_soon_as_a_subscription_registers() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_first_run_starts_from_where_the_subscription_was_registered_rather_than_where_it_was_started() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        delegate.globalCheckpoint = new StringCheckpoint("much-later");
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_subscription_that_has_run_before_keeps_its_own_checkpoint_and_registers_without_complaint() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("from-a-previous-run"));
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");

        assertThatCode(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("a checkpoint that was already stored is the ordinary case of a subscription with history, and a node starting behind a leader election long after another has been running it sees exactly this")
                .doesNotThrowAnyException();
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("from-a-previous-run");
    }

    @Test
    void a_checkpoint_written_between_the_existence_check_and_reading_the_position_is_refused_rather_than_taken_for_prior_history() {
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        // Stores another node's position as a side effect of answering the position query, so the write arrives
        // between this registration's existence check and its own read, both of which happen inside subscribe().
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("landed-during-registration"));
            return new StringCheckpoint("this-nodes-own-position");
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("existence is read before the position is, so this write is a race to compare against rather than prior history to accept in silence")
                .isInstanceOf(StartPositionAlreadyPinnedException.class)
                .hasMessageContaining("this-nodes-own-position")
                .hasMessageContaining("landed-during-registration");

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("landed-during-registration");
        assertThat(model.subscriptionIds())
                .as("a refused registration leaves its id free, so the same node can register it again")
                .doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void two_nodes_registering_at_the_same_time_at_different_positions_refuses_the_one_whose_position_was_not_stored() {
        // Two separate model instances stand for two nodes. Their own bookkeeping is independent, but they share
        // the one resource that matters here, the checkpoint storage both are about to write the same subscription
        // id in. The storage's exists() always answers false (see RaceSimulatingCheckpointStorage), standing in
        // for the race #669 describes, where both nodes read before either has written.
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        RecordingSubscriptionModel firstDelegate = new RecordingSubscriptionModel();
        RecordingSubscriptionModel secondDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> firstPositionSource = () -> new StringCheckpoint("first-nodes-position");
        GlobalCheckpointSource<@Nullable Checkpoint> secondPositionSource = () -> new StringCheckpoint("second-nodes-position");
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(firstDelegate, firstPositionSource, storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(secondDelegate, secondPositionSource, storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        // The write decides this at registration, so the second node hears about it here rather than later when
        // either node starts.
        assertThatThrownBy(() -> secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .isInstanceOf(StartPositionAlreadyPinnedException.class)
                .hasMessageContaining("first-nodes-position")
                .hasMessageContaining("second-nodes-position");

        assertThatCode(() -> firstNode.resumeSubscription(SUBSCRIPTION_ID))
                .as("the node whose position was stored is unaffected")
                .doesNotThrowAnyException();
        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("first-nodes-position");
        assertThat(secondNode.subscriptionIds()).doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void two_nodes_registering_at_the_same_time_at_the_same_position_both_complete() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        RecordingSubscriptionModel firstDelegate = new RecordingSubscriptionModel();
        RecordingSubscriptionModel secondDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> sharedPositionSource = () -> new StringCheckpoint("shared-position");
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(firstDelegate, sharedPositionSource, storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(secondDelegate, sharedPositionSource, storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThatCode(() -> secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("both nodes read the same position, so the stored one is this node's own answer arriving second and there is nothing to refuse")
                .doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("shared-position");
        assertThat(secondNode.subscriptionIds()).contains(SUBSCRIPTION_ID);
    }

    @Test
    void two_nodes_at_the_same_position_both_complete_even_when_their_checkpoints_are_of_different_kinds() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        GlobalCheckpointSource<@Nullable Checkpoint> aRecord = () -> new StringCheckpoint("shared-position");
        // Not a record, so equals() is identity here and only asString() can tell that the two nodes are at the
        // same position.
        GlobalCheckpointSource<@Nullable Checkpoint> notARecord = () -> () -> "shared-position";
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(), aRecord, storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(), notARecord, storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThatCode(() -> secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("shared-position");
    }

    @Test
    void two_nodes_at_the_same_position_both_complete_on_a_storage_that_reports_a_matching_write_as_success() {
        // The MongoDB storages tell the two outcomes apart by comparing values, which ifAbsent() allows, so the
        // second node's write is reported as success and the comparison after a refusal is never reached. Redis
        // and the in-memory storage answer on existence alone and take that other route.
        ValueComparingCheckpointStorage storage = new ValueComparingCheckpointStorage();
        GlobalCheckpointSource<@Nullable Checkpoint> sharedPositionSource = () -> new StringCheckpoint("shared-position");
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(), sharedPositionSource, storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(), sharedPositionSource, storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThatCode(() -> secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("shared-position");
        assertThat(secondNode.subscriptionIds()).contains(SUBSCRIPTION_ID);
    }

    @Test
    void a_node_at_a_different_position_is_refused_on_a_storage_that_reports_a_matching_write_as_success() {
        ValueComparingCheckpointStorage storage = new ValueComparingCheckpointStorage();
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new StringCheckpoint("first-nodes-position"), storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new StringCheckpoint("second-nodes-position"), storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThatThrownBy(() -> secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .isInstanceOf(StartPositionAlreadyPinnedException.class)
                .hasMessageContaining("first-nodes-position")
                .hasMessageContaining("second-nodes-position");

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("first-nodes-position");
    }

    @Test
    void a_refused_registration_completes_when_it_is_made_again_and_starts_from_the_position_that_was_stored() {
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        AtomicInteger reads = new AtomicInteger();
        // Another node's write arrives during the first registration only, so the second registration finds it
        // already stored, which is what a node that registers again after a refusal sees.
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            if (reads.getAndIncrement() == 0) {
                storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("the-position-that-was-stored"));
            }
            return new StringCheckpoint("this-nodes-own-position");
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);
        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).isInstanceOf(StartPositionAlreadyPinnedException.class);

        assertThatCode(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("the-position-that-was-stored");
        assertThatCode(() -> model.resumeSubscription(SUBSCRIPTION_ID)).doesNotThrowAnyException();
    }

    @Test
    void a_registration_on_an_already_running_model_is_refused_the_same_way_and_reaches_the_wrapped_model_not_at_all() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("another-nodes-position"));
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate,
                () -> new StringCheckpoint("this-nodes-own-position"), storage);
        model.start(true);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("a registration handed straight to the wrapped model is refused for the same reason as one that waits, so which of the two it is does not decide where the subscription starts")
                .isInstanceOf(StartPositionAlreadyPinnedException.class);

        assertThat(delegate.subscribeCalls).isEmpty();
        assertThat(model.subscriptionIds()).doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void a_registration_whose_write_is_delayed_past_a_later_ones_is_refused_rather_than_started_from_that_later_position() throws Exception {
        // The node that reads its position first stalls before writing, so the other node's later position
        // reaches storage first. Neither node can order the two afterwards, and starting the earlier one from the
        // later position would skip the events between them, so its registration is refused instead.
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        CountDownLatch earlierNodeReachedItsWrite = new CountDownLatch(1);
        CountDownLatch laterNodeWrote = new CountDownLatch(1);
        // Holds the earlier node inside its own save, after it has read its position, so the later node's write is
        // the one that reaches storage first.
        CheckpointStorage sharedStorage = new CheckpointStorage() {
            @Override
            public boolean evaluatesWriteConditions() {
                return true;
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                if (checkpoint.asString().equals("earlier-position")) {
                    earlierNodeReachedItsWrite.countDown();
                    awaitOrFail(laterNodeWrote);
                }
                return storage.save(subscriptionId, checkpoint, condition);
            }

            @Override
            public Checkpoint read(String subscriptionId) {
                return storage.read(subscriptionId);
            }

            @Override
            public boolean exists(String subscriptionId) {
                return storage.exists(subscriptionId);
            }

            @Override
            public OptionalLong writeVersion(String subscriptionId) {
                return storage.writeVersion(subscriptionId);
            }

            @Override
            public void delete(String subscriptionId) {
                storage.delete(subscriptionId);
            }
        };
        GlobalCheckpointSource<@Nullable Checkpoint> earlierPositionSource = () -> new StringCheckpoint("earlier-position");
        GlobalCheckpointSource<@Nullable Checkpoint> laterPositionSource = () -> new StringCheckpoint("later-position");
        ManualStartSubscriptionModel earlierNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(), earlierPositionSource, sharedStorage);
        ManualStartSubscriptionModel laterNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(), laterPositionSource, sharedStorage);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> earlierRegistration;
        try {
            earlierRegistration = pool.submit(() -> earlierNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
            }));
            awaitOrFail(earlierNodeReachedItsWrite);
            laterNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
            });
            laterNodeWrote.countDown();
            assertThatThrownBy(() -> earlierRegistration.get(10, TimeUnit.SECONDS))
                    .as("this is the one case that skips events, so the registration is refused with both positions named rather than accepted quietly")
                    .hasCauseInstanceOf(StartPositionAlreadyPinnedException.class)
                    .hasMessageContaining("earlier-position")
                    .hasMessageContaining("later-position");
        } finally {
            pool.shutdownNow();
        }

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString())
                .as("the write that reached storage first is the one that stands, whichever node read its position first")
                .isEqualTo("later-position");
        assertThat(earlierNode.subscriptionIds()).doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void two_nodes_registering_at_the_same_time_at_different_positions_both_complete_when_the_storage_can_order_them() {
        // Same shape as two_nodes_registering_at_the_same_time_at_different_positions_refuses_the_one_whose_position_was_not_stored,
        // but the storage can compare the two positions, so the second node is no longer refused: the race is
        // settled by which position is earlier, and both nodes end up agreeing on it.
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new OrderedCheckpoint(5), storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new OrderedCheckpoint(2), storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        assertThatCode(() -> secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("the storage resolves the race by position, so the second node's earlier position is adopted instead of being refused")
                .doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString())
                .as("the earlier of the two positions governs, whichever node wrote first")
                .isEqualTo("order-2");
        assertThat(secondNode.subscriptionIds()).contains(SUBSCRIPTION_ID);
    }

    @Test
    void a_registration_whose_write_is_delayed_past_a_later_ones_adopts_the_earlier_position_when_the_storage_can_order_them() throws Exception {
        // Same shape as a_registration_whose_write_is_delayed_past_a_later_ones_is_refused_rather_than_started_from_that_later_position,
        // but the storage can compare the two positions, so the delay no longer decides the outcome: the earlier
        // position wins over the later one that reached storage first, instead of being refused for having lost the
        // write.
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        CountDownLatch earlierNodeReachedItsWrite = new CountDownLatch(1);
        CountDownLatch laterNodeWrote = new CountDownLatch(1);
        CheckpointStorage sharedStorage = new CheckpointStorage() {
            @Override
            public boolean evaluatesWriteConditions() {
                return true;
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                if (checkpoint.asString().equals("order-2")) {
                    earlierNodeReachedItsWrite.countDown();
                    awaitOrFail(laterNodeWrote);
                }
                return storage.save(subscriptionId, checkpoint, condition);
            }

            @Override
            public Checkpoint read(String subscriptionId) {
                return storage.read(subscriptionId);
            }

            @Override
            public boolean exists(String subscriptionId) {
                return storage.exists(subscriptionId);
            }

            @Override
            public OptionalLong writeVersion(String subscriptionId) {
                return storage.writeVersion(subscriptionId);
            }

            @Override
            public void delete(String subscriptionId) {
                storage.delete(subscriptionId);
            }

            @Override
            public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
                return storage.resolveFirstCheckpointRace(subscriptionId, candidate);
            }
        };
        ManualStartSubscriptionModel earlierNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new OrderedCheckpoint(2), sharedStorage);
        ManualStartSubscriptionModel laterNode = ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new OrderedCheckpoint(9), sharedStorage);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> earlierRegistration = pool.submit(() -> earlierNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
            }));
            awaitOrFail(earlierNodeReachedItsWrite);
            laterNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
            });
            laterNodeWrote.countDown();
            assertThatCode(() -> earlierRegistration.get(10, TimeUnit.SECONDS))
                    .as("ordering settles this without skipping anything, so both registrations complete")
                    .doesNotThrowAnyException();
        } finally {
            pool.shutdownNow();
        }

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString())
                .as("the earlier position governs regardless of which write reached storage first")
                .isEqualTo("order-2");
        assertThat(earlierNode.subscriptionIds()).contains(SUBSCRIPTION_ID);
    }

    @Test
    void a_checkpoint_deleted_and_rewritten_to_a_later_position_during_registration_is_resolved_by_order_not_taken_on_presence_alone() {
        // #771's second hole: checkpointAlreadyExisted only tells this class that something was stored before the
        // position was captured, not that it is still the same something by the time the write runs.
        // cancelSubscription deletes a checkpoint, so a delete followed by another node's registration is reachable
        // here, and a storage able to order the two positions is what tells apart "safe to leave alone" from "this
        // one raced in and is later than what this node captured", instead of trusting presence alone.
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        storage.checkpoints.put(SUBSCRIPTION_ID, new OrderedCheckpoint(3));
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        // Simulates another node cancelling and re-registering the same subscription, with a later position, while
        // this registration is between its own existence check and its own position capture.
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            storage.checkpoints.put(SUBSCRIPTION_ID, new OrderedCheckpoint(50));
            return new OrderedCheckpoint(10);
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        assertThatCode(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("a storage able to order the two positions resolves this instead of accepting whatever is present")
                .doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString())
                .as("this node's earlier position replaces the later one that appeared during registration, so nothing between them is skipped")
                .isEqualTo("order-10");
    }

    @Test
    void a_checkpoint_deleted_and_rewritten_to_an_earlier_position_during_registration_is_left_alone_when_the_storage_can_order_them() {
        // The mirror image of the test above: the checkpoint that appears during registration is earlier than this
        // node's own position, which is the ordinary "another node is already ahead" case, so it is left alone
        // exactly as presence-based acceptance already would have left it, only now confirmed rather than assumed.
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        storage.checkpoints.put(SUBSCRIPTION_ID, new OrderedCheckpoint(3));
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            storage.checkpoints.put(SUBSCRIPTION_ID, new OrderedCheckpoint(7));
            return new OrderedCheckpoint(40);
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        assertThatCode(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        })).doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString())
                .as("the stored position is already earlier than this node's own, so it stays exactly as it was")
                .isEqualTo("order-7");
    }

    @Test
    void a_registration_is_refused_when_the_position_that_was_stored_cannot_be_read_back() {
        // Nothing here can show that the stored position is the one this registration read, so it is refused for
        // the same reason a differing position is. The failure that stopped it from being read is the cause.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        CheckpointStorage unreadableAfterRefusal = refusingStorage(() -> {
            throw new IllegalStateException("the checkpoint store is unreachable");
        });
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate,
                () -> new StringCheckpoint("this-nodes-position"), unreadableAfterRefusal);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .isInstanceOf(StartPositionAlreadyPinnedException.class)
                .hasMessageContaining("this-nodes-position")
                .hasMessageContaining("failed")
                .hasCauseInstanceOf(IllegalStateException.class);

        assertThat(model.isPaused(SUBSCRIPTION_ID))
                .as("nothing is registered, so there is no withheld subscription to start")
                .isFalse();
    }

    @Test
    void a_stored_position_that_reads_back_as_nothing_is_refused_without_being_named_as_null() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        CheckpointStorage emptyAfterRefusal = refusingStorage(() -> null);
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate,
                () -> new StringCheckpoint("this-nodes-position"), emptyAfterRefusal);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }))
                .as("a read that finds nothing does not show the checkpoint was removed, since a read served from behind the write answers the same way")
                .isInstanceOf(StartPositionAlreadyPinnedException.class)
                .hasMessageContaining("found nothing")
                .hasMessageNotContaining("null")
                .hasNoCause();
    }

    @Test
    void an_earlier_registrant_always_wins_the_pin_even_when_a_later_registrant_starts_first() {
        // Stands for a rolling deploy where one node registers, the source position moves on, and a second node
        // registers the same subscription id minutes later. A duplicate id on the same model would throw, so a
        // second model instance stands for the second node, sharing the storage the way the racing test above does.
        // The second registration finds the first one's pin already stored, which is the ordinary case a checkpoint
        // storage read here always finds for a subscription somebody already registered, not a race to log.
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        RecordingSubscriptionModel earlierDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> earlierPositionSource = () -> new StringCheckpoint("earlier-registration-position");
        ManualStartSubscriptionModel earlierNode = ManualStartSubscriptionModel.stoppedByDefault(earlierDelegate, earlierPositionSource, storage);
        earlierNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        RecordingSubscriptionModel laterDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> laterPositionSource = () -> new StringCheckpoint("later-registration-position");
        ManualStartSubscriptionModel laterNode = ManualStartSubscriptionModel.stoppedByDefault(laterDelegate, laterPositionSource, storage);
        laterNode.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });

        // The later registrant starts first, but the pin was already decided at registration, so this changes
        // nothing about which position the subscription resumes from.
        assertThatCode(() -> laterNode.resumeSubscription(SUBSCRIPTION_ID)).doesNotThrowAnyException();
        assertThatCode(() -> earlierNode.resumeSubscription(SUBSCRIPTION_ID)).doesNotThrowAnyException();

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("earlier-registration-position");
    }

    @Test
    void a_subscription_registered_on_a_running_model_still_pins_its_registration_position_before_starting_live() {
        // The pin write happens unconditionally, before this model knows whether the registration will be deferred
        // or handed straight to the wrapped model, because the pin must land before any resumeSubscription can
        // possibly race ahead of it. On an already-running model that means a subscription which was never
        // deferred still gets pinned. This proves that costs nothing observable. The wrapped model still receives
        // exactly the caller's own startAt, immediately, exactly as it did before this pin existed.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> new StringCheckpoint("live-registration-position");
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);
        model.start(true);
        StartAt callersOwnStartAt = StartAt.subscriptionModelDefault();

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, callersOwnStartAt, __ -> {
        });

        assertThat(delegate.subscribeCalls).hasSize(1);
        SubscribeCall call = delegate.subscribeCalls.getFirst();
        assertThat(call.startAt()).isSameAs(callersOwnStartAt);
        assertThat(subscription).isSameAs(delegate.subscriptions.get(SUBSCRIPTION_ID));
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("live-registration-position");
    }

    @Test
    void a_registration_that_starts_at_now_writes_no_checkpoint() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(storage.checkpoints)
                .as("nothing below reads a stored checkpoint for this subscription, so a write here would only take the id from a node that does")
                .isEmpty();
    }

    @Test
    void a_registration_that_names_its_own_checkpoint_writes_nothing() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringCheckpoint("chosen-by-the-caller")), __ -> {
        });

        assertThat(storage.checkpoints).isEmpty();
    }

    @Test
    void a_first_run_asking_to_replay_from_the_beginning_still_replays_instead_of_resuming() {
        // The start position the Spring Boot starter builds for StartPosition.BEGINNING. It answers the first-run
        // question by asking the same checkpoint storage this model writes to, so a position recorded before it is
        // resolved would turn the replay the caller asked for into a resume from the moment of registration.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        StartAt replayFromTheBeginning = StartAt.checkpoint(new StringCheckpoint("beginning"));
        StartAt beginningThenResume = StartAt.dynamic(() -> storage.exists(SUBSCRIPTION_ID)
                ? StartAt.subscriptionModelDefault()
                : replayFromTheBeginning);
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, beginningThenResume, __ -> {
        });
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints).isEmpty();
        StartAt startAtTheWrappedModelGot = delegate.subscribeCalls.getFirst().startAt();
        assertThat(startAtTheWrappedModelGot.get(new SubscriptionModelContext(RecordingSubscriptionModel.class)))
                .as("the storage is still empty when the wrapped model resolves the same function, so it replays")
                .isSameAs(replayFromTheBeginning);
    }

    @Test
    void a_dynamic_start_position_that_resolves_to_the_model_default_is_pinned_like_a_plain_one() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        // Branching on the model type as well, since that is what the functions the Spring Boot starter builds do,
        // and it is the wrapped model this one has to name when it resolves.
        StartAt defaultForTheWrappedModel = StartAt.dynamic(context ->
                context.hasSubscriptionModelType(RecordingSubscriptionModel.class) ? StartAt.subscriptionModelDefault() : StartAt.now());
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, defaultForTheWrappedModel, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_dynamic_start_position_asking_for_the_model_type_by_equality_is_pinned_for_a_subclassed_model() {
        // A model resolves the position against its own class literal, so a subclass or a Spring proxy of it would be
        // asked about here under a name hasSubscriptionModelType does not match. Answering that with anything but the
        // model default records nothing, and this delegate then records a position when the subscription starts
        // instead, which is the skip this write exists to prevent.
        SubclassedSubscriptionModel delegate = new SubclassedSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(RecordingSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_dynamic_start_position_naming_only_the_model_it_wants_is_pinned_for_a_subclassed_model() {
        // The same equality check the other way round, answering with nothing for every model but the one it names.
        // Nothing to descend to here, so the classes the delegate inherits from are what is left to ask.
        SubclassedSubscriptionModel delegate = new SubclassedSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(RecordingSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : null);
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_dynamic_start_position_that_resolves_to_nothing_is_not_pinned() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> null), __ -> {
        });

        assertThat(storage.checkpoints)
                .as("answering with nothing tells the wrapped model to leave this subscription to the model below it, which reads no checkpoint either")
                .isEmpty();
    }

    @Test
    void the_wrapped_model_receives_the_start_position_the_caller_passed_and_it_is_resolved_once_here() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        AtomicInteger resolutions = new AtomicInteger();
        StartAt callersOwnStartAt = StartAt.dynamic(() -> {
            resolutions.incrementAndGet();
            return StartAt.subscriptionModelDefault();
        });
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);
        model.start(true);

        model.subscribe(SUBSCRIPTION_ID, null, callersOwnStartAt, __ -> {
        });

        assertThat(delegate.subscribeCalls.getFirst().startAt()).isSameAs(callersOwnStartAt);
        assertThat(resolutions.get())
                .as("one resolution per layer asked, and this delegate wraps nothing and answers on the first one")
                .isEqualTo(1);
        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_start_position_that_answers_with_nothing_for_the_outer_model_is_recorded_from_the_layer_below() {
        // The shape the annotations build for the default start position, which answers with nothing for a catch-up
        // layer and with the model default for everything else. A stack without a competing consumer above the
        // catch-up model puts that layer outermost, and stopping there would read the registration as having no
        // position to record, leaving the durable model below to record one when the subscription starts.
        RecordingSubscriptionModel checkpointReadingModel = new RecordingSubscriptionModel();
        ReplayingSubscriptionModel replayingModel = new ReplayingSubscriptionModel(checkpointReadingModel);
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        checkpointReadingModel.globalCheckpoint = new StringCheckpoint("at-registration");
        AtomicInteger resolutions = new AtomicInteger();
        StartAt startAt = StartAt.dynamic(context -> {
            resolutions.incrementAndGet();
            return context.hasSubscriptionModelType(ReplayingSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault();
        });
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(replayingModel, checkpointReadingModel, storage);

        model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
        assertThat(resolutions.get())
                .as("asked for the outer model, which answered with nothing, and then for the one it wraps")
                .isEqualTo(2);
    }

    @Test
    void a_layer_that_decides_something_other_than_the_start_leaves_the_answer_to_the_model_below() {
        // The Spring Boot starter's own stack, where the competing consumer layer resolves the position to work out
        // whether to compete and leaves where the subscription starts to the durable model below it. Every start
        // position Occurrent itself builds is safe from this. Reaching the skip below takes a function written by
        // hand, and the StartAt here is one. Stopping at the competing consumer's answer records nothing, and the
        // durable model then records a position when the subscription starts, minutes later on a rolling deploy,
        // skipping everything written in between.
        RecordingSubscriptionModel checkpointReadingModel = new RecordingSubscriptionModel();
        DeferringSubscriptionModel deferringModel = new DeferringSubscriptionModel(checkpointReadingModel);
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        checkpointReadingModel.globalCheckpoint = new StringCheckpoint("at-registration");
        List<Class<?>> asked = new CopyOnWriteArrayList<>();
        StartAt startAt = StartAt.dynamic(context -> {
            asked.add(context.subscriptionModelType());
            return context.hasSubscriptionModelType(RecordingSubscriptionModel.class) ? StartAt.subscriptionModelDefault() : StartAt.now();
        });
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(deferringModel, checkpointReadingModel, storage);

        model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
        assertThat(asked)
                .as("nothing the deferring layer answers changes where the subscription starts, so it is not asked")
                .containsExactly(RecordingSubscriptionModel.class);
    }

    @Test
    void a_layer_that_decides_something_other_than_the_start_records_no_position_of_its_own() {
        RecordingSubscriptionModel checkpointReadingModel = new RecordingSubscriptionModel();
        DeferringSubscriptionModel deferringModel = new DeferringSubscriptionModel(checkpointReadingModel);
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        checkpointReadingModel.globalCheckpoint = new StringCheckpoint("at-registration");
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(DeferringSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(deferringModel, checkpointReadingModel, storage);

        model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints)
                .as("the model that does read a checkpoint asked to start at now, and a position written here is one "
                        + "nothing starts from over a subscription the caller asked to replay")
                .isEmpty();
    }

    @Test
    void a_registration_below_a_deferring_layer_that_answers_with_nothing_is_still_recorded() {
        // Two layers to descend through, one that leaves the start to the model below and one that answers with
        // nothing, which is the shape a hand-wired stack has when the catch-up layer sits under the competing
        // consumer one.
        RecordingSubscriptionModel checkpointReadingModel = new RecordingSubscriptionModel();
        ReplayingSubscriptionModel replayingModel = new ReplayingSubscriptionModel(checkpointReadingModel);
        DeferringSubscriptionModel deferringModel = new DeferringSubscriptionModel(replayingModel);
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        checkpointReadingModel.globalCheckpoint = new StringCheckpoint("at-registration");
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(ReplayingSubscriptionModel.class)
                ? null : StartAt.subscriptionModelDefault());
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(deferringModel, checkpointReadingModel, storage);

        model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_registration_under_another_model_of_this_kind_is_recorded_from_the_model_below_both_of_them() {
        // This model leaves the start to the model below too, so it says so and the outer one of two passes over it.
        RecordingSubscriptionModel checkpointReadingModel = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        checkpointReadingModel.globalCheckpoint = new StringCheckpoint("at-registration");
        ManualStartSubscriptionModel inner = ManualStartSubscriptionModel.stoppedByDefault(checkpointReadingModel);
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(RecordingSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());
        ManualStartSubscriptionModel outer = ManualStartSubscriptionModel.stoppedByDefault(inner, checkpointReadingModel, storage);

        outer.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_checkpoint_storage_that_cannot_evaluate_write_conditions_is_refused() {
        UnconditionalCheckpointStorage storage = new UnconditionalCheckpointStorage();

        assertThatThrownBy(() -> ManualStartSubscriptionModel.stoppedByDefault(new RecordingSubscriptionModel(),
                () -> new StringCheckpoint("at-registration"), storage))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(UnconditionalCheckpointStorage.class.getName());
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
    }

    // Nothing is stored as far as exists() can tell, and the conditional write is refused anyway, which is the state
    // a node is left in when another registration won the position between its own check and its own write.
    // readsBack decides what there is to read by the time it looks.
    private static CheckpointStorage refusingStorage(Supplier<@Nullable Checkpoint> readsBack) {
        return new CheckpointStorage() {
            @Override
            public boolean evaluatesWriteConditions() {
                return true;
            }

            @Override
            public Checkpoint read(String subscriptionId) {
                return readsBack.get();
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }

            @Override
            public OptionalLong writeVersion(String subscriptionId) {
                return OptionalLong.empty();
            }

            @Override
            public void delete(String subscriptionId) {
            }

            @Override
            public boolean exists(String subscriptionId) {
                return false;
            }
        };
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private record SubscribeCall(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                 Consumer<CloudEvent> action) {
    }

    private record StringCheckpoint(String value) implements Checkpoint {
        @Override
        public String asString() {
            return value;
        }
    }

    // Records what it is asked to do rather than doing anything, since these tests are about which calls reach the
    // wrapped model and when. parkOnSubscribe stands in for a model whose feed is stopped, which registers a paused
    // subscription instead of a running one.
    private static class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions {
        final List<SubscribeCall> subscribeCalls = new CopyOnWriteArrayList<>();
        final Map<String, Subscription> subscriptions = new HashMap<>();
        final List<String> resumeCalls = new CopyOnWriteArrayList<>();
        final List<String> cancelCalls = new CopyOnWriteArrayList<>();
        final Set<String> paused = new LinkedHashSet<>();
        final List<Consumer<CloudEvent>> actions = new CopyOnWriteArrayList<>();

        boolean parkOnSubscribe = false;
        boolean failNextSubscribe = false;
        @Nullable CountDownLatch subscribeEntered = null;
        @Nullable CountDownLatch holdSubscribeUntil = null;
        @Nullable String failSubscribeFor = null;
        @Nullable Checkpoint globalCheckpoint = null;
        boolean running = true;
        int stopCalls = 0;
        int shutdownCalls = 0;

        void feed(CloudEvent cloudEvent) {
            actions.forEach(action -> action.accept(cloudEvent));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (subscribeEntered != null) {
                subscribeEntered.countDown();
            }
            if (holdSubscribeUntil != null) {
                awaitOrFail(holdSubscribeUntil);
            }
            if (failNextSubscribe) {
                failNextSubscribe = false;
                throw new IllegalStateException("Cannot subscribe right now");
            }
            if (subscriptionId.equals(failSubscribeFor)) {
                throw new IllegalStateException("Cannot subscribe " + subscriptionId);
            }
            subscribeCalls.add(new SubscribeCall(subscriptionId, filter, startAt, action));
            Subscription subscription = new RecordedSubscription(subscriptionId);
            subscriptions.put(subscriptionId, subscription);
            if (parkOnSubscribe) {
                paused.add(subscriptionId);
            } else {
                actions.add(action);
            }
            return subscription;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (!paused.remove(subscriptionId)) {
                throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
            }
            resumeCalls.add(subscriptionId);
            // Every real model reports itself running again after a resume rather than limiting the resume to that
            // one subscription.
            running = true;
            subscribeCalls.stream().filter(call -> call.subscriptionId().equals(subscriptionId)).forEach(call -> actions.add(call.action()));
            return subscriptions.get(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            cancelCalls.add(subscriptionId);
            subscriptions.remove(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public void stop() {
            stopCalls++;
            running = false;
            // Stopping pauses what was running rather than cancelling it, so a single subscription can be resumed
            // afterwards.
            paused.addAll(subscriptions.keySet());
            actions.clear();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                Set.copyOf(paused).forEach(this::resumeSubscription);
            }
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.containsKey(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Set<String> subscriptionIds() {
            return Set.copyOf(subscriptions.keySet());
        }

        @Override
        public void shutdown() {
            shutdownCalls++;
        }
    }

    // Stands for a model a caller has subclassed, or that Spring has handed back as a proxy, either of which shows up
    // under a class the model itself never names when it resolves a start position.
    private static final class SubclassedSubscriptionModel extends RecordingSubscriptionModel {
    }

    // Stands for the competing consumer layer, which resolves the position to work out whether to compete and leaves
    // where the subscription starts to the model below. A plain class rather than a record, so the classes it
    // inherits from cannot answer anything the walk itself did not.
    private static final class DeferringSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper {
        private final SubscriptionModel wrapped;

        DeferringSubscriptionModel(SubscriptionModel wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean decidesWhereTheSubscriptionStarts() {
            return false;
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return wrapped;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return wrapped.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return wrapped.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            wrapped.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            wrapped.cancelSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            wrapped.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            wrapped.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return wrapped.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return wrapped.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return wrapped.isPaused(subscriptionId);
        }

        @Override
        public void shutdown() {
            wrapped.shutdown();
        }
    }

    // Stands for a layer that replays history of its own, the position the wrapped model below it reads a checkpoint
    // for. Everything is passed straight down, since only its type matters to these tests.
    private record ReplayingSubscriptionModel(SubscriptionModel wrapped) implements SubscriptionModel, SubscriptionModelWrapper {

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return wrapped;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return wrapped.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return wrapped.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            wrapped.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            wrapped.cancelSubscription(subscriptionId);
        }

        @Override
        public void stop() {
            wrapped.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            wrapped.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return wrapped.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return wrapped.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return wrapped.isPaused(subscriptionId);
        }

        @Override
        public void shutdown() {
            wrapped.shutdown();
        }
    }

    private record RecordedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    // Records what it is asked to save rather than being a full checkpoint store, except for ifAbsent(), which it
    // evaluates for real. Every test in this file that pins a position while one is already stored relies on that
    // refusal, the same way the real storages the production code runs against do.
    private static final class RecordingCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();
        final Map<String, CheckpointWriteCondition> conditions = new HashMap<>();

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (condition instanceof CheckpointWriteCondition.IfAbsent && checkpoints.containsKey(subscriptionId)) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }
            checkpoints.put(subscriptionId, checkpoint);
            conditions.put(subscriptionId, condition);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }

    // This one ignores the condition and writes anyway, and it leaves evaluatesWriteConditions() at its default of
    // false, which is the only thing the factory asks about. Another storage answering false may refuse the write
    // instead.
    private static final class UnconditionalCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }

    // Reports an ifAbsent() write of the value already stored as success rather than refusing it, which
    // CheckpointWriteCondition allows and the MongoDB storages do. exists() answers false the way the racing
    // storage below does, so a registration reaches the write with nothing stored as far as it knows.
    private static final class ValueComparingCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            Checkpoint stored = checkpoints.get(subscriptionId);
            if (condition instanceof CheckpointWriteCondition.IfAbsent && stored != null
                && !stored.asString().equals(checkpoint.asString())) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return false;
        }
    }

    // exists() always answers false, whatever is already stored. That is what two nodes racing to pin the same
    // subscription id would each see from their own read, whichever real wall-clock order the two calls land in, so
    // this makes the race in #669 deterministic instead of depending on winning an actual thread scheduling race.
    // save() evaluates ifAbsent() for real, the way every checkpoint storage in this repository does.
    private static final class RaceSimulatingCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (condition instanceof CheckpointWriteCondition.IfAbsent && checkpoints.containsKey(subscriptionId)) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return false;
        }
    }

    // Compares by an integer order instead of a real position, standing in for what NativeMongoCheckpointStorage
    // does by comparing MongoOperationTimeCheckpoint's operationTime. exists() and save() behave exactly like
    // RecordingCheckpointStorage, so every test that reaches resolveFirstCheckpointRace does so the same way the
    // production code does, through a real ifAbsent() refusal. resolveFirstCheckpointRace answers empty for a
    // checkpoint that is not an OrderedCheckpoint, mirroring the real storage's answer for one that is not a
    // MongoOperationTimeCheckpoint, which is what tells apart a race this fixture can resolve from a stored
    // checkpoint from real delivery that it cannot.
    private static final class OrderAwareCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (condition instanceof CheckpointWriteCondition.IfAbsent && checkpoints.containsKey(subscriptionId)) {
                throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
            }
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            if (!(candidate instanceof OrderedCheckpoint candidateOrdered)) {
                return Optional.empty();
            }
            Checkpoint stored = checkpoints.get(subscriptionId);
            if (stored == null) {
                checkpoints.put(subscriptionId, candidate);
                return Optional.of(candidate);
            }
            if (!(stored instanceof OrderedCheckpoint storedOrdered)) {
                return Optional.empty();
            }
            if (storedOrdered.order() > candidateOrdered.order()) {
                checkpoints.put(subscriptionId, candidate);
                return Optional.of(candidate);
            }
            return Optional.of(stored);
        }
    }

    private record OrderedCheckpoint(int order) implements Checkpoint {
        @Override
        public String asString() {
            return "order-" + order;
        }
    }
}
