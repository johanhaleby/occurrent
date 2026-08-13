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
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

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

    // System.getLogger(ManualStartSubscriptionModel.class.getName()) backs onto java.util.logging absent another
    // platform logging bridge, which is what these tests assert against.
    private static final class CapturingLogHandler extends Handler {
        private final List<LogRecord> records = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        static CapturingLogHandler attached() {
            CapturingLogHandler handler = new CapturingLogHandler();
            java.util.logging.Logger.getLogger(ManualStartSubscriptionModel.class.getName()).addHandler(handler);
            return handler;
        }

        void detach() {
            java.util.logging.Logger.getLogger(ManualStartSubscriptionModel.class.getName()).removeHandler(this);
        }

        List<LogRecord> records() {
            return records;
        }
    }

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
    void the_start_position_is_never_resolved_while_a_subscription_is_only_registered() {
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
            return null;
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> registering = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
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
            Future<Subscription> registering = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            }));
            assertThat(registrationInProgress.await(10, TimeUnit.SECONDS))
                    .as("the position is read on every registration, whatever the state looked like on the way in")
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

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(storage.conditions.get(SUBSCRIPTION_ID)).isEqualTo(CheckpointWriteCondition.ifAbsent());
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

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_first_run_starts_from_where_the_subscription_was_registered_rather_than_where_it_was_started() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        delegate.globalCheckpoint = new StringCheckpoint("much-later");
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_subscription_that_has_run_before_keeps_its_own_checkpoint_without_logging_anything() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("from-a-previous-run"));
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");

        CapturingLogHandler logHandler = CapturingLogHandler.attached();
        try {
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            });
        } finally {
            logHandler.detach();
        }
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("from-a-previous-run");
        assertThat(logHandler.records())
                .as("a checkpoint from a previous run winning is the ordinary case, not one worth logging")
                .isEmpty();
    }

    @Test
    void a_checkpoint_written_between_the_existence_check_and_capturing_the_position_is_treated_as_a_race_not_prior_history() {
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        // Writes another node's pin as a side effect of answering the position query, standing for that node's
        // write landing between this node's existence check and its own position capture, both of which now
        // happen inside subscribe() itself.
        GlobalCheckpointSource<@Nullable Checkpoint> positionSource = () -> {
            storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("landed-during-registration"));
            return new StringCheckpoint("this-nodes-own-position");
        };
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, positionSource, storage);

        CapturingLogHandler logHandler = CapturingLogHandler.attached();
        try {
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            });
        } finally {
            logHandler.detach();
        }

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("landed-during-registration");
        assertThat(logHandler.records())
                .as("existence is checked before the position is captured, so this write is a race to compare against rather than prior history to accept silently")
                .anySatisfy(record -> assertThat(record.getMessage())
                        .contains("this-nodes-own-position", "landed-during-registration"));
    }

    @Test
    void two_nodes_registering_at_the_same_time_at_different_positions_logs_the_second_nodes_loss_instead_of_leaving_it_silent() {
        // Two separate model instances stand for two nodes. Their own bookkeeping is independent, but they share the
        // one resource that matters here: the checkpoint storage both are about to pin the same subscription id in.
        // The storage's exists() always answers false (see RaceSimulatingCheckpointStorage), standing in for the
        // race #669 describes, where both nodes' reads land before either has written.
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        RecordingSubscriptionModel firstDelegate = new RecordingSubscriptionModel();
        RecordingSubscriptionModel secondDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> firstPositionSource = () -> new StringCheckpoint("first-nodes-position");
        GlobalCheckpointSource<@Nullable Checkpoint> secondPositionSource = () -> new StringCheckpoint("second-nodes-position");
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(firstDelegate, firstPositionSource, storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(secondDelegate, secondPositionSource, storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        // The pin happens at registration now, so the second node's loss is decided and logged here, not later
        // when either node actually starts.
        CapturingLogHandler logHandler = CapturingLogHandler.attached();
        try {
            secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            });
        } finally {
            logHandler.detach();
        }

        assertThatCode(() -> firstNode.resumeSubscription(SUBSCRIPTION_ID)).doesNotThrowAnyException();
        assertThatCode(() -> secondNode.resumeSubscription(SUBSCRIPTION_ID)).doesNotThrowAnyException();
        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("first-nodes-position");
        assertThat(logHandler.records())
                .anySatisfy(record -> assertThat(record.getMessage())
                        .contains("first-nodes-position", "second-nodes-position"));
    }

    @Test
    void two_nodes_registering_at_the_same_time_at_the_same_position_logs_nothing() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        RecordingSubscriptionModel firstDelegate = new RecordingSubscriptionModel();
        RecordingSubscriptionModel secondDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> sharedPositionSource = () -> new StringCheckpoint("shared-position");
        ManualStartSubscriptionModel firstNode = ManualStartSubscriptionModel.stoppedByDefault(firstDelegate, sharedPositionSource, storage);
        ManualStartSubscriptionModel secondNode = ManualStartSubscriptionModel.stoppedByDefault(secondDelegate, sharedPositionSource, storage);
        firstNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        CapturingLogHandler logHandler = CapturingLogHandler.attached();
        try {
            secondNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            });
        } finally {
            logHandler.detach();
        }

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("shared-position");
        assertThat(logHandler.records())
                .as("the two nodes captured the same position, so accepting the stored one is exactly this node's own write arriving second")
                .isEmpty();
    }

    @Test
    void a_registration_whose_write_is_delayed_past_a_later_ones_starts_from_that_later_position_and_warns() throws Exception {
        // The node that captures first stalls before writing, so the other node's later position reaches storage
        // first and wins. Neither node can order the two positions afterwards, so the earlier one starts from the
        // later position and the events between the two never reach the subscription.
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        CountDownLatch earlierNodeReachedItsWrite = new CountDownLatch(1);
        CountDownLatch laterNodeWrote = new CountDownLatch(1);
        // Holds the earlier node inside its own save, after it has captured its position, so the later node's write
        // is the one that reaches storage first.
        CheckpointStorage sharedStorage = new CheckpointStorage() {
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

        CapturingLogHandler logHandler = CapturingLogHandler.attached();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> earlierRegistration = pool.submit(() -> earlierNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            }));
            awaitOrFail(earlierNodeReachedItsWrite);
            laterNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
            });
            laterNodeWrote.countDown();
            earlierRegistration.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
            logHandler.detach();
        }

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString())
                .as("the write that reached storage first wins, whichever node captured its position first")
                .isEqualTo("later-position");
        assertThat(logHandler.records())
                .as("this is the one case that skips events, so it is named at WARNING with both positions rather than accepted quietly")
                .anySatisfy(record -> {
                    assertThat(record.getLevel()).isEqualTo(java.util.logging.Level.WARNING);
                    assertThat(record.getMessage()).contains("earlier-position", "later-position");
                });
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
        earlierNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        RecordingSubscriptionModel laterDelegate = new RecordingSubscriptionModel();
        GlobalCheckpointSource<@Nullable Checkpoint> laterPositionSource = () -> new StringCheckpoint("later-registration-position");
        ManualStartSubscriptionModel laterNode = ManualStartSubscriptionModel.stoppedByDefault(laterDelegate, laterPositionSource, storage);
        laterNode.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
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
        StartAt callersOwnStartAt = StartAt.now();

        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, callersOwnStartAt, __ -> {
        });

        assertThat(delegate.subscribeCalls).hasSize(1);
        SubscribeCall call = delegate.subscribeCalls.getFirst();
        assertThat(call.startAt()).isSameAs(callersOwnStartAt);
        assertThat(subscription).isSameAs(delegate.subscriptions.get(SUBSCRIPTION_ID));
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("live-registration-position");
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
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
    private static final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptions {
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

    // exists() always answers false, whatever is already stored. That is what two nodes racing to pin the same
    // subscription id would each see from their own read, whichever real wall-clock order the two calls land in, so
    // this makes the race in #669 deterministic instead of depending on winning an actual thread scheduling race.
    // save() evaluates ifAbsent() for real, the way every checkpoint storage in this repository does.
    private static final class RaceSimulatingCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

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
}
