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
import org.occurrent.subscription.GlobalCheckpointSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

        assertThat(IntrospectableSubscriptionModel.of(model)).containsSame(model);
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
    void nothing_is_written_to_checkpoint_storage_until_a_subscription_is_started() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });

        assertThat(storage.checkpoints).isEmpty();
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
    void a_subscription_that_has_run_before_keeps_its_own_checkpoint() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        RecordingCheckpointStorage storage = new RecordingCheckpointStorage();
        storage.checkpoints.put(SUBSCRIPTION_ID, new StringCheckpoint("from-a-previous-run"));
        ManualStartSubscriptionModel model = ManualStartSubscriptionModel.stoppedByDefault(delegate, delegate, storage);
        delegate.globalCheckpoint = new StringCheckpoint("at-registration");

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), __ -> {
        });
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.checkpoints.get(SUBSCRIPTION_ID).asString()).isEqualTo("from-a-previous-run");
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
    private static final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, IntrospectableSubscriptionModel {
        final List<SubscribeCall> subscribeCalls = new CopyOnWriteArrayList<>();
        final Map<String, Subscription> subscriptions = new HashMap<>();
        final List<String> resumeCalls = new CopyOnWriteArrayList<>();
        final List<String> cancelCalls = new CopyOnWriteArrayList<>();
        final Set<String> paused = new LinkedHashSet<>();
        final List<Consumer<CloudEvent>> actions = new CopyOnWriteArrayList<>();

        boolean parkOnSubscribe = false;
        boolean failNextSubscribe = false;
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

    private static final class RecordingCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> checkpoints = new HashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
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
}
