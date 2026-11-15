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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A catch-up calls the two-argument globalCheckpoint and canResumeFrom, so a subclass of {@link DurableSubscriptionModel}
 * that overrides only the one-argument method is still called, and the stillWanted of the two-argument call reaches the
 * wrapped model when that override calls super.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelSubclassOverrideTest {

    private static final Checkpoint WRAPPED_ANSWER = new StringBasedCheckpoint("wrapped");
    private static final Checkpoint OVERRIDE_ANSWER = new StringBasedCheckpoint("override");

    private final RecordingLiveModel wrapped = new RecordingLiveModel();

    @Test
    void a_two_argument_canResumeFrom_calls_a_subclass_that_overrides_only_the_one_argument_method_and_returns_its_answer() {
        // Given
        List<Checkpoint> asked = new CopyOnWriteArrayList<>();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage()) {
            @Override
            public boolean canResumeFrom(Checkpoint checkpoint) {
                asked.add(checkpoint);
                return false;
            }
        };
        Checkpoint checkpoint = new StringBasedCheckpoint("asked");

        // When
        boolean canResume = model.canResumeFrom(checkpoint, () -> true);

        // Then
        assertThat(canResume).as("answer").isFalse();
        assertThat(asked).as("checkpoints the override was asked about").containsExactly(checkpoint);
        assertThat(wrapped.calls).as("calls the wrapped model got").isEmpty();
    }

    @Test
    void a_two_argument_globalCheckpoint_calls_a_subclass_that_overrides_only_the_one_argument_method_and_returns_its_answer() {
        // Given
        List<String> asked = new CopyOnWriteArrayList<>();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage()) {
            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                asked.add("globalCheckpoint");
                return OVERRIDE_ANSWER;
            }
        };

        // When
        Checkpoint checkpoint = model.globalCheckpoint(() -> true);

        // Then
        assertThat(checkpoint).as("answer").isEqualTo(OVERRIDE_ANSWER);
        assertThat(asked).as("calls the override got").hasSize(1);
        assertThat(wrapped.calls).as("calls the wrapped model got").isEmpty();
    }

    @Test
    void the_stillWanted_of_a_two_argument_canResumeFrom_reaches_the_wrapped_model_when_the_override_calls_super() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage()) {
            @Override
            public boolean canResumeFrom(Checkpoint checkpoint) {
                return super.canResumeFrom(checkpoint);
            }
        };
        BooleanSupplier stillWanted = () -> true;
        wrapped.canResume = false;

        // When
        boolean canResume = model.canResumeFrom(WRAPPED_ANSWER, stillWanted);

        // Then
        assertThat(canResume).as("answer of the wrapped model").isFalse();
        assertThat(wrapped.calls).as("calls the wrapped model got").containsExactly(new Call("canResumeFrom(Checkpoint, BooleanSupplier)", stillWanted));
    }

    @Test
    void the_stillWanted_of_a_two_argument_globalCheckpoint_reaches_the_wrapped_model_when_the_override_calls_super() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage()) {
            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                return super.globalCheckpoint();
            }
        };
        BooleanSupplier stillWanted = () -> true;

        // When
        Checkpoint checkpoint = model.globalCheckpoint(stillWanted);

        // Then
        assertThat(checkpoint).as("answer of the wrapped model").isEqualTo(WRAPPED_ANSWER);
        assertThat(wrapped.calls).as("calls the wrapped model got").containsExactly(new Call("globalCheckpoint(BooleanSupplier)", stillWanted));
    }

    @Test
    void a_one_argument_canResumeFrom_with_no_two_argument_call_around_it_reaches_the_wrapped_one_argument_method() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        wrapped.canResume = false;

        // When
        boolean canResume = model.canResumeFrom(WRAPPED_ANSWER);

        // Then
        assertThat(canResume).as("answer of the wrapped model").isFalse();
        assertThat(wrapped.calls).as("calls the wrapped model got").containsExactly(new Call("canResumeFrom(Checkpoint)", null));
    }

    @Test
    void a_one_argument_globalCheckpoint_with_no_two_argument_call_around_it_reaches_the_wrapped_one_argument_method() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

        // When
        Checkpoint checkpoint = model.globalCheckpoint();

        // Then
        assertThat(checkpoint).as("answer of the wrapped model").isEqualTo(WRAPPED_ANSWER);
        assertThat(wrapped.calls).as("calls the wrapped model got").containsExactly(new Call("globalCheckpoint()", null));
    }

    @Test
    void a_one_argument_call_after_a_two_argument_call_has_returned_does_not_see_its_stillWanted() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        model.canResumeFrom(WRAPPED_ANSWER, () -> true);
        model.globalCheckpoint(() -> true);
        wrapped.calls.clear();

        // When
        model.canResumeFrom(WRAPPED_ANSWER);
        model.globalCheckpoint();

        // Then
        assertThat(wrapped.calls).as("calls the wrapped model got").containsExactly(new Call("canResumeFrom(Checkpoint)", null), new Call("globalCheckpoint()", null));
    }

    @Test
    void a_one_argument_canResumeFrom_inside_a_nested_two_argument_call_sees_the_stillWanted_of_the_outer_call_again() {
        // Given an override that makes a nested two-argument call, with another stillWanted, before it calls super
        BooleanSupplier outerStillWanted = () -> true;
        BooleanSupplier innerStillWanted = () -> false;
        AtomicBoolean nested = new AtomicBoolean();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage()) {
            @Override
            public boolean canResumeFrom(Checkpoint checkpoint) {
                if (!nested.getAndSet(true)) {
                    canResumeFrom(checkpoint, innerStillWanted);
                }
                return super.canResumeFrom(checkpoint);
            }
        };

        // When
        model.canResumeFrom(WRAPPED_ANSWER, outerStillWanted);

        // Then the call to super after the nested call is the outer call's
        assertThat(wrapped.calls).as("calls the wrapped model got")
                .containsExactly(new Call("canResumeFrom(Checkpoint, BooleanSupplier)", innerStillWanted), new Call("canResumeFrom(Checkpoint, BooleanSupplier)", outerStillWanted));
    }

    @Test
    void a_one_argument_globalCheckpoint_inside_a_nested_two_argument_call_sees_the_stillWanted_of_the_outer_call_again() {
        // Given an override that makes a nested two-argument call, with another stillWanted, before it calls super
        BooleanSupplier outerStillWanted = () -> true;
        BooleanSupplier innerStillWanted = () -> false;
        AtomicBoolean nested = new AtomicBoolean();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage()) {
            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                if (!nested.getAndSet(true)) {
                    globalCheckpoint(innerStillWanted);
                }
                return super.globalCheckpoint();
            }
        };

        // When
        model.globalCheckpoint(outerStillWanted);

        // Then the call to super after the nested call is the outer call's
        assertThat(wrapped.calls).as("calls the wrapped model got")
                .containsExactly(new Call("globalCheckpoint(BooleanSupplier)", innerStillWanted), new Call("globalCheckpoint(BooleanSupplier)", outerStillWanted));
    }

    private record Call(String method, @Nullable BooleanSupplier stillWanted) {
    }

    private static final class RecordingLiveModel implements CheckpointAwareSubscriptionModel {
        final List<Call> calls = new CopyOnWriteArrayList<>();
        volatile boolean canResume = true;

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            calls.add(new Call("globalCheckpoint()", null));
            return WRAPPED_ANSWER;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint(BooleanSupplier stillWanted) {
            calls.add(new Call("globalCheckpoint(BooleanSupplier)", stillWanted));
            return WRAPPED_ANSWER;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            calls.add(new Call("canResumeFrom(Checkpoint)", null));
            return canResume;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
            calls.add(new Call("canResumeFrom(Checkpoint, BooleanSupplier)", stillWanted));
            return canResume;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new UnsupportedOperationException();
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
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }
    }
}
