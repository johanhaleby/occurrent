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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@link DurableSubscriptionModel} hands the stillWanted of globalCheckpoint and canResumeFrom to the model it wraps,
 * so a retry that model runs can end when the caller stops wanting the answer.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStillWantedTest {

    private static final Checkpoint ANSWER = new StringBasedCheckpoint("answer");

    private final RecordingLiveModel wrapped = new RecordingLiveModel();
    private final DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());

    @Test
    void passes_the_stillWanted_of_globalCheckpoint_to_the_wrapped_model_and_returns_its_answer() {
        BooleanSupplier stillWanted = () -> true;

        Checkpoint checkpoint = model.globalCheckpoint(stillWanted);

        assertThat(checkpoint).isEqualTo(ANSWER);
        assertThat(wrapped.globalCheckpointStillWanted.get()).as("stillWanted the wrapped model got").isSameAs(stillWanted);
    }

    @Test
    void passes_the_stillWanted_of_canResumeFrom_to_the_wrapped_model_and_returns_its_answer() {
        BooleanSupplier stillWanted = () -> true;
        wrapped.canResume = false;

        boolean canResume = model.canResumeFrom(ANSWER, stillWanted);

        assertThat(canResume).isFalse();
        assertThat(wrapped.canResumeFromStillWanted.get()).as("stillWanted the wrapped model got").isSameAs(stillWanted);
    }

    private static final class RecordingLiveModel implements CheckpointAwareSubscriptionModel {
        final AtomicReference<BooleanSupplier> globalCheckpointStillWanted = new AtomicReference<>();
        final AtomicReference<BooleanSupplier> canResumeFromStillWanted = new AtomicReference<>();
        volatile boolean canResume = true;

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return ANSWER;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint(BooleanSupplier stillWanted) {
            globalCheckpointStillWanted.set(stillWanted);
            return ANSWER;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
            canResumeFromStillWanted.set(stillWanted);
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
