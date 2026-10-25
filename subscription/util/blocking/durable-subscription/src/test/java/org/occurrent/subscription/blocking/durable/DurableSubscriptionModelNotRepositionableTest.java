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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A {@link DurableSubscriptionModel} over a live model that can't resume at a position isn't found as
 * {@link RepositionableSubscriptions}, and refuses a resume at a position called on it directly.
 */
@DisplayName("DurableSubscriptionModelNotRepositionable")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelNotRepositionableTest {

    private static final String SUBSCRIPTION_ID = "subscription";

    @Test
    void the_lookup_finds_nothing_when_the_wrapped_model_cannot_reposition() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(new LiveModelThatCannotReposition(), new InMemoryCheckpointStorage());

        // When
        var found = RepositionableSubscriptions.findIn(model);

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void throws_UnsupportedOperationException_naming_the_wrapped_model_when_resumed_at_a_position() {
        // Given
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        Checkpoint stored = new StringBasedCheckpoint("stored");
        storage.save(SUBSCRIPTION_ID, stored);
        DurableSubscriptionModel model = new DurableSubscriptionModel(new LiveModelThatCannotReposition(), storage);

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(new StringBasedCheckpoint("newer"))));

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(UnsupportedOperationException.class).hasMessage("LiveModelThatCannotReposition is not repositionable"),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(stored)
        );
    }

    private static class LiveModelThatCannotReposition implements CheckpointAwareSubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("global");
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
            return true;
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
