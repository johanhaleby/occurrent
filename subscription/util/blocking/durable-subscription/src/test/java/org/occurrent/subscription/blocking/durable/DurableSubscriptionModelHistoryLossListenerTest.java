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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The model it wraps can outlive it, so shutting it down has to take back the listener it added there. The position a
 * subscription restarts from after its history is lost is stored even when its persist predicate stores nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelHistoryLossListenerTest {

    private static final String SUBSCRIPTION_ID = "sub";

    @Test
    void shutting_down_removes_the_history_loss_listener_it_added_to_the_wrapped_model() {
        HistoryLossReportingModel wrapped = new HistoryLossReportingModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        assertThat(wrapped.listeners).hasSize(1);

        model.shutdown();

        assertThat(wrapped.listeners).as("the wrapped model no longer holds the listener after shutdown").isEmpty();
    }

    @Test
    void a_subscription_from_a_start_position_of_its_own_whose_predicate_declined_its_event_still_has_where_it_restarts_stored_after_its_history_was_lost() {
        // Given
        HistoryLossReportingModel wrapped = new HistoryLossReportingModel();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        AtomicInteger eventsOffered = new AtomicInteger();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(__ -> {
            eventsOffered.incrementAndGet();
            return false;
        }));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringBasedCheckpoint("own")), __ -> {
        });
        wrapped.actions.getFirst().accept(checkpointAwareCloudEvent("declined"));
        assertThat(eventsOffered).as("events offered to the predicate").hasValue(1);
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored for the declined event").isNull();

        // When
        wrapped.listeners.getFirst().restartingAfterHistoryLoss(SUBSCRIPTION_ID, new StringBasedCheckpoint("restarted-from"), () -> true);

        // Then
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").extracting(Checkpoint::asString).isEqualTo("restarted-from");
    }

    private static CloudEvent checkpointAwareCloudEvent(String checkpoint) {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("1")
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Created")
                .build();
        return new CheckpointAwareCloudEvent(cloudEvent, new StringBasedCheckpoint(checkpoint));
    }

    private static final class HistoryLossReportingModel implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions {
        final List<HistoryLossListener> listeners = new ArrayList<>();
        final List<Consumer<CloudEvent>> actions = new ArrayList<>();

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            listeners.remove(listener);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.add(action);
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

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }

        @Override
        public void shutdown() {
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
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
