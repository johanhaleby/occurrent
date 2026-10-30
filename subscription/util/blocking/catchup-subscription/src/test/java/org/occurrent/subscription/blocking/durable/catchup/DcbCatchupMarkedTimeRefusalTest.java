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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * Only a catch-up of a stream subscription stores the marked form of a time replay, and a DCB catch-up replays only by
 * position. Given that form, explicitly or from storage, it refuses it from subscribe before it writes or delivers
 * anything, rather than taking it for a global position.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupMarkedTimeRefusalTest {

    private static final DcbCriteria CRITERIA = DcbCriteria.tags(Tag.parse("name:1"));
    private static final Checkpoint MARKED_TIME = new StringBasedCheckpoint(
            CatchupCheckpoints.format("2026-10-01T12:00:00Z", "2026-10-01T11:00:00Z", new StringBasedCheckpoint("live-start")));

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private RecordingLiveModel liveModel;
    private InMemoryEventStore eventStore;
    private InMemoryCheckpointStorage storage;
    private final List<CloudEvent> delivered = new CopyOnWriteArrayList<>();

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        liveModel = new RecordingLiveModel(inMemorySubscriptionModel);
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        storage = new InMemoryCheckpointStorage();
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        List<CloudEvent> history = converter.toCloudEvents(List.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "history"))).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse("name:1"))))
                .toList();
        eventStore.append(history);
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_dcb_catchup_refuses_a_marked_time_given_as_its_start() {
        // Given
        DcbCatchupSubscriptionModel catchup = new DcbCatchupSubscriptionModel(liveModel, eventStore, CRITERIA, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));

        // When
        // Then
        assertThatThrownBy(() -> catchup.subscribe("sub", StartAt.checkpoint(MARKED_TIME), delivered::add))
                .as("what subscribe throws")
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContainingAll("sub", MARKED_TIME.asString(), "replays only by position", "StreamCatchupSubscriptionModel", "delete the checkpoint");
        assertNothingWrittenSubscribedOrDelivered(null);
    }

    @Test
    void a_dcb_catchup_refuses_a_marked_time_read_from_storage() {
        // Given
        storage.save("sub", MARKED_TIME);
        DcbCatchupSubscriptionModel catchup = new DcbCatchupSubscriptionModel(liveModel, eventStore, CRITERIA, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));

        // When
        // Then
        assertThatThrownBy(() -> catchup.subscribe("sub", StartAt.subscriptionModelDefault(), delivered::add))
                .as("what subscribe throws")
                .isInstanceOf(CatchupCannotResumeException.class)
                .hasMessageContainingAll("sub", MARKED_TIME.asString(), "replays only by position", "StreamCatchupSubscriptionModel", "delete the checkpoint");
        assertNothingWrittenSubscribedOrDelivered(MARKED_TIME);
    }

    private void assertNothingWrittenSubscribedOrDelivered(@Nullable Checkpoint storedBefore) {
        assertThat(storage.read("sub")).as("the checkpoint for the subscription after the refusal").isEqualTo(storedBefore);
        assertThat(liveModel.subscribed).as("what the refusal subscribed on the live model").isEmpty();
        assertThat(delivered).as("what the refusal delivered").isEmpty();
    }

    private static class RecordingLiveModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        private final List<String> subscribed = new CopyOnWriteArrayList<>();

        private RecordingLiveModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscribed.add(subscriptionId);
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
    }
}
