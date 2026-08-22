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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A configured {@link org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource} stamps every checkpoint
 * {@link StreamCatchupSubscriptionModel} writes with {@code notOlderThan(version)}. No source, or a source
 * answering empty, leaves every write {@code any()}, exactly the behaviour before ADR 116. The source lives on
 * {@link CheckpointStorageConfig.UseCheckpointInStorage} rather than only on
 * {@link CheckpointStorageConfig.PersistCheckpointDuringCatchupPhase}, because the catch-up-to-live handover writes
 * a checkpoint through {@code UseCheckpointInStorage} even when nothing is configured to persist during the
 * catch-up phase itself, and that write needs stamping too, so the second test below covers exactly that shape. Run
 * over {@link InMemoryCheckpointStorage}, which evaluates a {@code CheckpointWriteCondition} for real rather than
 * refusing it, so the stored version proves which condition the model actually stamped the write with.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupSubscriptionModelCheckpointWriteVersionSourceTest {

    private static final String SUBSCRIPTION_ID = "subscription";

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CheckpointAwareSubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        subscriptionModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_configured_source_stamps_the_periodic_catchup_phase_write_not_older_than_the_version_it_answers() {
        write(nameDefined("event1"));
        CheckpointStorage storage = new InMemoryCheckpointStorage();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(
                useCheckpointStorage(storage, id -> OptionalLong.of(9)).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config);

        subscription.subscribe(SUBSCRIPTION_ID, StartAtTime.beginningOfTime(), event -> {
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).hasValue(9L));
    }

    @Test
    void a_configured_source_also_stamps_the_catchup_to_live_handover_write_when_nothing_persists_during_catchup() {
        write(nameDefined("event1"));
        CheckpointStorage storage = new InMemoryCheckpointStorage();
        // No andPersistCheckpointDuringCatchupPhase...: only the handover write happens, and it must still be
        // stamped, since it goes through UseCheckpointInStorage rather than PersistCheckpointDuringCatchupPhase.
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(useCheckpointStorage(storage, id -> OptionalLong.of(3)));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config);

        subscription.subscribe(SUBSCRIPTION_ID, StartAtTime.beginningOfTime(), event -> {
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).hasValue(3L));
    }

    @Test
    void a_source_answering_empty_leaves_the_write_any_the_same_as_no_source() {
        write(nameDefined("event1"));
        CheckpointStorage storage = new InMemoryCheckpointStorage();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(
                useCheckpointStorage(storage, id -> OptionalLong.empty()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config);

        subscription.subscribe(SUBSCRIPTION_ID, StartAtTime.beginningOfTime(), event -> {
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(storage.exists(SUBSCRIPTION_ID)).isTrue());
        assertThat(storage.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    @Test
    void no_source_configured_leaves_the_write_any_unchanged_from_before_adr_116() {
        write(nameDefined("event1"));
        CheckpointStorage storage = new InMemoryCheckpointStorage();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(
                useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config);

        subscription.subscribe(SUBSCRIPTION_ID, StartAtTime.beginningOfTime(), event -> {
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(storage.exists(SUBSCRIPTION_ID)).isTrue());
        assertThat(storage.writeVersion(SUBSCRIPTION_ID)).isEmpty();
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link CheckpointAwareSubscriptionModel},
     * mirroring the sibling test classes in this module. Any position start is translated to {@code now}, since the
     * in-memory model only supports {@code now}/{@code default}.
     */
    private static final class CheckpointAwareInMemorySubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;

        private CheckpointAwareInMemorySubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            StartAt startAtToUse = resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
            return delegate.subscribe(subscriptionId, filter, startAtToUse, action);
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
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
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
