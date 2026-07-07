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
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * Tests for {@link CatchupSubscriptionModel} in DCB mode (replay and resume by {@code position}, see ADR 20).
 * <p>
 * These use the in-memory event store and subscription model so the DCB-specific logic (position-windowed replay,
 * position resume, the query post-filter and the multi-window paging) is exercised deterministically without a
 * database. The in-memory subscription model is not position aware, so a small {@link CheckpointAwareInMemorySubscriptionModel}
 * test double adapts it: it translates the concrete resume position the catch-up hands over into {@code StartAt.now()}
 * (the in-memory model only supports now and default) and reports a stub global position. The faithful change-stream
 * resume across the catch-up to live seam is exercised against a real MongoDB change stream by
 * {@code DcbCatchupSubscriptionModelMongoTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupSubscriptionModelTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CheckpointAwareInMemorySubscriptionModel subscriptionModel;
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
    void replays_matching_dcb_events_from_the_beginning_of_the_sequence_in_position_order() {
        NameDefined name1 = nameDefined("name1");
        NameDefined name2 = nameDefined("name2");
        NameDefined name3 = nameDefined("name3");
        appendTagged("name:1", name1);
        appendTagged("other:1", nameDefined("ignored"));
        appendTagged("name:1", name2);
        appendTagged("name:1", name3);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        AtomicBoolean replayedOnVirtualThread = new AtomicBoolean(false);
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            replayedOnVirtualThread.set(Thread.currentThread().isVirtual());
            received.add(cloudEventConverter.toDomainEvent(cloudEvent));
        }).waitUntilStarted();

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(name1, name2, name3);
            assertThat(replayedOnVirtualThread).isTrue();
        });
    }

    @Test
    void delivers_events_written_during_and_after_catchup_through_the_live_handover_without_duplicates() {
        NameDefined historic1 = nameDefined("historic1");
        NameDefined historic2 = nameDefined("historic2");
        appendTagged("name:1", historic1);
        appendTagged("name:1", historic2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();
        await().untilAsserted(() -> assertThat(received).containsExactly(historic1, historic2));

        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        appendTagged("name:1", live1);
        appendTagged("other:1", nameDefined("ignoredLive"));
        appendTagged("name:1", live2);

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(historic1, historic2, live1, live2);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void resumes_replay_after_a_supplied_dcb_position_and_skips_earlier_events() {
        NameDefined position1 = nameDefined("position1");
        NameDefined position2 = nameDefined("position2");
        NameDefined position3 = nameDefined("position3");
        appendTagged("name:1", position1); // position 1
        appendTagged("name:1", position2); // position 2
        appendTagged("name:1", position3); // position 3

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(position3));
    }

    @Test
    void resumes_replay_from_a_dcb_position_read_back_from_storage() {
        appendTagged("name:1", nameDefined("position1")); // position 1
        NameDefined position2 = nameDefined("position2");
        appendTagged("name:1", position2);                // position 2

        CheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save("subscription", GlobalCheckpoint.of(1));

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")),
                new CatchupSubscriptionModelConfig(useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(position2));
    }

    @Test
    void live_only_subscription_applies_the_dcb_query_post_filter() {
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        // Default start with nothing stored subscribes live (no replay), mirroring the stream path.
        subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        NameDefined matching = nameDefined("matching");
        appendTagged("name:1", matching);
        appendTagged("other:1", nameDefined("nonMatching"));

        await().untilAsserted(() -> assertThat(received).containsExactly(matching));
    }

    @Test
    void replays_across_multiple_position_windows() {
        List<NameDefined> events = List.of(nameDefined("e1"), nameDefined("e2"), nameDefined("e3"), nameDefined("e4"), nameDefined("e5"));
        events.forEach(event -> appendTagged("name:1", event));

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        // A window of 2 positions forces the replay to page across several windows to cover all five events.
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")),
                new CatchupSubscriptionModelConfig(100).dcbCatchupPositionWindowSize(2));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(events));
    }

    @Test
    void position_catchup_fails_loudly_instead_of_silently_resuming_at_now_when_the_delegate_reports_no_resume_token() {
        appendTagged("name:1", nameDefined("position1"));

        CheckpointAwareSubscriptionModel nullCheckpointSubscriptionModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel) {
            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                return null;
            }
        };
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(nullCheckpointSubscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
        });

        assertThat(started).isInstanceOf(CatchupSubscription.class);
        Future<Subscription> delegatedSubscription = ((CatchupSubscription) started).delegatedSubscription();
        assertThatThrownBy(() -> delegatedSubscription.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause().hasMessageContaining("no resume token");
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse(tag))))
                .toList();
        eventStore.append(cloudEvents);
    }

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link CheckpointAwareSubscriptionModel} for
     * these tests. The catch-up hands over to the live phase with a concrete checkpoint, but the in-memory
     * model only supports {@code now}/{@code default}, so any position start is translated to {@code now}. The stub
     * global position is enough for the catch-up to take its normal handover path.
     */
    private static class CheckpointAwareInMemorySubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;

        private CheckpointAwareInMemorySubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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

    private static final class InMemoryCheckpointStorage implements CheckpointStorage {
        private final ConcurrentMap<String, Checkpoint> positions = new ConcurrentHashMap<>();

        @Override
        public Checkpoint read(String subscriptionId) {
            return positions.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
            positions.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public void delete(String subscriptionId) {
            positions.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return positions.containsKey(subscriptionId);
        }
    }
}
