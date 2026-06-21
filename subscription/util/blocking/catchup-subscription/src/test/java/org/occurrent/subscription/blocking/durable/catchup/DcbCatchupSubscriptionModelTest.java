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
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.useSubscriptionPositionStorage;

/**
 * Tests for {@link CatchupSubscriptionModel} in DCB mode (replay and resume by {@code dcbposition}, see ADR 20).
 * <p>
 * These use the in-memory event store and subscription model so the DCB-specific logic (position-windowed replay,
 * dcbposition resume, the query post-filter and the multi-window paging) is exercised deterministically without a
 * database. The in-memory subscription model is not position aware, so a small {@link PositionAwareInMemorySubscriptionModel}
 * test double adapts it: it translates the concrete resume position the catch-up hands over into {@code StartAt.now()}
 * (the in-memory model only supports now and default) and reports a stub global position. The faithful change-stream
 * resume across the catch-up to live seam is exercised against a real MongoDB change stream by
 * {@code DcbCatchupSubscriptionModelMongoTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupSubscriptionModelTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private PositionAwareInMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        subscriptionModel = new PositionAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
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
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"));

        subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(name1, name2, name3));
    }

    @Test
    void delivers_events_written_during_and_after_catchup_through_the_live_handover_without_duplicates() {
        NameDefined historic1 = nameDefined("historic1");
        NameDefined historic2 = nameDefined("historic2");
        appendTagged("name:1", historic1);
        appendTagged("name:1", historic2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"));

        subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0)), toDomainEvents(received)).waitUntilStarted();
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
        appendTagged("name:1", position1); // dcbposition 1
        appendTagged("name:1", position2); // dcbposition 2
        appendTagged("name:1", position3); // dcbposition 3

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"));

        subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(2)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(position3));
    }

    @Test
    void resumes_replay_from_a_dcb_position_read_back_from_storage() {
        appendTagged("name:1", nameDefined("position1")); // dcbposition 1
        NameDefined position2 = nameDefined("position2");
        appendTagged("name:1", position2);                // dcbposition 2

        SubscriptionPositionStorage storage = new InMemorySubscriptionPositionStorage();
        storage.save("subscription", DcbSubscriptionPosition.of(1));

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"),
                new CatchupSubscriptionModelConfig(useSubscriptionPositionStorage(storage).andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(1)));

        subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(position2));
    }

    @Test
    void live_only_subscription_applies_the_dcb_query_post_filter() {
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"));

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
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbQuery.tagsAllOf("name:1"),
                new CatchupSubscriptionModelConfig(100).dcbCatchupPositionWindowSize(2));

        subscription.subscribe("subscription", StartAt.subscriptionPosition(DcbSubscriptionPosition.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(events));
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(events))
                .map(event -> DcbCloudEvents.withTags(event, List.of(tag)))
                .toList();
        eventStore.append(cloudEvents);
    }

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link PositionAwareSubscriptionModel} for
     * these tests. The catch-up hands over to the live phase with a concrete subscription position, but the in-memory
     * model only supports {@code now}/{@code default}, so any position start is translated to {@code now}. The stub
     * global position is enough for the catch-up to take its normal handover path.
     */
    private static final class PositionAwareInMemorySubscriptionModel implements PositionAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;

        private PositionAwareInMemorySubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            StartAt startAtToUse = resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
            return delegate.subscribe(subscriptionId, filter, startAtToUse, action);
        }

        @Override
        public @Nullable SubscriptionPosition globalSubscriptionPosition() {
            return new StringBasedSubscriptionPosition("in-memory-global-position");
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

    private static final class InMemorySubscriptionPositionStorage implements SubscriptionPositionStorage {
        private final ConcurrentMap<String, SubscriptionPosition> positions = new ConcurrentHashMap<>();

        @Override
        public SubscriptionPosition read(String subscriptionId) {
            return positions.get(subscriptionId);
        }

        @Override
        public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
            positions.put(subscriptionId, subscriptionPosition);
            return subscriptionPosition;
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
