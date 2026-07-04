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
import org.occurrent.subscription.GlobalSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.api.blocking.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * In-memory unit tests for {@link StreamCatchupSubscriptionModel}, used directly rather than through the
 * {@code CatchupSubscriptionModel} dispatcher. These prove the extracted class works standalone (this module has no
 * {@code eventstore-api-dcb} dependency of its own; see the module's {@code pom.xml} and
 * {@code mvn dependency:tree}), covering both the legacy time-ordered catch-up and the position-ordered catch-up.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupSubscriptionModelTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private PositionAwareSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        subscriptionModel = new PositionAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void replays_historic_events_by_time_when_the_store_does_not_write_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isFalse();

        NameDefined event1 = nameDefined("event1");
        NameDefined event2 = nameDefined("event2");
        write(eventStore, event1);
        write(eventStore, event2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(event1, event2));
    }

    @Test
    void replays_historic_events_by_position_when_the_store_writes_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        assertThat(eventStore.writesPosition()).isTrue();

        NameDefined event1 = nameDefined("event1");
        NameDefined event2 = nameDefined("event2");
        write(eventStore, event1);
        write(eventStore, event2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.subscriptionPosition(GlobalSubscriptionPosition.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(event1, event2));
    }

    @Test
    void live_only_subscription_delegates_to_the_wrapped_model_when_start_is_now() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.now(), toDomainEvents(received)).waitUntilStarted();

        NameDefined live = nameDefined("live");
        write(eventStore, live);

        await().untilAsserted(() -> assertThat(received).containsExactly(live));
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void write(InMemoryEventStore eventStore, DomainEvent event) {
        Stream<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link PositionAwareSubscriptionModel} for
     * these tests, mirroring the catchup-subscription module's own test double: any position start is translated
     * to {@code now}, since the in-memory model only supports {@code now}/{@code default}.
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
}
