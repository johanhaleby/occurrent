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
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * A stream catch-up refuses an event store that does not implement {@code CatchupReads} when it is constructed.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupReadsRequirementTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_catchup_over_the_in_memory_event_store_replays_its_history_and_then_delivers_live_events() {
        NameDefined first = write("first");
        NameDefined second = write("second");
        List<String> delivered = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel), eventStore,
                new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), event -> delivered.add(event.getId())).waitUntilStarted();
        NameDefined live = write("live");

        await().untilAsserted(() -> assertThat(delivered).containsExactly(first.eventId(), second.eventId(), live.eventId()));
    }

    @Test
    void an_event_store_queries_decorator_that_does_not_implement_catchup_reads_is_refused_when_the_catchup_is_constructed() {
        EventStoreQueries decorator = new QueriesOnly(eventStore);

        assertThatThrownBy(() -> new StreamCatchupSubscriptionModel(unusedSubscriptionModel(), decorator, new CatchupSubscriptionModelConfig(100)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot run a catch-up subscription over " + QueriesOnly.class.getName())
                .hasMessageContaining("CatchupReads");
    }

    private NameDefined write(String name) {
        NameDefined event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
        eventStore.write(event.eventId(), cloudEventConverter.toCloudEvents(List.of(event)));
        return event;
    }

    private static CheckpointAwareSubscriptionModel unusedSubscriptionModel() {
        return (CheckpointAwareSubscriptionModel) Proxy.newProxyInstance(CheckpointAwareSubscriptionModel.class.getClassLoader(), new Class<?>[]{CheckpointAwareSubscriptionModel.class},
                (proxy, method, args) -> {
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    throw new AssertionError("The subscription model must not be used when the catch-up is refused: " + method);
                });
    }

    private static final class QueriesOnly implements EventStoreQueries {
        private final EventStoreQueries delegate;

        private QueriesOnly(EventStoreQueries delegate) {
            this.delegate = delegate;
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return delegate.query(filter, skip, limit, sortBy);
        }

        @Override
        public long count(Filter filter) {
            return delegate.count(filter);
        }

        @Override
        public boolean exists(Filter filter) {
            return delegate.exists(filter);
        }
    }

    private static final class CheckpointAwareInMemorySubscriptionModel implements CheckpointAwareSubscriptionModel {
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
}
