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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive counterpart of the blocking write refusal test. A refusal is a failed {@link Mono}, and nothing is
 * written. These tests drive the real reactive application service over a real model, which is never given to the
 * test extension that turns the refusals off.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelWriteRefusalTest {

    private InMemoryReactiveEventStore eventStore;
    private SynchronousSubscriptionModel model;
    private List<String> handled;
    private GenericApplicationService<TestEvent> applicationService;

    @BeforeEach
    void initialize() {
        eventStore = new InMemoryReactiveEventStore();
        model = new SynchronousSubscriptionModel();
        handled = new CopyOnWriteArrayList<>();
        applicationService = GenericApplicationService.builder(eventStore, new TestEventConverter())
                .synchronousSubscriptions(model)
                .build();
    }

    @Test
    void a_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_after_the_model_is_stopped_goes_through_when_no_handler_is_registered() {
        model.stop();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_write_after_the_model_is_started_again_goes_through_and_reaches_the_handler() {
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();
        model.start();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_to_the_stream_a_paused_subscription_filters_on_is_refused_and_stores_nothing() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamId("x")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");

        StepVerifier.create(applicationService.execute("x", events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(eventStore.contains("x")).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_to_another_stream_than_the_one_a_paused_subscription_filters_on_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamId("x")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");

        StepVerifier.create(applicationService.execute("y", events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains("y")).as("the write was stored").isTrue();
        assertThat(eventStore.contains("x")).as("the other stream was written").isFalse();
    }

    @Test
    void a_write_that_gives_an_event_the_stream_version_a_paused_subscription_filters_on_is_refused_and_stores_nothing() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamVersion(1)), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_that_gives_no_event_the_stream_version_a_paused_subscription_filters_on_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamVersion(2)), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_write_that_a_paused_subscription_does_not_match_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> handled.add("orders-projection")));
        model.subscribe("other-projection", StreamSubscriptionFilter.filter(Filter.type("NameWasChanged")), event -> Mono.fromRunnable(() -> handled.add("other-projection")));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameWasChanged"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
        assertThat(handled).containsExactly("other-projection");
    }

    private static final class InMemoryReactiveEventStore implements EventStore {
        private final ConcurrentHashMap<String, List<CloudEvent>> streams = new ConcurrentHashMap<>();

        boolean contains(String streamId) {
            return streams.containsKey(streamId);
        }

        @Override
        public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
            return Mono.fromSupplier(() -> {
                List<CloudEvent> all = streams.getOrDefault(streamId, List.of());
                List<CloudEvent> slice = all.stream().skip(skip).limit(limit).toList();
                return new EventStream<>() {
                    @Override
                    public String id() {
                        return streamId;
                    }

                    @Override
                    public long version() {
                        return all.size();
                    }

                    @Override
                    public Flux<CloudEvent> events() {
                        return Flux.fromIterable(slice);
                    }
                };
            });
        }

        @Override
        public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
            return write(streamId, events);
        }

        @Override
        public Mono<WriteResult> write(String streamId, Flux<CloudEvent> events) {
            return events.collectList().map(newEvents -> {
                List<CloudEvent> stream = streams.computeIfAbsent(streamId, id -> new ArrayList<>());
                long oldVersion = stream.size();
                stream.addAll(newEvents);
                return new WriteResult(streamId, oldVersion, stream.size());
            });
        }

        @Override
        public Mono<Boolean> exists(String streamId) {
            return Mono.fromSupplier(() -> streams.containsKey(streamId));
        }
    }

    private record TestEvent(String type) {
    }

    private static final class TestEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(domainEvent.type())
                    .build();
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return new TestEvent(cloudEvent.getType());
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            throw new UnsupportedOperationException();
        }
    }
}
