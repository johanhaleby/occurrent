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

package org.occurrent.springboot.mongo.blocking;

import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The start boundary of a handler that goes straight to live comes from the event store the subscription model reads,
 * which is the only one or the {@code @Primary} one. Another event store in the context, one with no live start such
 * as the in-memory one or one on another database, must not decide it.
 * <p>
 * The observable is the live start that the starter turns into the handler's first checkpoint. Each store records the
 * live start it reported when it was created, so the test can tell whose it was.
 * <p>
 * Each scenario boots its own application with {@code SpringApplication.run}, on a database of its own.
 */
@DisplayName("The start boundary comes from the event store the subscription model reads")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class StartBoundaryEventStoreChoiceMongoTest {

    private static final String HANDLER_ID = "boundaryProbe";
    private static final URI SOURCE = URI.create("urn:occurrent:start-boundary-event-store-choice");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @BeforeEach
    void forget_what_an_earlier_boot_observed() {
        Observed.reset();
    }

    @Test
    void an_in_memory_store_created_before_the_primary_mongo_store_does_not_turn_the_boundary_off() {
        try (ConfigurableApplicationContext ignored = boot(InMemoryStoreFirstApplication.class, "in-memory-first")) {
            assertThat(Observed.createdAt.get("inMemory")).as("sequence number of the in-memory store's creation")
                    .isNotNull().isLessThan(Observed.createdAt.get("primary"));
            String primaryLiveStart = Observed.firstLiveStart.get("primary");
            assertThat(primaryLiveStart).as("the live start the primary MongoDB store reported when it was created").isNotNull();

            assertThat(Observed.liveStartsTurnedIntoCheckpoints).as("live starts the handler's first checkpoint was made of")
                    .containsOnly(primaryLiveStart);
        }
    }

    @Test
    void with_two_stores_the_boundary_comes_from_the_primary_one() {
        try (ConfigurableApplicationContext ignored = boot(TwoStoresApplication.class, "two-stores")) {
            String primaryLiveStart = Observed.firstLiveStart.get("primary");
            String otherLiveStart = Observed.firstLiveStart.get("other");
            assertThat(primaryLiveStart).as("the live start the primary store reported when it was created").isNotNull();
            assertThat(otherLiveStart).as("the live start the other store reported when it was created").isNotNull().isNotEqualTo(primaryLiveStart);

            assertThat(Observed.liveStartsTurnedIntoCheckpoints).as("live starts the handler's first checkpoint was made of")
                    .containsOnly(primaryLiveStart);
        }
    }

    @Test
    void a_creation_cycle_at_the_first_boundary_callback_does_not_fail_startup() {
        ConfigurableApplicationContext[] context = new ConfigurableApplicationContext[1];
        try {
            org.assertj.core.api.Assertions.assertThatCode(() -> context[0] = boot(DependentPrimaryStoreApplication.class, "dependent-primary"))
                    .as("starting an application whose primary store is created for the first store's boundary callback and needs that store")
                    .doesNotThrowAnyException();

            String primaryLiveStart = Observed.firstLiveStart.get("primary");
            assertThat(primaryLiveStart).as("the live start the primary store reported when it was created").isNotNull();
            assertThat(Observed.liveStartsTurnedIntoCheckpoints).as("live starts the handler's first checkpoint was made of")
                    .containsOnly(primaryLiveStart);
        } finally {
            if (context[0] != null) {
                context[0].close();
            }
        }
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        return SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    /**
     * What the stores and the checkpoint maker report, by the name each store is given. Static since the stores are
     * created by the application the test boots.
     */
    static final class Observed {
        static final AtomicInteger sequence = new AtomicInteger();
        static final Map<String, Integer> createdAt = new ConcurrentHashMap<>();
        static final Map<String, String> firstLiveStart = new ConcurrentHashMap<>();
        static final List<String> liveStartsTurnedIntoCheckpoints = new CopyOnWriteArrayList<>();

        static void reset() {
            sequence.set(0);
            createdAt.clear();
            firstLiveStart.clear();
            liveStartsTurnedIntoCheckpoints.clear();
        }

        static void created(String name) {
            createdAt.put(name, sequence.incrementAndGet());
        }
    }

    static class RecordingMongoEventStore extends SpringMongoEventStore {
        private final String name;

        RecordingMongoEventStore(String name, MongoTemplate template, EventStoreConfig config) {
            super(template, config);
            this.name = name;
            Observed.created(name);
        }

        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            CatchupReadScope scope = super.openCatchupReadScope(notBefore);
            return new CatchupReadScope() {
                @Override
                public @Nullable String liveStart() {
                    @Nullable String liveStart = scope.liveStart();
                    if (liveStart != null) {
                        Observed.firstLiveStart.putIfAbsent(name, liveStart);
                    }
                    return liveStart;
                }

                @Override
                public long currentPosition() {
                    return scope.currentPosition();
                }

                @Override
                public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                    return scope.readInPositionOrder(filter, range);
                }

                @Override
                public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                    return scope.query(filter, skip, limit, sortBy);
                }

                @Override
                public void close() {
                    scope.close();
                }
            };
        }
    }

    // Records each live start the starter turns into a checkpoint, which is what a first start at the boundary does
    static class LiveStartRecorder implements BeanPostProcessor {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (bean instanceof LiveStartCheckpoints checkpoints) {
                return (LiveStartCheckpoints) liveStart -> {
                    Observed.liveStartsTurnedIntoCheckpoints.add(liveStart);
                    return checkpoints.checkpointOf(liveStart);
                };
            }
            return bean;
        }
    }

    // Goes straight to live on a first start, which is what the boundary applies to
    static class BoundaryProbe {
        @StreamSubscription(id = HANDLER_ID)
        void on(TestEvent event) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Probe {

        @Bean
        static LiveStartRecorder liveStartRecorder() {
            return new LiveStartRecorder();
        }

        @Bean
        BoundaryProbe boundaryProbe() {
            return new BoundaryProbe();
        }

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }
    }

    // The in-memory store is declared first, so it is created first and its callback is the first the starter sees.
    // Its live start is null, and the Mongo store the model reads is the @Primary one.
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @org.springframework.context.annotation.Import(Probe.class)
    static class InMemoryStoreFirstApplication {

        @Bean
        InMemoryEventStore inMemoryEventStore() {
            Observed.created("inMemory");
            return new InMemoryEventStore();
        }

        // Created for the in-memory store to be first. Spring would otherwise create the Mongo store earlier than the
        // order the beans are declared in.
        @Bean
        @Primary
        @DependsOn("inMemoryEventStore")
        RecordingMongoEventStore primaryEventStore(MongoTemplate template, EventStoreConfig config) {
            return new RecordingMongoEventStore("primary", template, config);
        }
    }

    // Two stores that both report a live start. The cluster time moves on before each is created, so the two live
    // starts differ.
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @org.springframework.context.annotation.Import(Probe.class)
    static class TwoStoresApplication {

        @Bean
        RecordingMongoEventStore otherEventStore(MongoTemplate template, EventStoreConfig config) {
            moveTheClusterTimeOn(template);
            return new RecordingMongoEventStore("other", template, config);
        }

        @Bean
        @Primary
        RecordingMongoEventStore primaryEventStore(MongoTemplate template, EventStoreConfig config) {
            moveTheClusterTimeOn(template);
            return new RecordingMongoEventStore("primary", template, config);
        }
    }

    // The first store the starter sees is the auto-configured one, and its callback creates the @Primary store. The
    // @Primary store needs the auto-configured one, which is still in creation at that point.
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    @org.springframework.context.annotation.Import(Probe.class)
    static class DependentPrimaryStoreApplication {

        @Bean
        @Primary
        RecordingMongoEventStore primaryEventStore(MongoTemplate template, EventStoreConfig config,
                                                   @Qualifier("occurrentSpringMongoEventStore") SpringMongoEventStore autoConfigured) {
            moveTheClusterTimeOn(template);
            return new RecordingMongoEventStore("primary", template, config);
        }
    }

    private static void moveTheClusterTimeOn(MongoTemplate template) {
        template.getCollection("unrelated").insertOne(new Document("_id", UUID.randomUUID().toString()));
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
