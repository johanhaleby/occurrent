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

package org.occurrent.springboot.mongo.reactor;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The reactive counterpart of {@code LiveStartCheckpointStorageOrderingMongoTest}. The start boundary is pinned as a
 * first checkpoint only over a checkpoint storage the starter knows orders it, which is {@link ReactorCheckpointStorage}.
 * Over any other storage a pin would keep a checkpoint another node stored after the boundary was read, so a first
 * start there begins where it did in 0.34.0.
 * <p>
 * The storage that cannot be ordered is a wrapper around a real {@link ReactorCheckpointStorage} on the same
 * collection. It keeps {@code ifAbsent()} atomic across nodes and answers empty from
 * {@code resolveFirstCheckpointRace}, as a storage that does not order checkpoints does, and records what is written.
 * <p>
 * Unlike the blocking test, no node is held before it registers, so the second node here boots after the first one
 * stored its checkpoint. What this proves about the second node is that it asks the storage to resolve nothing and
 * saves no boundary, which is what a pin over this storage would do.
 */
@DisplayName("Reactive live start boundary over a checkpoint storage the starter may or may not know to order it")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveLiveStartCheckpointStorageOrderingMongoTest {

    private static final String SUBSCRIPTION_ID = "reactiveStorageOrderingSubscriber";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-live-start-checkpoint-storage-ordering");
    private static final long WAIT_SECONDS = 60;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_first_start_at_default_over_a_storage_the_starter_cannot_order_stores_no_pinned_boundary() {
        Node node = new Node();
        try (ConfigurableApplicationContext context = boot(node, newDatabase(), UnorderedStorageConfiguration.class, DefaultStartHandlerConfiguration.class)) {
            node.write("written-after-registration");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.received).as("events the node received").contains("written-after-registration"));

            Checkpoint boundary = requireNonNull(node.liveStartCheckpoint, "the checkpoint the starter made of the live start");
            assertThat(node.savedPositions).as("positions saved for the subscription, the boundary pinned by the starter not among them").doesNotContain(boundary.asString());
        }
    }

    @Test
    void a_second_node_over_a_storage_the_starter_cannot_order_asks_the_storage_to_resolve_nothing_and_saves_no_boundary() {
        String database = newDatabase();
        Node first = new Node();
        Node second = new Node();
        try (ConfigurableApplicationContext firstContext = boot(first, database, UnorderedStorageConfiguration.class, DefaultStartHandlerConfiguration.class)) {
            first.write("written-after-the-first-node-registered");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(first.received).as("events the first node received").contains("written-after-the-first-node-registered"));
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(first.storage().read(SUBSCRIPTION_ID).block()).as("checkpoint stored by the first node").isNotNull());

            try (ConfigurableApplicationContext secondContext = boot(second, database, UnorderedStorageConfiguration.class, DefaultStartHandlerConfiguration.class)) {
                first.write("written-after-both-nodes-registered");
                await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
                    List<String> receivedBySomeNode = new ArrayList<>(first.received);
                    receivedBySomeNode.addAll(second.received);
                    assertThat(receivedBySomeNode).as("events delivered to a node").contains("written-after-the-first-node-registered", "written-after-both-nodes-registered");
                });

                Checkpoint secondBoundary = requireNonNull(second.liveStartCheckpoint, "the checkpoint the starter made of the second node's live start");
                assertAll(
                        () -> assertThat(second.savedPositions).as("positions the second node saved for the subscription, its boundary not among them").doesNotContain(secondBoundary.asString()),
                        () -> assertThat(second.resolutions).as("first-position resolutions the second node asked the storage for").isEmpty()
                );
            }
        }
    }

    @Test
    void a_start_at_now_that_resumes_the_same_as_it_started_begins_at_the_boundary_over_a_storage_the_starter_cannot_order() {
        Node node = new Node();
        node.eventWrittenBeforeRegistration = "written-before-registration";
        try (ConfigurableApplicationContext context = boot(node, newDatabase(), UnorderedStorageConfiguration.class, NowSameAsStartAtHandlerConfiguration.class)) {
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.received).as("events the node received").contains("written-before-registration"));

            assertThat(node.savedPositions).as("positions saved before the event was delivered, nothing the starter pinned").doesNotContain(requireNonNull(node.liveStartCheckpoint).asString());
        }
    }

    @Test
    void a_first_start_at_default_over_a_reactor_checkpoint_storage_declared_by_the_application_with_a_collection_of_its_own_pins_the_boundary_and_receives_the_event_written_before_registration() {
        Node node = new Node();
        node.eventWrittenBeforeRegistration = "written-before-registration";
        try (ConfigurableApplicationContext context = boot(node, newDatabase(), OwnCollectionStorageConfiguration.class, DefaultStartHandlerConfiguration.class)) {
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.received).as("events the node received").contains("written-before-registration"));

            Checkpoint boundary = requireNonNull(node.liveStartCheckpoint, "the checkpoint the starter made of the live start");
            assertThat(node.savedPositions).as("positions saved for the subscription, the first one the pinned boundary").first().isEqualTo(boundary.asString());
        }
    }

    private static String newDatabase() {
        return mongoDBContainer.getReplicaSetUrl("reactive-storage-ordering-" + UUID.randomUUID().toString().substring(0, 8));
    }

    private static ConfigurableApplicationContext boot(Node node, String url, Class<?>... configurations) {
        Class<?>[] sources = Arrays.copyOf(configurations, configurations.length + 1);
        sources[configurations.length] = NodeApplication.class;
        return new SpringApplicationBuilder(sources)
                .initializers(context -> ((ConfigurableApplicationContext) context).getBeanFactory().registerSingleton("node", node))
                .run(
                        "--spring.mongodb.uri=" + url,
                        "--spring.main.web-application-type=none",
                        "--occurrent.event-store.capabilities=stream",
                        "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    private static CloudEventConverter<TestEvent> testEventConverter() {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(ReflectionCloudEventTypeMapper.qualified())
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    /**
     * What a node records about itself.
     */
    static final class Node {
        final List<String> received = new CopyOnWriteArrayList<>();
        final List<String> savedPositions = new CopyOnWriteArrayList<>();
        final List<String> resolutions = new CopyOnWriteArrayList<>();
        volatile @Nullable String eventWrittenBeforeRegistration;
        volatile @Nullable EventStore eventStore;
        volatile @Nullable Checkpoint liveStartCheckpoint;
        volatile @Nullable CheckpointStorage storage;

        CheckpointStorage storage() {
            return requireNonNull(storage, "the checkpoint storage of the node");
        }

        void write(String name) {
            requireNonNull(eventStore, "the event store of the node")
                    .write(UUID.randomUUID().toString(), testEventConverter().toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), name))).block();
        }

        // Runs when the handler bean is built, which is after the event store bean was created and its boundary read, and
        // before the handler is registered
        void beforeRegistration(EventStore eventStore) {
            this.eventStore = eventStore;
            @Nullable String name = eventWrittenBeforeRegistration;
            if (name != null) {
                write(name);
            }
        }
    }

    /**
     * Delegates everything to a real {@link ReactorCheckpointStorage}, so {@code ifAbsent()} is atomic across nodes, and
     * answers empty from the first-position resolution, as the interface default does. Records what is written.
     */
    static final class UnorderedCheckpointStorage implements CheckpointStorage {
        private final CheckpointStorage delegate;
        private final Node node;

        UnorderedCheckpointStorage(CheckpointStorage delegate, Node node) {
            this.delegate = delegate;
            this.node = node;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (SUBSCRIPTION_ID.equals(subscriptionId)) {
                node.savedPositions.add(checkpoint.asString());
            }
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return delegate.evaluatesWriteConditionsFor(subscriptionId);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return delegate.delete(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            node.resolutions.add(candidate.asString());
            return Mono.empty();
        }
    }

    /**
     * A {@link ReactorCheckpointStorage} that records what is saved.
     */
    static final class RecordingReactorCheckpointStorage extends ReactorCheckpointStorage {
        private final Node node;

        RecordingReactorCheckpointStorage(ReactiveMongoOperations mongo, String collection, Node node) {
            super(mongo, collection);
            this.node = node;
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (SUBSCRIPTION_ID.equals(subscriptionId)) {
                node.savedPositions.add(checkpoint.asString());
            }
            return super.save(subscriptionId, checkpoint, condition);
        }
    }

    /**
     * Records the live start the starter turns into a checkpoint, and leaves the answer to whether a storage orders it to
     * the starter.
     */
    static class NodeHooks implements BeanPostProcessor, BeanFactoryAware {
        private @Nullable BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
            this.beanFactory = beanFactory;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
            if (bean instanceof LiveStartCheckpoints starter) {
                assert beanFactory != null;
                Node node = beanFactory.getBean(Node.class);
                return new LiveStartCheckpoints() {
                    @Override
                    public @Nullable Checkpoint checkpointOf(String liveStart) {
                        @Nullable Checkpoint checkpoint = starter.checkpointOf(liveStart);
                        node.liveStartCheckpoint = checkpoint;
                        return checkpoint;
                    }

                    @Override
                    public boolean ordersCheckpointsIn(Object checkpointStorage) {
                        return starter.ordersCheckpointsIn(checkpointStorage);
                    }
                };
            }
            return bean;
        }
    }

    static class DefaultStartHandler {
        private final Node node;

        DefaultStartHandler(Node node, EventStore eventStore) {
            this.node = node;
            node.beforeRegistration(eventStore);
        }

        @StreamSubscription(id = SUBSCRIPTION_ID)
        Mono<Void> on(TestEvent event) {
            node.received.add(event.name());
            return Mono.empty();
        }
    }

    static class NowSameAsStartAtHandler {
        private final Node node;

        NowSameAsStartAtHandler(Node node, EventStore eventStore) {
            this.node = node;
            node.beforeRegistration(eventStore);
        }

        @StreamSubscription(id = SUBSCRIPTION_ID, startAt = StreamSubscription.StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event) {
            node.received.add(event.name());
            return Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DefaultStartHandlerConfiguration {
        @Bean
        DefaultStartHandler defaultStartHandler(Node node, EventStore eventStore) {
            return new DefaultStartHandler(node, eventStore);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NowSameAsStartAtHandlerConfiguration {
        @Bean
        NowSameAsStartAtHandler nowSameAsStartAtHandler(Node node, EventStore eventStore) {
            return new NowSameAsStartAtHandler(node, eventStore);
        }
    }

    // Registered before the configuration that enables the starter, as an application's own configuration is, so the
    // starter's own checkpoint storage steps aside for it
    @Configuration(proxyBeanMethods = false)
    static class UnorderedStorageConfiguration {
        @Bean
        CheckpointStorage unorderedCheckpointStorage(Node node, ReactiveMongoOperations mongo, OccurrentProperties occurrentProperties) {
            UnorderedCheckpointStorage storage = new UnorderedCheckpointStorage(
                    new ReactorCheckpointStorage(mongo, occurrentProperties.getSubscription().resolveCollection()), node);
            node.storage = storage;
            return storage;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnCollectionStorageConfiguration {
        @Bean
        ReactorCheckpointStorage ownCollectionCheckpointStorage(Node node, ReactiveMongoOperations mongo) {
            RecordingReactorCheckpointStorage storage = new RecordingReactorCheckpointStorage(mongo, "the-applications-own-checkpoints", node);
            node.storage = storage;
            return storage;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    static class NodeApplication {

        @Bean
        static NodeHooks nodeHooks() {
            return new NodeHooks();
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

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
