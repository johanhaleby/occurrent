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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
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
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.OccurrentProperties.EventStoreProperties;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.subscription.util.predicate.EveryN;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * The start boundary is pinned as a first checkpoint only over a checkpoint storage the MongoDB starter knows orders it,
 * which is {@link SpringMongoCheckpointStorage}. The boundary is read when the event store bean is created, long before
 * the subscription registers. A checkpoint another node stored in between can only be told apart from one stored before
 * by a storage that compares the two by position. Over any other storage, a pin would keep the checkpoint the other
 * node stored after the boundary was read, and the events between the two positions would reach no node. A first start
 * over such a storage begins where it did in 0.34.0, at the position its subscription model records when it subscribes,
 * and that is the same on one node as on ten.
 * <p>
 * The storage that cannot be ordered is a wrapper around a real {@link SpringMongoCheckpointStorage} on the same
 * collection. It keeps {@code ifAbsent()} atomic across nodes and answers empty from the two-argument
 * {@code resolveFirstCheckpointRace}, as a storage that does not order checkpoints does, and records what is written
 * to it.
 */
@DisplayName("Live start boundary over a checkpoint storage the starter may or may not know to order it")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class LiveStartCheckpointStorageOrderingMongoTest {

    private static final String SUBSCRIPTION_ID = "storageOrderingSubscriber";
    private static final URI SOURCE = URI.create("urn:occurrent:live-start-checkpoint-storage-ordering");
    private static final long WAIT_SECONDS = 60;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_first_start_at_default_over_a_storage_the_starter_cannot_order_takes_its_first_position_from_the_subscription_model_and_pins_nothing() {
        Node node = new Node();
        node.eventWrittenBeforeRegistration = "written-before-registration";
        try (ConfigurableApplicationContext context = boot(node, newDatabase(), UnorderedStorageConfiguration.class, DefaultStartHandlerConfiguration.class)) {
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.storage().read(SUBSCRIPTION_ID)).as("first checkpoint stored").isNotNull());
            node.write("written-after-registration");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.received).as("events the node received").contains("written-after-registration"));

            Checkpoint boundary = requireNonNull(node.liveStartCheckpoint, "the checkpoint the starter made of the live start");
            // The event was written after the boundary and before the subscription registered. Missing it is the 0.34.0 start
            // for a storage the starter cannot show to order the boundary, and the same on every node that starts this way
            assertAll(
                    () -> assertThat(node.savedPositions).as("positions saved for the subscription, the boundary pinned by the starter not among them").doesNotContain(boundary.asString()),
                    () -> assertThat(node.received).as("events the node received").doesNotContain("written-before-registration")
            );
        }
    }

    @Test
    void a_first_start_over_a_storage_the_starter_cannot_order_does_not_pin_the_boundary_over_the_checkpoint_another_node_stored_after_the_boundary_was_read() throws Exception {
        String database = newDatabase();
        Node behind = new Node();
        behind.holdRegistration = true;
        Node holding = new Node();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        @Nullable ConfigurableApplicationContext behindContext = null;
        @Nullable ConfigurableApplicationContext holdingContext = null;
        try {
            // The behind node has read its boundary when its event store bean was created, and is held before it registers
            Future<ConfigurableApplicationContext> behindBoot = pool.submit(() -> boot(behind, database, UnorderedStorageConfiguration.class, OwnSubscriptionModelConfiguration.class, DefaultStartHandlerConfiguration.class));
            assertThat(behind.registrationHeld.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the behind node read its boundary and reached its registration").isTrue();
            // Moves the cluster time on, so the holding node's boundary is strictly after the behind node's
            writeUnrelatedDocument(database);

            // The holding node registers first, finds nothing stored and stores its first checkpoint
            holdingContext = boot(holding, database, UnorderedStorageConfiguration.class, OwnSubscriptionModelConfiguration.class, DefaultStartHandlerConfiguration.class);
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(holding.storage().read(SUBSCRIPTION_ID)).as("first checkpoint stored by the holding node").isNotNull());
            holding.write("written-after-the-holding-nodes-first-checkpoint");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(holding.received).as("events the holding node received").contains("written-after-the-holding-nodes-first-checkpoint"));

            behind.mayRegister.countDown();
            behindContext = behindBoot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            CatchupSubscriptionModel behindModel = behindContext.getBean(CatchupSubscriptionModel.class);
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(behindModel.isRunning(SUBSCRIPTION_ID)).as("the behind node's subscription runs").isTrue());
            assertThat(operationTimeOf(behind.liveStart)).as("the behind node's boundary").isLessThan(operationTimeOf(holding.liveStart));
            holding.write("written-after-both-nodes-registered");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(behind.received).as("events the behind node received").contains("written-after-both-nodes-registered"));

            Checkpoint behindBoundary = requireNonNull(behind.liveStartCheckpoint, "the checkpoint the starter made of the behind node's live start");
            List<String> receivedBySomeNode = new ArrayList<>(behind.received);
            receivedBySomeNode.addAll(holding.received);
            assertAll(
                    () -> assertThat(behind.savedPositions).as("positions the behind node saved for the subscription, its boundary not among them").doesNotContain(behindBoundary.asString()),
                    () -> assertThat(behind.resolutions).as("first-position resolutions the behind node asked the storage for").isEmpty(),
                    () -> assertThat(receivedBySomeNode).as("events delivered to a node").contains("written-after-the-holding-nodes-first-checkpoint", "written-after-both-nodes-registered")
            );
        } finally {
            behind.mayRegister.countDown();
            pool.shutdownNow();
            if (behindContext != null) {
                behindContext.close();
            }
            if (holdingContext != null) {
                holdingContext.close();
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
    void a_first_start_at_default_over_a_spring_mongo_checkpoint_storage_declared_by_the_application_with_a_collection_of_its_own_pins_the_boundary_and_receives_the_event_written_before_registration() {
        Node node = new Node();
        node.eventWrittenBeforeRegistration = "written-before-registration";
        try (ConfigurableApplicationContext context = boot(node, newDatabase(), OwnCollectionStorageConfiguration.class, DefaultStartHandlerConfiguration.class)) {
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.received).as("events the node received").contains("written-before-registration"));

            Checkpoint boundary = requireNonNull(node.liveStartCheckpoint, "the checkpoint the starter made of the live start");
            assertThat(node.savedPositions).as("positions saved for the subscription, the first one the pinned boundary").first().isEqualTo(boundary.asString());
        }
    }

    private static String newDatabase() {
        return mongoDBContainer.getReplicaSetUrl("storage-ordering-" + UUID.randomUUID().toString().substring(0, 8));
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

    private static void writeUnrelatedDocument(String url) {
        ConnectionString connectionString = new ConnectionString(url);
        try (MongoClient client = MongoClients.create(connectionString)) {
            client.getDatabase(requireNonNull(connectionString.getDatabase())).getCollection("unrelated").insertOne(new Document("_id", "moves-the-cluster-time-on"));
        }
    }

    private static org.bson.BsonTimestamp operationTimeOf(@Nullable String liveStart) {
        assertThat(liveStart).as("a live start was read").isNotNull();
        return Document.parse(liveStart).get("operationTime", org.bson.BsonTimestamp.class);
    }

    private static CloudEventConverter<TestEvent> testEventConverter() {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(ReflectionCloudEventTypeMapper.qualified())
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    /**
     * What a node records about itself, and what holds it before it registers.
     */
    static final class Node {
        final List<String> received = new CopyOnWriteArrayList<>();
        final List<String> savedPositions = new CopyOnWriteArrayList<>();
        final List<String> resolutions = new CopyOnWriteArrayList<>();
        final CountDownLatch registrationHeld = new CountDownLatch(1);
        final CountDownLatch mayRegister = new CountDownLatch(1);
        volatile boolean holdRegistration;
        volatile @Nullable String eventWrittenBeforeRegistration;
        volatile @Nullable EventStore eventStore;
        volatile @Nullable String liveStart;
        volatile @Nullable Checkpoint liveStartCheckpoint;
        volatile @Nullable CheckpointStorage storage;

        CheckpointStorage storage() {
            return requireNonNull(storage, "the checkpoint storage of the node");
        }

        void write(String name) {
            requireNonNull(eventStore, "the event store of the node")
                    .write(UUID.randomUUID().toString(), testEventConverter().toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), name)));
        }

        // Runs when the handler bean is built, which is after the event store bean was created and its boundary read, and
        // before the handler is registered
        void beforeRegistration(EventStore eventStore) {
            this.eventStore = eventStore;
            @Nullable String name = eventWrittenBeforeRegistration;
            if (name != null) {
                write(name);
            }
            if (holdRegistration) {
                registrationHeld.countDown();
                try {
                    if (!mayRegister.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The node was never released, so the order was not run");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    /**
     * Delegates everything to a real {@link SpringMongoCheckpointStorage}, so {@code ifAbsent()} is atomic across nodes,
     * and answers empty from both first-position resolutions, as the interface default does. Records what is written.
     */
    static final class UnorderedCheckpointStorage implements CheckpointStorage {
        private final CheckpointStorage delegate;
        private final Node node;

        UnorderedCheckpointStorage(CheckpointStorage delegate, Node node) {
            this.delegate = delegate;
            this.node = node;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
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
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            node.resolutions.add(candidate.asString());
            return Optional.empty();
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            node.resolutions.add(candidate.asString());
            return Optional.empty();
        }
    }

    /**
     * A {@link SpringMongoCheckpointStorage} that records what is saved.
     */
    static final class RecordingSpringMongoCheckpointStorage extends SpringMongoCheckpointStorage {
        private final Node node;

        RecordingSpringMongoCheckpointStorage(MongoTemplate mongoTemplate, String collection, Node node) {
            super(mongoTemplate, collection);
            this.node = node;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
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
                        node.liveStart = liveStart;
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
        void on(TestEvent event) {
            node.received.add(event.name());
        }
    }

    static class NowSameAsStartAtHandler {
        private final Node node;

        NowSameAsStartAtHandler(Node node, EventStore eventStore) {
            this.node = node;
            node.beforeRegistration(eventStore);
        }

        @StreamSubscription(id = SUBSCRIPTION_ID, startAt = StreamSubscription.StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(TestEvent event) {
            node.received.add(event.name());
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
        CheckpointStorage unorderedCheckpointStorage(Node node, MongoTemplate mongoTemplate, OccurrentProperties occurrentProperties) {
            UnorderedCheckpointStorage storage = new UnorderedCheckpointStorage(
                    new SpringMongoCheckpointStorage(mongoTemplate, occurrentProperties.getSubscription().resolveCollection()), node);
            node.storage = storage;
            return storage;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnCollectionStorageConfiguration {
        @Bean
        SpringMongoCheckpointStorage ownCollectionCheckpointStorage(Node node, MongoTemplate mongoTemplate) {
            RecordingSpringMongoCheckpointStorage storage = new RecordingSpringMongoCheckpointStorage(mongoTemplate, "the-applications-own-checkpoints", node);
            node.storage = storage;
            return storage;
        }
    }

    // No competing consumers, so each node delivers on its own whatever the checkpoint storage holds
    @Configuration(proxyBeanMethods = false)
    static class OwnSubscriptionModelConfiguration {
        @Bean(destroyMethod = "shutdown")
        CatchupSubscriptionModel ownSubscriptionModel(MongoTemplate mongoTemplate, CheckpointStorage checkpointStorage,
                                                      EventStoreQueries eventStoreQueries, OccurrentProperties occurrentProperties) {
            EventStoreProperties eventStore = occurrentProperties.getEventStore();
            SpringMongoSubscriptionModel mongo = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventStore.resolveCollection(), eventStore.resolveTimeRepresentation()));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(mongo, checkpointStorage, new DurableSubscriptionModelConfig(EveryN.everyEvent()));
            return new CatchupSubscriptionModel(durable, eventStoreQueries, new CatchupSubscriptionModelConfig(useCheckpointStorage(checkpointStorage)));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
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
