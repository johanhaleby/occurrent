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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
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
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

/**
 * Two nodes share one MongoDB and run an application-owned {@code SubscriptionModel} without competing consumers, a
 * catch-up model over a durable model over the Mongo model that writes its checkpoints with {@code any()}. The node
 * that holds the earlier start boundary is the one that registers its subscription last.
 * <p>
 * The holding node starts first. It pins its boundary, delivers from it and saves a resume token after each event. The
 * behind node read its boundary earlier, before the holding node was up, and is held between having found nothing
 * stored and writing its first checkpoint. Its conditional write is refused when it is released, and the storage
 * settles the race by position, writing the earlier boundary over what the holding node saved. The holding node then
 * saves a later resume token over it, before the behind node subscribes. Reading storage there would start the behind
 * node after the events written between the two boundaries, which no node delivers. It starts at the checkpoint the
 * pin settled instead, so the event reaches it.
 * <p>
 * The order is forced with latches at the behind node's first checkpoint write and at its subscribe, never with sleeps.
 * The test fails on the events the behind node received, and checks on the way that each step of the order was entered.
 */
@DisplayName("Live start as the first checkpoint on two nodes with an application-owned subscription model")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class LiveStartFirstCheckpointRaceOwnSubscriptionModelMongoTest {

    private static final String SUBSCRIPTION_ID = "ownModelLiveStartRaceSubscriber";
    private static final URI SOURCE = URI.create("urn:occurrent:own-model-live-start-first-checkpoint-race");
    private static final long WAIT_SECONDS = 60;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void the_node_that_registers_last_with_the_earlier_boundary_receives_the_events_written_before_the_other_nodes_boundary() throws Exception {
        Race race = new Race();
        Node behind = new Node(true, race);
        Node holding = new Node(false, race);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        @Nullable ConfigurableApplicationContext behindContext = null;
        @Nullable ConfigurableApplicationContext holdingContext = null;
        try {
            // The behind node has read its boundary and found nothing stored, and holds before it writes its first checkpoint
            Future<ConfigurableApplicationContext> behindBoot = pool.submit(() -> boot(behind));
            assertThat(race.behindNodeIsAtItsFirstCheckpointWrite.await(WAIT_SECONDS, TimeUnit.SECONDS))
                    .as("the behind node reached its first checkpoint write, so its boundary was read").isTrue();
            write(behind, "e1");
            // Moves the cluster time on, so the boundary of the holding node is strictly after e1
            writeUnrelatedDocument(race);

            // The holding node pins its later boundary and delivers from it. It has delivered nothing yet, so the storage can still order the two
            holdingContext = pool.submit(() -> boot(holding)).get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            assertThat(operationTimeOf(behind.liveStart)).as("the behind node's boundary").isLessThan(operationTimeOf(holding.liveStart));
            assertThat(storedCheckpoint(holding)).as("the checkpoint stored by the holding node").isEqualTo(boundaryOf(holding));

            // The behind node is released and the storage settles the race by writing the earlier boundary over the later one
            race.behindNodeMayWrite.countDown();
            assertThat(race.behindNodeResolvedTheRace.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the behind node's conditional write was refused and settled by position").isTrue();
            assertThat(storedCheckpoint(holding)).as("the checkpoint stored once the race was settled").isEqualTo(boundaryOf(behind));
            assertThat(race.behindNodeIsAtItsSubscribe.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the behind node got to its subscribe, held there").isTrue();

            // The holding node saves a resume token over the earlier boundary before the behind node subscribes
            write(holding, "e2");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(holding.received).as("events the holding node received").contains("e2"));
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(storedCheckpoint(holding))
                    .as("the holding node saved a later resume token over the earlier boundary").isNotEqualTo(boundaryOf(behind)));
            race.behindNodeMaySubscribe.countDown();

            behindContext = behindBoot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            write(holding, "e3");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(behind.received).as("events the behind node received").contains("e3"));

            assertThat(holding.received).as("events the holding node received, none written before its boundary").doesNotContain("e1");
            assertThat(behind.received).as("events the behind node received").contains("e1");
        } finally {
            race.behindNodeMayWrite.countDown();
            race.behindNodeMaySubscribe.countDown();
            pool.shutdownNow();
            if (behindContext != null) {
                behindContext.close();
            }
            if (holdingContext != null) {
                holdingContext.close();
            }
        }
    }

    private static void write(Node node, String name) {
        EventStore eventStore = node.eventStore;
        assertThat(eventStore).as("the event store of the node").isNotNull();
        eventStore.write(UUID.randomUUID().toString(), testEventConverter().toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), name)));
    }

    private static CloudEventConverter<TestEvent> testEventConverter() {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(ReflectionCloudEventTypeMapper.qualified())
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    private static ConfigurableApplicationContext boot(Node node) {
        return new SpringApplicationBuilder(OwnSubscriptionModelConfiguration.class, NodeApplication.class)
                .initializers(context -> ((ConfigurableApplicationContext) context).getBeanFactory().registerSingleton("node", node))
                .run(
                        "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(node.race.database),
                        "--spring.main.web-application-type=none",
                        "--occurrent.event-store.capabilities=stream",
                        "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    private static void writeUnrelatedDocument(Race race) {
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl(race.database))) {
            client.getDatabase(race.database).getCollection("unrelated").insertOne(new Document("_id", "moves-the-cluster-time-on"));
        }
    }

    private static BsonTimestamp operationTimeOf(@Nullable String liveStart) {
        assertThat(liveStart).as("a live start was read").isNotNull();
        return Document.parse(liveStart).get("operationTime", BsonTimestamp.class);
    }

    private static @Nullable String boundaryOf(Node node) {
        Checkpoint boundary = node.liveStartCheckpoint;
        assertThat(boundary).as("the checkpoint the starter made of the live start of the node").isNotNull();
        return boundary.asString();
    }

    private static @Nullable String storedCheckpoint(Node node) {
        CheckpointStorage storage = node.checkpointStorage;
        assertThat(storage).as("the checkpoint storage of the node").isNotNull();
        Checkpoint stored = storage.read(SUBSCRIPTION_ID);
        return stored == null ? null : stored.asString();
    }

    /**
     * What the two nodes share, which is how the behind node is held.
     */
    static final class Race {
        // Nothing another run stored can be found
        final String database = "own-model-live-start-race-" + UUID.randomUUID().toString().substring(0, 8);
        final CountDownLatch behindNodeIsAtItsFirstCheckpointWrite = new CountDownLatch(1);
        final CountDownLatch behindNodeMayWrite = new CountDownLatch(1);
        final CountDownLatch behindNodeResolvedTheRace = new CountDownLatch(1);
        final CountDownLatch behindNodeIsAtItsSubscribe = new CountDownLatch(1);
        final CountDownLatch behindNodeMaySubscribe = new CountDownLatch(1);
    }

    static final class Node {
        final boolean behind;
        final Race race;
        final AtomicBoolean heldAtTheWrite = new AtomicBoolean();
        final AtomicBoolean heldAtTheSubscribe = new AtomicBoolean();
        final List<String> received = new CopyOnWriteArrayList<>();
        volatile @Nullable EventStore eventStore;
        volatile @Nullable String liveStart;
        volatile @Nullable Checkpoint liveStartCheckpoint;
        volatile @Nullable CheckpointStorage checkpointStorage;

        Node(boolean behind, Race race) {
            this.behind = behind;
            this.race = race;
        }

        Object onCheckpointStorageCall(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            if (!behind || arguments.length == 0 || !SUBSCRIPTION_ID.equals(arguments[0])) {
                return invocation.callRealMethod();
            }
            String method = invocation.getMethod().getName();
            if (method.equals("save") && arguments.length == 3 && CheckpointWriteCondition.ifAbsent().equals(arguments[2]) && heldAtTheWrite.compareAndSet(false, true)) {
                race.behindNodeIsAtItsFirstCheckpointWrite.countDown();
                if (!race.behindNodeMayWrite.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The behind node was never released, so the race was not run");
                }
                return invocation.callRealMethod();
            }
            if (method.equals("resolveFirstCheckpointRace") && arguments.length == 2) {
                try {
                    return invocation.callRealMethod();
                } finally {
                    race.behindNodeResolvedTheRace.countDown();
                }
            }
            return invocation.callRealMethod();
        }

        // Held at the model the application owns, where the start it was given is resolved, so the node subscribes
        // after the holding node saved a later checkpoint whichever way the start reads storage
        void holdTheSubscribe() {
            if (behind && heldAtTheSubscribe.compareAndSet(false, true)) {
                race.behindNodeIsAtItsSubscribe.countDown();
                try {
                    if (!race.behindNodeMaySubscribe.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The behind node was never released, so the race was not run");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    /**
     * Records the live start each node turns into a checkpoint, the event store to write through and the checkpoint
     * storage, and holds the behind node at its first checkpoint write.
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
                Node node = node();
                return (LiveStartCheckpoints) liveStart -> {
                    @Nullable Checkpoint checkpoint = starter.checkpointOf(liveStart);
                    node.liveStart = liveStart;
                    node.liveStartCheckpoint = checkpoint;
                    return checkpoint;
                };
            }
            if (bean instanceof CheckpointStorage storage) {
                Node node = node();
                node.checkpointStorage = storage;
                return Mockito.mock(storage.getClass(), Mockito.withSettings().spiedInstance(storage).defaultAnswer(node::onCheckpointStorageCall));
            }
            if (bean instanceof EventStore eventStore && node().eventStore == null) {
                node().eventStore = eventStore;
            }
            return bean;
        }

        private Node node() {
            assert beanFactory != null;
            return beanFactory.getBean(Node.class);
        }
    }

    static class HoldingCatchupSubscriptionModel extends CatchupSubscriptionModel {
        private final Node node;

        HoldingCatchupSubscriptionModel(Node node, DurableSubscriptionModel durable, EventStoreQueries eventStoreQueries, CatchupSubscriptionModelConfig config) {
            super(durable, eventStoreQueries, config);
            this.node = node;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, @Nullable StartAt startAt, Consumer<CloudEvent> action) {
            if (SUBSCRIPTION_ID.equals(subscriptionId)) {
                node.holdTheSubscribe();
            }
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (SUBSCRIPTION_ID.equals(subscriptionId)) {
                node.holdTheSubscribe();
            }
            return super.subscribePaused(subscriptionId, filter, startAt, action);
        }
    }

    static class NewHandler {
        private final Node node;

        NewHandler(Node node) {
            this.node = node;
        }

        @StreamSubscription(id = SUBSCRIPTION_ID)
        void on(TestEvent event) {
            node.received.add(event.name());
        }
    }

    // Registered before the configuration that enables the starter, as an application's own configuration is, so the
    // starter's own subscription model steps aside for it
    @Configuration(proxyBeanMethods = false)
    static class OwnSubscriptionModelConfiguration {

        // No competing consumers and no write version source, so every checkpoint is written with any()
        @Bean(destroyMethod = "shutdown")
        HoldingCatchupSubscriptionModel ownSubscriptionModel(Node node, MongoTemplate mongoTemplate, CheckpointStorage checkpointStorage,
                                                             EventStoreQueries eventStoreQueries, OccurrentProperties occurrentProperties) {
            EventStoreProperties eventStore = occurrentProperties.getEventStore();
            SpringMongoSubscriptionModel mongo = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventStore.resolveCollection(), eventStore.resolveTimeRepresentation()));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(mongo, checkpointStorage, new DurableSubscriptionModelConfig(EveryN.everyEvent()));
            CatchupSubscriptionModelConfig catchup = new CatchupSubscriptionModelConfig(useCheckpointStorage(checkpointStorage));
            return new HoldingCatchupSubscriptionModel(node, durable, eventStoreQueries, catchup);
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

        @Bean
        NewHandler newHandler(Node node) {
            return new NewHandler(node);
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
