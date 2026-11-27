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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Two reactive nodes that share one MongoDB and one checkpoint storage start the same new annotated handler at the
 * same moment. Each stores the live start its own event store read as the handler's first checkpoint. A live start is
 * an operation time, which the checkpoint storage can order, so the race ends with the earlier node's position stored
 * and both nodes up. Had the position been a checkpoint the storage cannot order, the node that lost the race would
 * refuse to start, or the later position would be what is stored and skip every event between the two.
 * <p>
 * The race is forced rather than hoped for. The nodes boot one after the other, so the live starts are known to be
 * ordered, and both then stop where the first checkpoint is written, after each has seen nothing stored and before
 * either has written. The later node writes first, which is the order that loses the earlier position unless the
 * storage settles it by comparing the two.
 * <p>
 * The node that lost the race goes on from the position the storage settled, even when the other node has delivered an
 * event and saved a later checkpoint by the time it subscribes. An event written between the two live starts reaches it.
 */
@DisplayName("Live start as the first checkpoint on two reactive nodes")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveLiveStartFirstCheckpointRaceMongoTest {

    private static final String SUBSCRIPTION_ID = "reactiveLiveStartRaceSubscriber";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-live-start-first-checkpoint-race");
    private static final String DATABASE_PREFIX = "live-start-race-";
    private static final long WAIT_SECONDS = 60;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void both_nodes_start_and_the_earlier_live_start_is_the_stored_first_checkpoint() throws Exception {
        Race race = new Race();
        Node earlier = new Node(true, race);
        Node later = new Node(false, race);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        @Nullable ConfigurableApplicationContext earlierContext = null;
        @Nullable ConfigurableApplicationContext laterContext = null;
        try {
            Future<ConfigurableApplicationContext> earlierBoot = pool.submit(() -> boot(earlier));
            assertThat(race.earlierNodeIsAtTheFirstCheckpoint.await(WAIT_SECONDS, TimeUnit.SECONDS))
                    .as("the earlier node reached its first checkpoint write, so its live start was read").isTrue();
            // Moves the cluster time on, so the later node's live start is strictly after the earlier node's
            writeUnrelatedDocument(race);
            Future<ConfigurableApplicationContext> laterBoot = pool.submit(() -> boot(later));

            @Nullable Throwable earlierFailure = failureOf(earlierBoot);
            @Nullable Throwable laterFailure = failureOf(laterBoot);
            earlierContext = earlierFailure == null ? earlierBoot.get() : null;
            laterContext = laterFailure == null ? laterBoot.get() : null;

            assertThat(operationTimeOf(earlier.liveStart)).as("the earlier node's live start")
                    .isLessThan(operationTimeOf(later.liveStart));
            Checkpoint expected = earlier.liveStartCheckpoint;
            assertThat(expected).as("the checkpoint the starter made of the earlier node's live start").isNotNull();
            assertAll(
                    () -> assertThat(earlierFailure).as("the node with the earlier live start started").isNull(),
                    () -> assertThat(laterFailure).as("the node with the later live start started").isNull(),
                    () -> assertThat(storedFirstCheckpoint(later)).as("the first checkpoint stored for the handler")
                            .isEqualTo(expected.asString())
            );
        } finally {
            pool.shutdownNow();
            if (earlierContext != null) {
                earlierContext.close();
            }
            if (laterContext != null) {
                laterContext.close();
            }
        }
    }

    @Test
    void the_node_that_lost_the_race_receives_an_event_written_between_the_two_live_starts_although_the_other_node_has_saved_a_later_checkpoint() throws Exception {
        Race race = new Race();
        race.holdTheEarlierNodesSecondRead = true;
        Node earlier = new Node(true, race);
        Node later = new Node(false, race);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        @Nullable ConfigurableApplicationContext earlierContext = null;
        @Nullable ConfigurableApplicationContext laterContext = null;
        try {
            Future<ConfigurableApplicationContext> earlierBoot = pool.submit(() -> boot(earlier));
            assertThat(race.earlierNodeIsAtTheFirstCheckpoint.await(WAIT_SECONDS, TimeUnit.SECONDS))
                    .as("the earlier node reached its first checkpoint write, so its live start was read").isTrue();
            write(earlier, "e1");
            Future<ConfigurableApplicationContext> laterBoot = pool.submit(() -> boot(later));
            laterContext = laterBoot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            assertThat(operationTimeOf(earlier.liveStart)).as("the earlier node's live start")
                    .isLessThan(operationTimeOf(later.liveStart));

            // The later node is live, delivers an event and saves its checkpoint, before the earlier node can read storage
            write(later, "e2");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(later.received).as("events the later node received").contains("e2"));
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(later.savesLanded.get())
                    .as("checkpoint saves the later node completed").isGreaterThan(later.savesLandedWhenDelivered.get("e2")));
            race.laterNodeHasSavedItsCheckpoint.countDown();

            earlierContext = earlierBoot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            write(later, "e3");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(earlier.received).as("events the earlier node received").contains("e3"));
            // Each node saves a checkpoint for every event after the handler returns, so wait for the last before the nodes are closed
            for (Node node : List.of(earlier, later)) {
                await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.received).as("events the node received").contains("e3"));
                await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(node.savesLanded.get())
                        .as("checkpoint saves the node completed").isGreaterThanOrEqualTo(node.received.size()));
            }

            assertThat(earlier.received).as("events the earlier node received").contains("e1");
        } finally {
            race.laterNodeHasSavedItsCheckpoint.countDown();
            pool.shutdownNow();
            if (earlierContext != null) {
                earlierContext.close();
            }
            if (laterContext != null) {
                laterContext.close();
            }
        }
    }

    @Test
    void a_node_that_finds_a_resume_token_stored_by_a_node_with_a_later_live_start_receives_an_event_written_between_the_two_live_starts() throws Exception {
        Race race = new Race();
        race.theEarlierNodesExistenceReadFollowsTheLaterNodesFirstSave = true;
        Node earlier = new Node(true, race);
        Node later = new Node(false, race);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        @Nullable ConfigurableApplicationContext earlierContext = null;
        @Nullable ConfigurableApplicationContext laterContext = null;
        try {
            // The earlier node has read its live start and is held before it reads whether a checkpoint is stored
            Future<ConfigurableApplicationContext> earlierBoot = pool.submit(() -> boot(earlier));
            assertThat(race.earlierNodeIsAtTheFirstCheckpoint.await(WAIT_SECONDS, TimeUnit.SECONDS))
                    .as("the earlier node read its live start and reached its read of the stored checkpoint").isTrue();
            write(earlier, "between-the-two-live-starts");
            Future<ConfigurableApplicationContext> laterBoot = pool.submit(() -> boot(later));
            laterContext = laterBoot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            assertThat(operationTimeOf(earlier.liveStart)).as("the earlier node's live start")
                    .isLessThan(operationTimeOf(later.liveStart));

            // The later node delivers an event and stores a resume token, before the earlier node reads what is stored
            write(later, "delivered-by-the-later-node");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(later.received).as("events the later node received").contains("delivered-by-the-later-node"));
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(later.savesLanded.get())
                    .as("checkpoint saves the later node completed").isGreaterThan(later.savesLandedWhenDelivered.get("delivered-by-the-later-node")));
            race.laterNodeHasSavedItsCheckpoint.countDown();

            earlierContext = earlierBoot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            write(later, "after-both-are-live");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(earlier.received).as("events the earlier node received").contains("after-both-are-live"));
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(later.savesLanded.get())
                    .as("checkpoint saves the later node completed").isGreaterThanOrEqualTo(later.received.size()));

            assertThat(earlier.received).as("events the earlier node received").contains("between-the-two-live-starts");
        } finally {
            race.laterNodeHasSavedItsCheckpoint.countDown();
            pool.shutdownNow();
            if (earlierContext != null) {
                earlierContext.close();
            }
            if (laterContext != null) {
                laterContext.close();
            }
        }
    }

    @Test
    void a_restart_with_a_resume_token_stored_since_before_its_live_start_goes_on_from_the_token() throws Exception {
        Race race = new Race();
        race.nothingIsHeld = true;
        Node first = new Node(true, race);
        Node restarted = new Node(false, race);
        @Nullable ConfigurableApplicationContext firstContext = null;
        @Nullable ConfigurableApplicationContext restartedContext = null;
        try {
            firstContext = boot(first);
            write(first, "delivered-before-the-restart");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(first.received).as("events the first node received").contains("delivered-before-the-restart"));
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(first.savesLanded.get())
                    .as("checkpoint saves the first node completed").isGreaterThan(first.savesLandedWhenDelivered.get("delivered-before-the-restart")));
            String tokenOfTheFirstRun = storedFirstCheckpoint(first);
            firstContext.close();
            firstContext = null;

            restartedContext = boot(restarted);
            write(restarted, "delivered-after-the-restart");
            await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(restarted.received).as("events the restarted node received").contains("delivered-after-the-restart"));

            assertThat(operationTimeOf(first.liveStart)).as("the first node's live start").isLessThan(operationTimeOf(restarted.liveStart));
            assertAll(
                    () -> assertThat(tokenOfTheFirstRun).as("the checkpoint the first run left").isNotNull(),
                    () -> assertThat(restarted.received).as("events the restarted node received").doesNotContain("delivered-before-the-restart"),
                    () -> assertThat(restartedStoredDocument(race).get("positionMovedEarlier")).as("mark of a stored position moved earlier").isNull(),
                    () -> assertThat(restartedStoredDocument(race).get("lineageOrigin")).as("lineage origin of the stored checkpoint")
                            .isEqualTo(operationTimeOf(first.liveStart))
            );
        } finally {
            if (firstContext != null) {
                firstContext.close();
            }
            if (restartedContext != null) {
                restartedContext.close();
            }
        }
    }

    private static Document restartedStoredDocument(Race race) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(race.database));
        try (MongoClient client = MongoClients.create(connectionString)) {
            @Nullable Document document = client.getDatabase(requireNonNull(connectionString.getDatabase())).getCollection("subscriptions").find(new Document("_id", SUBSCRIPTION_ID)).first();
            assertThat(document).as("the stored checkpoint document").isNotNull();
            return document;
        }
    }

    private static void write(Node node, String name) {
        EventStore eventStore = node.eventStore;
        assertThat(eventStore).as("the event store of the node").isNotNull();
        eventStore.write(UUID.randomUUID().toString(), testEventConverter(ReflectionCloudEventTypeMapper.qualified()).toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), name))).block();
    }

    private static CloudEventConverter<TestEvent> testEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    private static ConfigurableApplicationContext boot(Node node) {
        return new SpringApplicationBuilder(NodeApplication.class)
                .initializers(context -> ((ConfigurableApplicationContext) context).getBeanFactory().registerSingleton("node", node))
                .run(
                        "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(node.race.database),
                        "--spring.main.web-application-type=none",
                        "--occurrent.event-store.capabilities=stream",
                        "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    private static @Nullable Throwable failureOf(Future<?> boot) throws InterruptedException {
        try {
            boot.get(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (java.util.concurrent.TimeoutException e) {
            return e;
        }
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

    private static @Nullable String storedFirstCheckpoint(Node node) {
        CheckpointStorage storage = node.checkpointStorage;
        assertThat(storage).as("the checkpoint storage of the node").isNotNull();
        return storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block();
    }

    /**
     * What the two nodes share, which is how they are held at the write of the first checkpoint.
     */
    static final class Race {
        // Each test runs on a database of its own, so nothing one test stored can be found by the other
        final String database = DATABASE_PREFIX + UUID.randomUUID().toString().substring(0, 8);
        final CountDownLatch earlierNodeIsAtTheFirstCheckpoint = new CountDownLatch(1);
        final CyclicBarrier bothNodesFoundNothingStored = new CyclicBarrier(2);
        final CountDownLatch laterNodeHasWritten = new CountDownLatch(1);
        final CountDownLatch laterNodeHasSavedItsCheckpoint = new CountDownLatch(1);
        volatile boolean holdTheEarlierNodesSecondRead;
        // The earlier node reads whether a checkpoint is stored only once the later node has delivered an event and saved a
        // checkpoint, and the later node reads it at once, as two nodes do whose boot overlaps
        volatile boolean theEarlierNodesExistenceReadFollowsTheLaterNodesFirstSave;
        // Neither node is held, as for nodes that boot one after the other
        volatile boolean nothingIsHeld;
    }

    static final class Node {
        final boolean earlierLiveStart;
        final Race race;
        final AtomicBoolean heldAtTheCheck = new AtomicBoolean();
        final AtomicBoolean heldAtTheWrite = new AtomicBoolean();
        final AtomicBoolean heldAtTheSecondRead = new AtomicBoolean();
        final AtomicInteger savesLanded = new AtomicInteger();
        final Map<String, Integer> savesLandedWhenDelivered = new ConcurrentHashMap<>();
        final List<String> received = new CopyOnWriteArrayList<>();
        volatile @Nullable EventStore eventStore;
        volatile @Nullable String liveStart;
        volatile @Nullable Checkpoint liveStartCheckpoint;
        volatile @Nullable CheckpointStorage checkpointStorage;

        Node(boolean earlierLiveStart, Race race) {
            this.earlierLiveStart = earlierLiveStart;
            this.race = race;
        }

        // The starter asks for the stored checkpoint and the conditional write from the booting thread and blocks on
        // each, so holding the call holds the node
        @SuppressWarnings("unchecked")
        Object onCheckpointStorageCall(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            if (arguments.length == 0 || !SUBSCRIPTION_ID.equals(arguments[0])) {
                return invocation.callRealMethod();
            }
            String method = invocation.getMethod().getName();
            if (race.nothingIsHeld) {
                if (method.equals("save") && arguments.length == 3) {
                    return ((Mono<Checkpoint>) invocation.callRealMethod()).doOnSuccess(__ -> savesLanded.incrementAndGet());
                }
                return invocation.callRealMethod();
            }
            if (race.theEarlierNodesExistenceReadFollowsTheLaterNodesFirstSave && method.equals("read") && heldAtTheCheck.compareAndSet(false, true)) {
                if (!earlierLiveStart) {
                    return invocation.callRealMethod();
                }
                // The boundary was read when the event store was created, which came before this read
                race.earlierNodeIsAtTheFirstCheckpoint.countDown();
                return Mono.<Void>fromRunnable(() -> {
                    try {
                        if (!race.laterNodeHasSavedItsCheckpoint.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("The later node never saved a checkpoint, so the earlier node's read was held for nothing");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }).subscribeOn(Schedulers.boundedElastic()).then(Mono.defer(() -> {
                    try {
                        return (Mono<Checkpoint>) invocation.callRealMethod();
                    } catch (Throwable t) {
                        return Mono.error(t);
                    }
                }));
            }
            if (method.equals("read") && heldAtTheCheck.compareAndSet(false, true)) {
                Optional<Checkpoint> found = ((Mono<Checkpoint>) invocation.callRealMethod()).blockOptional();
                if (earlierLiveStart) {
                    race.earlierNodeIsAtTheFirstCheckpoint.countDown();
                }
                race.bothNodesFoundNothingStored.await(WAIT_SECONDS, TimeUnit.SECONDS);
                return Mono.justOrEmpty(found);
            }
            if (method.equals("save") && arguments.length == 3 && heldAtTheWrite.compareAndSet(false, true)) {
                if (earlierLiveStart) {
                    if (!race.laterNodeHasWritten.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The later node never wrote its first checkpoint, so the race was not run");
                    }
                    return invocation.callRealMethod();
                }
                return ((Mono<Checkpoint>) invocation.callRealMethod()).doFinally(__ -> race.laterNodeHasWritten.countDown());
            }
            if (method.equals("read") && earlierLiveStart && race.holdTheEarlierNodesSecondRead && heldAtTheSecondRead.compareAndSet(false, true)) {
                Mono<Checkpoint> read = (Mono<Checkpoint>) invocation.callRealMethod();
                return Mono.<Void>fromRunnable(() -> {
                    try {
                        if (!race.laterNodeHasSavedItsCheckpoint.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("The later node never saved a checkpoint, so the earlier node's second read was held for nothing");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }).subscribeOn(Schedulers.boundedElastic()).then(read);
            }
            if (method.equals("save") && arguments.length == 3) {
                return ((Mono<Checkpoint>) invocation.callRealMethod()).doOnSuccess(__ -> savesLanded.incrementAndGet());
            }
            return invocation.callRealMethod();
        }

        void delivered(String name) {
            savesLandedWhenDelivered.put(name, savesLanded.get());
            received.add(name);
        }
    }

    /**
     * Records the live start each node turns into a checkpoint, and holds the node at its first checkpoint write.
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
            if (bean instanceof EventStore eventStore) {
                node().eventStore = eventStore;
                return bean;
            }
            if (bean instanceof CheckpointStorage storage) {
                Node node = node();
                node.checkpointStorage = storage;
                return Mockito.mock(storage.getClass(), Mockito.withSettings().spiedInstance(storage).defaultAnswer(node::onCheckpointStorageCall));
            }
            return bean;
        }

        private Node node() {
            assert beanFactory != null;
            return beanFactory.getBean(Node.class);
        }
    }

    static class NewHandler {
        private final Node node;

        NewHandler(Node node) {
            this.node = node;
        }

        @StreamSubscription(id = SUBSCRIPTION_ID)
        Mono<Void> on(TestEvent event) {
            node.delivered(event.name());
            return Mono.empty();
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
            return testEventConverter(typeMapper);
        }

        @Bean
        NewHandler newHandler(Node node) {
            return new NewHandler(node);
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
