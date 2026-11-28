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
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Two nodes that share one MongoDB and one checkpoint storage start the same new annotated handler at the same moment.
 * Each stores the live start its own event store read as the handler's first checkpoint. A live start is an operation
 * time, which the checkpoint storage can order, so the race ends with the earlier node's position stored and both nodes
 * up. Had the position been a checkpoint the storage cannot order, the node that lost the race would refuse to start,
 * or the later position would be what is stored and skip every event between the two.
 * <p>
 * The race is forced rather than hoped for. The nodes boot one after the other, so the live starts are known to be
 * ordered, and both then stop where the first checkpoint is written, after each has seen nothing stored and before
 * either has written. The later node writes first, which is the order that loses the earlier position unless the
 * storage settles it by comparing the two.
 */
@DisplayName("Live start as the first checkpoint on two nodes")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class LiveStartFirstCheckpointRaceMongoTest {

    private static final String SUBSCRIPTION_ID = "liveStartRaceSubscriber";
    private static final URI SOURCE = URI.create("urn:occurrent:live-start-first-checkpoint-race");
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
            writeUnrelatedDocument();
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

    private static ConfigurableApplicationContext boot(Node node) {
        return new SpringApplicationBuilder(NodeApplication.class)
                .initializers(context -> ((ConfigurableApplicationContext) context).getBeanFactory().registerSingleton("node", node))
                .run(
                        "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("live-start-first-checkpoint-race"),
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

    private static void writeUnrelatedDocument() {
        try (MongoClient client = MongoClients.create(mongoDBContainer.getReplicaSetUrl("live-start-first-checkpoint-race"))) {
            client.getDatabase("live-start-first-checkpoint-race").getCollection("unrelated").insertOne(new Document("_id", "moves-the-cluster-time-on"));
        }
    }

    private static BsonTimestamp operationTimeOf(@Nullable String liveStart) {
        assertThat(liveStart).as("a live start was read").isNotNull();
        return Document.parse(liveStart).get("operationTime", BsonTimestamp.class);
    }

    private static @Nullable String storedFirstCheckpoint(Node node) {
        CheckpointStorage storage = node.checkpointStorage;
        assertThat(storage).as("the checkpoint storage of the node").isNotNull();
        Checkpoint stored = storage.read(SUBSCRIPTION_ID);
        return stored == null ? null : stored.asString();
    }

    /**
     * What the two nodes share, which is how they are held at the write of the first checkpoint.
     */
    static final class Race {
        final CountDownLatch earlierNodeIsAtTheFirstCheckpoint = new CountDownLatch(1);
        final CyclicBarrier bothNodesFoundNothingStored = new CyclicBarrier(2);
        final CountDownLatch laterNodeHasWritten = new CountDownLatch(1);
    }

    static final class Node {
        final boolean earlierLiveStart;
        final Race race;
        final AtomicBoolean heldAtTheCheck = new AtomicBoolean();
        final AtomicBoolean heldAtTheWrite = new AtomicBoolean();
        volatile @Nullable String liveStart;
        volatile @Nullable Checkpoint liveStartCheckpoint;
        volatile @Nullable CheckpointStorage checkpointStorage;

        Node(boolean earlierLiveStart, Race race) {
            this.earlierLiveStart = earlierLiveStart;
            this.race = race;
        }

        Object onCheckpointStorageCall(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            if (arguments.length == 0 || !SUBSCRIPTION_ID.equals(arguments[0])) {
                return invocation.callRealMethod();
            }
            String method = invocation.getMethod().getName();
            if (method.equals("exists") && heldAtTheCheck.compareAndSet(false, true)) {
                Object found = invocation.callRealMethod();
                if (earlierLiveStart) {
                    race.earlierNodeIsAtTheFirstCheckpoint.countDown();
                }
                race.bothNodesFoundNothingStored.await(WAIT_SECONDS, TimeUnit.SECONDS);
                return found;
            }
            if (method.equals("save") && arguments.length == 3 && heldAtTheWrite.compareAndSet(false, true)) {
                if (earlierLiveStart && !race.laterNodeHasWritten.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The later node never wrote its first checkpoint, so the race was not run");
                }
                try {
                    return invocation.callRealMethod();
                } finally {
                    if (!earlierLiveStart) {
                        race.laterNodeHasWritten.countDown();
                    }
                }
            }
            return invocation.callRealMethod();
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
        @StreamSubscription(id = SUBSCRIPTION_ID)
        void on(TestEvent event) {
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
        NewHandler newHandler() {
            return new NewHandler();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
