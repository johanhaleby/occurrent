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
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.springboot.common.internal.LiveStartCheckpoints;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
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
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A live first start on the starter's default model, a competing-consumer model over a catch-up model over a durable
 * model, pins the start boundary as the first checkpoint and still starts at the model default. The node can get the
 * lease long after it started, and the node that held it meanwhile has delivered events and moved the stored
 * checkpoint on. The durable model of the node then resumes from the checkpoint stored by then and stores this node's
 * write version with it. Starting at the pinned boundary instead would deliver everything the other node delivered
 * once more.
 * <p>
 * The node that holds the lease meanwhile is a lease on the subscription id held from the test, which saves the
 * checkpoint it would have saved after delivering an event written after the boundary, then lets go of the lease.
 */
@DisplayName("Live start as the first checkpoint on the competing-consumer model")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class LiveStartFirstCheckpointRaceCompetingConsumerMongoTest {

    private static final String SUBSCRIPTION_ID = "competingConsumerLiveStartSubscriber";
    private static final String LOCKS_COLLECTION = "competing-consumer-locks";
    private static final URI SOURCE = URI.create("urn:occurrent:competing-consumer-live-start-first-checkpoint");
    private static final long WAIT_SECONDS = 60;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_node_that_gets_the_lease_after_its_start_resumes_from_the_checkpoint_stored_by_then_under_its_own_write_version() {
        String url = mongoDBContainer.getReplicaSetUrl("competing-consumer-live-start-" + UUID.randomUUID().toString().substring(0, 8));
        // The container prefixes the name it is given, so the database is the one the url names
        String database = requireNonNull(new ConnectionString(url).getDatabase());
        Recorder recorder = new Recorder();
        try (MongoClient client = MongoClients.create(url)) {
            SpringMongoLeaseCompetingConsumerStrategy otherNode = new SpringMongoLeaseCompetingConsumerStrategy.Builder(new MongoTemplate(client, database))
                    .collectionName(LOCKS_COLLECTION).build();
            try {
                assertThat(otherNode.registerCompetingConsumer(SUBSCRIPTION_ID, "other-node")).as("the other node holds the lease").isTrue();

                try (ConfigurableApplicationContext context = boot(recorder, url)) {
                    CheckpointStorage storage = context.getBean(CheckpointStorage.class);
                    SpringMongoLeaseCompetingConsumerStrategy strategy = context.getBean(SpringMongoLeaseCompetingConsumerStrategy.class);
                    Checkpoint boundary = recorder.boundary;
                    assertThat(boundary).as("the checkpoint the starter made of the live start").isNotNull();
                    assertThat(asString(storage.read(SUBSCRIPTION_ID))).as("the boundary is stored as the first checkpoint").isEqualTo(boundary.asString());

                    // The other node delivers the event and saves the checkpoint that follows it, while this node waits for the lease
                    Checkpoint afterE1;
                    try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> events = client.getDatabase(database).getCollection("events").watch().cursor()) {
                        write(context.getBean(EventStore.class), "e1");
                        afterE1 = new MongoResumeTokenCheckpoint(events.next().getResumeToken());
                    }
                    storage.save(SUBSCRIPTION_ID, afterE1);
                    assertThat(recorder.received).as("events the node received without the lease").isEmpty();

                    // The other node lets go, and this node gets the lease and delivers the next event
                    otherNode.unregisterCompetingConsumer(SUBSCRIPTION_ID, "other-node");

                    write(context.getBean(EventStore.class), "e2");
                    await("the event is delivered").atMost(ofSeconds(WAIT_SECONDS)).untilAsserted(() -> assertThat(recorder.received).contains("e2"));
                    await("the checkpoint of the event is saved over the one the other node saved").atMost(ofSeconds(WAIT_SECONDS))
                            .untilAsserted(() -> assertThat(asString(storage.read(SUBSCRIPTION_ID))).isNotEqualTo(afterE1.asString()));
                    assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("the write version the checkpoint of the event was saved under").isPresent().isEqualTo(strategy.fencingToken(SUBSCRIPTION_ID));
                    assertThat(recorder.received).as("events the node received, none the other node delivered").doesNotContain("e1");
                }
            } finally {
                otherNode.shutdown();
            }
        }
    }

    @Test
    void a_node_that_finds_a_resume_token_stored_after_its_boundary_read_resumes_from_its_boundary_once_it_gets_the_lease_and_its_first_save_goes_through() {
        String url = mongoDBContainer.getReplicaSetUrl("competing-consumer-live-start-" + UUID.randomUUID().toString().substring(0, 8));
        String database = requireNonNull(new ConnectionString(url).getDatabase());
        Recorder recorder = new Recorder();
        AtomicReference<Checkpoint> tokenOfTheOtherNode = new AtomicReference<>();
        try (MongoClient client = MongoClients.create(url)) {
            SpringMongoLeaseCompetingConsumerStrategy otherNode = new SpringMongoLeaseCompetingConsumerStrategy.Builder(new MongoTemplate(client, database))
                    .collectionName(LOCKS_COLLECTION).build();
            try {
                assertThat(otherNode.registerCompetingConsumer(SUBSCRIPTION_ID, "other-node")).as("the other node holds the lease").isTrue();
                // This node has read its boundary when it reads whether a checkpoint is stored. The other node, which booted
                // after that boundary, has pinned its later boundary and saved the resume token of an event it delivered by then
                recorder.beforeTheExistenceRead = storage -> {
                    try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> events = client.getDatabase(database).getCollection("events").watch().cursor()) {
                        write(requireNonNull(recorder.eventStore), "e1");
                        client.getDatabase(database).getCollection("unrelated").insertOne(new Document("_id", "moves-the-cluster-time-on"));
                        BsonTimestamp otherNodesBoundary = MongoCommons.operationTimeAfter(client.getDatabase(database).runCommand(new Document("ping", 1)));
                        storage.save(SUBSCRIPTION_ID, new MongoOperationTimeCheckpoint(otherNodesBoundary), CheckpointWriteCondition.ifAbsent());
                        write(requireNonNull(recorder.eventStore), "e2");
                        events.next();
                        Checkpoint afterE2 = new MongoResumeTokenCheckpoint(events.next().getResumeToken());
                        storage.save(SUBSCRIPTION_ID, afterE2, CheckpointWriteCondition.notOlderThan(1));
                        tokenOfTheOtherNode.set(afterE2);
                    }
                };

                try (ConfigurableApplicationContext context = boot(recorder, url)) {
                    CheckpointStorage storage = context.getBean(CheckpointStorage.class);
                    SpringMongoLeaseCompetingConsumerStrategy strategy = context.getBean(SpringMongoLeaseCompetingConsumerStrategy.class);
                    Checkpoint boundary = recorder.boundary;
                    assertThat(boundary).as("the checkpoint the starter made of the live start").isNotNull();
                    assertThat(tokenOfTheOtherNode.get()).as("the resume token the other node saved before this node read what is stored").isNotNull();
                    assertThat(asString(storage.read(SUBSCRIPTION_ID))).as("the boundary this node read governs, over the resume token of the other node").isEqualTo(boundary.asString());
                    assertThat(recorder.received).as("events the node received without the lease").isEmpty();

                    // The other node lets go, and this node gets the lease and goes on from its boundary
                    otherNode.unregisterCompetingConsumer(SUBSCRIPTION_ID, "other-node");

                    await("the events written after the boundary are delivered").atMost(ofSeconds(WAIT_SECONDS)).untilAsserted(() -> assertThat(recorder.received).contains("e1", "e2"));
                    write(context.getBean(EventStore.class), "e3");
                    await("the event is delivered").atMost(ofSeconds(WAIT_SECONDS)).untilAsserted(() -> assertThat(recorder.received).contains("e3"));
                    await("the checkpoint of the event is saved over the one the other node saved").atMost(ofSeconds(WAIT_SECONDS))
                            .untilAsserted(() -> assertThat(asString(storage.read(SUBSCRIPTION_ID))).isNotEqualTo(tokenOfTheOtherNode.get().asString()));
                    assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("the write version the checkpoint of the event was saved under").isPresent().isEqualTo(strategy.fencingToken(SUBSCRIPTION_ID));
                }
            } finally {
                otherNode.shutdown();
            }
        }
    }

    private static @Nullable String asString(@Nullable Checkpoint checkpoint) {
        return checkpoint == null ? null : checkpoint.asString();
    }

    private static void write(EventStore eventStore, String name) {
        CloudEventConverter<TestEvent> converter = new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(ReflectionCloudEventTypeMapper.qualified())
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
        eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvent(new TestEvent(UUID.randomUUID().toString(), new Date(), name)));
    }

    private static ConfigurableApplicationContext boot(Recorder recorder, String url) {
        return new SpringApplicationBuilder(NodeApplication.class)
                .initializers(context -> ((ConfigurableApplicationContext) context).getBeanFactory().registerSingleton("recorder", recorder))
                .run(
                        "--spring.mongodb.uri=" + url,
                        "--spring.main.web-application-type=none",
                        "--occurrent.event-store.capabilities=stream",
                        "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    static final class Recorder {
        final List<String> received = new CopyOnWriteArrayList<>();
        volatile @Nullable Checkpoint boundary;
        volatile @Nullable EventStore eventStore;
        // Run once, just before this node reads whether a checkpoint is stored, which comes after it read its boundary
        volatile @Nullable Consumer<CheckpointStorage> beforeTheExistenceRead;
        final AtomicBoolean existenceReadHeld = new AtomicBoolean();
    }

    /**
     * Records the checkpoint the starter makes of the live start and the event store, and runs what another node does
     * before this node reads whether a checkpoint is stored.
     */
    static class RecordingHooks implements BeanPostProcessor, BeanFactoryAware {
        private @Nullable BeanFactory beanFactory;

        @Override
        public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
            this.beanFactory = beanFactory;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
            if (bean instanceof LiveStartCheckpoints starter) {
                assert beanFactory != null;
                Recorder recorder = beanFactory.getBean(Recorder.class);
                return new LiveStartCheckpoints() {
                    @Override
                    public @Nullable Checkpoint checkpointOf(String liveStart) {
                        @Nullable Checkpoint checkpoint = starter.checkpointOf(liveStart);
                        recorder.boundary = checkpoint;
                        return checkpoint;
                    }

                    @Override
                    public boolean ordersCheckpointsIn(Object checkpointStorage) {
                        return starter.ordersCheckpointsIn(checkpointStorage);
                    }
                };
            }
            if (bean instanceof EventStore eventStore) {
                assert beanFactory != null;
                beanFactory.getBean(Recorder.class).eventStore = eventStore;
                return bean;
            }
            if (bean instanceof CheckpointStorage storage) {
                assert beanFactory != null;
                Recorder recorder = beanFactory.getBean(Recorder.class);
                return Mockito.mock(storage.getClass(), Mockito.withSettings().spiedInstance(storage).defaultAnswer(invocation -> {
                    @Nullable Consumer<CheckpointStorage> before = recorder.beforeTheExistenceRead;
                    if (before != null && invocation.getMethod().getName().equals("exists") && SUBSCRIPTION_ID.equals(invocation.getArguments()[0])
                        && recorder.existenceReadHeld.compareAndSet(false, true)) {
                        before.accept(storage);
                    }
                    return invocation.callRealMethod();
                }));
            }
            return bean;
        }
    }

    static class NewHandler {
        private final Recorder recorder;

        NewHandler(Recorder recorder) {
            this.recorder = recorder;
        }

        @StreamSubscription(id = SUBSCRIPTION_ID)
        void on(TestEvent event) {
            recorder.received.add(event.name());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    static class NodeApplication {

        @Bean
        static RecordingHooks recordingHooks() {
            return new RecordingHooks();
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
        NewHandler newHandler(Recorder recorder) {
            return new NewHandler(recorder);
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
