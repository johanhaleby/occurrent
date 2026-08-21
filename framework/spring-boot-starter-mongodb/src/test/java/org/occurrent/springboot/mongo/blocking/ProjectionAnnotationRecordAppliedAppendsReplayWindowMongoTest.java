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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A write that lands while a projection is still replaying at startup, which is the case
 * <a href="https://github.com/johanhaleby/occurrent/issues/890">#890</a> reports. The catch-up delivers it through
 * the same action the history went through, and the change stream resumes past it, so that delivery is the only one
 * it gets and it has to be recorded there
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 6).
 * <p>
 * Held open with a latch rather than by racing a fast replay. The projection blocks on its first replayed event
 * until the test has written the event under test, so that write is guaranteed to land after the catch-up counted
 * what it was going to read and before it reconciles, which is the window. Racing it instead would reproduce #890
 * about as often as CI did, which is once.
 */
@DisplayName("Projection annotation (recordAppliedAppends, MongoDB, write during a startup replay)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ProjectionAnnotationRecordAppliedAppendsReplayWindowMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // Held across the two incarnations. Only the second arms them, so the first can seed history without blocking.
    private static final CountDownLatch replayReachedTheProjection = new CountDownLatch(1);
    private static final CountDownLatch releaseTheReplay = new CountDownLatch(1);
    private static volatile boolean holdTheReplay = false;

    @Test
    void a_write_that_lands_while_the_projection_is_still_replaying_is_recorded_by_the_catch_up_that_delivers_it() throws InterruptedException {
        String databaseName = "record-applied-appends-replay-window";
        flushDatabase(databaseName);

        try (ConfigurableApplicationContext seeding = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName))) {
            EventStore eventStore = seeding.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = seeding.getBean(CloudEventConverter.class);
            for (int i = 0; i < 5; i++) {
                eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("seed-" + i))));
            }
        }

        // Without this the default resume behaviour picks the checkpoint up and never replays, so there would be no
        // window to write into.
        deleteCheckpoint(databaseName, "replay-window-counter");

        holdTheReplay = true;
        try (ConfigurableApplicationContext replaying = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName))) {
            // startAt = BEGINNING resolves to a background startup, so run(..) returns with the replay in flight.
            assertThat(replayReachedTheProjection.await(60, TimeUnit.SECONDS))
                    .as("the replay must reach the projection, otherwise there is no window to write into")
                    .isTrue();

            EventStore eventStore = replaying.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = replaying.getBean(CloudEventConverter.class);
            WriteResult duringTheReplay = eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("during-the-replay"))));
            AppendId appendId = duringTheReplay.appendId().orElseThrow();

            releaseTheReplay.countDown();

            AppliedAppendStore appliedAppendStore = replaying.getBean(AppliedAppendStore.class);
            assertThat(appliedAppendStore.waitUntilApplied("replay-window-counter", appendId, Duration.ofSeconds(30)))
                    .as("the catch-up applied this append and no other delivery of it is coming, so it must be recorded")
                    .isTrue();
        }
    }

    private static void flushDatabase(String databaseName) {
        OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer, databaseName)).run();
    }

    private static void deleteCheckpoint(String databaseName, String subscriptionId) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(databaseName));
        try (MongoClient client = MongoClients.create(connectionString)) {
            client.getDatabase(connectionString.getDatabase()).getCollection("subscriptions")
                    .deleteOne(new org.bson.Document("_id", subscriptionId));
        }
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    @SpringBootApplication
    @EnableOccurrent
    static class RecordingProjectionApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-replay-window-test"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        CounterStore counterStore() {
            return new CounterStore();
        }

        @Bean
        RecordingCounterProjection recordingCounterProjection() {
            return new RecordingCounterProjection();
        }
    }

    static class RecordingCounterProjection {
        @Projection(id = "replay-window-counter", recordAppliedAppends = true, storeName = "counterStore", startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(Counted.class, (state, event) -> {
                        holdUntilTheTestHasWritten();
                        return new Counter(state.count() + 1);
                    })
                    .build();
        }

        // Blocks the first replayed event on the catch-up's own thread until the test has written the event under
        // test, so that write lands inside the window instead of racing it.
        private static void holdUntilTheTestHasWritten() {
            if (!holdTheReplay) {
                return;
            }
            holdTheReplay = false;
            replayReachedTheProjection.countDown();
            try {
                releaseTheReplay.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class CounterStore implements ViewStateRepository<Counter, String> {
        private final ConcurrentHashMap<String, Counter> store = new ConcurrentHashMap<>();

        @Override
        public Optional<Counter> findById(String id) {
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public void save(String id, Counter state) {
            store.put(id, state);
        }
    }

    record Counter(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Counted(String eventId, Date timestamp, String name) implements TestEvent {
        Counted(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
