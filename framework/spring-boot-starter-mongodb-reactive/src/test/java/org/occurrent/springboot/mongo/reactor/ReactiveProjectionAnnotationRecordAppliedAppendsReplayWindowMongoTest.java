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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive twin of {@code ProjectionAnnotationRecordAppliedAppendsReplayWindowMongoTest}, for the case
 * <a href="https://github.com/johanhaleby/occurrent/issues/890">#890</a> reports. A write lands while the projection
 * is still replaying at startup, the catch-up is the only thing that delivers it, and it has to be recorded there.
 * <p>
 * The replay is held open with a latch, which is safe on this stack for one specific reason. The reactor projection
 * DSL runs the whole update, the read, the fold and the write, on {@code Schedulers.boundedElastic()}, because the
 * view repository it wraps is a blocking one. So this blocks a boundedElastic thread rather than the reactive
 * driver's event loop, and the write below still goes through. A latch anywhere the driver runs would deadlock this
 * test against itself.
 */
@DisplayName("Reactive projection annotation (recordAppliedAppends, MongoDB, write during a startup replay)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveProjectionAnnotationRecordAppliedAppendsReplayWindowMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final CountDownLatch replayReachedTheProjection = new CountDownLatch(1);
    private static final CountDownLatch releaseTheReplay = new CountDownLatch(1);
    private static volatile boolean holdTheReplay = false;

    @Test
    void a_write_that_lands_while_the_projection_is_still_replaying_is_recorded_by_the_catch_up_that_delivers_it() throws InterruptedException {
        // No flush before this one, unlike its blocking twin: this module has no test-support flush on its
        // classpath, and it does not need one. Events left by an earlier run only make the history longer, and the
        // latch holds on whichever event comes first either way.
        String databaseName = "reactive-record-applied-appends-replay-window";

        try (ConfigurableApplicationContext seeding = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName))) {
            ApplicationService<TestEvent> applicationService = seeding.getBean(ApplicationService.class);
            for (int i = 0; i < 5; i++) {
                String name = "seed-" + i;
                applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Counted(name))).block();
            }
        }

        // Without this the default resume behaviour picks the checkpoint up and never replays, so there would be no
        // window to write into.
        deleteCheckpoint(databaseName, "reactive-replay-window-counter");

        holdTheReplay = true;
        try (ConfigurableApplicationContext replaying = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName))) {
            assertThat(replayReachedTheProjection.await(60, TimeUnit.SECONDS))
                    .as("the replay must reach the projection, otherwise there is no window to write into")
                    .isTrue();

            ApplicationService<TestEvent> applicationService = replaying.getBean(ApplicationService.class);
            WriteResult duringTheReplay = applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new Counted("during-the-replay"))).block();
            AppendId appendId = duringTheReplay.appendId().orElseThrow();

            releaseTheReplay.countDown();

            AppliedAppendStore appliedAppendStore = replaying.getBean(AppliedAppendStore.class);
            assertThat(appliedAppendStore.waitUntilApplied("reactive-replay-window-counter", appendId, Duration.ofSeconds(30)))
                    .as("the catch-up applied this append and no other delivery of it is coming, so it must be recorded")
                    .isTrue();
        }
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
    @EnableOccurrentReactive
    static class RecordingProjectionApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:reactive-record-applied-appends-replay-window-test"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        ViewStateRepository<Counter, String> counterStore() {
            ConcurrentHashMap<String, Counter> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        @Bean
        RecordingCounterProjection recordingCounterProjection() {
            return new RecordingCounterProjection();
        }
    }

    static class RecordingCounterProjection {
        @Projection(id = "reactive-replay-window-counter", recordAppliedAppends = true, storeName = "counterStore", startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(Counted.class, (state, event) -> {
                        holdUntilTheTestHasWritten();
                        return new Counter(state.count() + 1);
                    })
                    .build();
        }

        // Holds the first replayed event until the test has written the event under test, so that write lands inside
        // the window instead of racing it. See the class javadoc for why blocking here is safe.
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
