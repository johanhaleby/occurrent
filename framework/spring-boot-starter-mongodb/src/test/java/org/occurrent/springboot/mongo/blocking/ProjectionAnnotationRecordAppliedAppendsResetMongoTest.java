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
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The post-reset non-lie, in the two variants the plan calls for, written against {@code startAt = BEGINNING} where
 * the reset premise actually holds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>
 * decision 7, corrected by issue #865 / PR #869): a projection left at the default start position never replays on
 * Occurrent's own shipped composition, so the withheld U5 falsifiers against that configuration could never have
 * passed for the right reason. {@code startAt = StartPosition.BEGINNING} is the configuration decision 7's replay
 * guarantee actually describes.
 * <p>
 * Deleting the durable checkpoint directly rather than through {@code CancellableSubscriptions.cancelSubscription},
 * because the default {@code ResumeBehavior} would otherwise resume from an intact checkpoint rather than replay
 * again (issue #865's round-6 finding, also why {@code Projection#recordAppliedAppends}'s javadoc now says to clear
 * the checkpoint alongside declaring {@code startAt = BEGINNING}). {@code ReplicaSetReadyMongoDBContainer} scopes
 * the database name, so the checkpoint collection is reached through the same scoped connection string the
 * application itself used, not the raw database name.
 */
@DisplayName("Projection annotation (recordAppliedAppends, MongoDB, post-reset)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ProjectionAnnotationRecordAppliedAppendsResetMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void an_ordinary_reset_clears_appends_recorded_before_it_and_a_replay_never_resurrects_them() {
        String databaseName = "record-applied-appends-ordinary-reset";
        // mongoDBContainer.withReuse(true) keeps the container, and this fixed database name, alive across separate
        // test-suite runs on the same machine, so without a flush a repeat local run would layer this run's writes
        // on top of the last one's. OccurrentMongoFlush empties collections rather than dropping the database, which
        // would invalidate the change stream this test's own live subscription depends on.
        flushDatabase(databaseName);
        AppendId preResetAppendId;
        try (ConfigurableApplicationContext firstIncarnation = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName))) {
            EventStore eventStore = firstIncarnation.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = firstIncarnation.getBean(CloudEventConverter.class);
            AppliedAppendStore appliedAppendStore = firstIncarnation.getBean(AppliedAppendStore.class);

            WriteResult result = eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("one"))));
            preResetAppendId = result.appendId().orElseThrow();
            assertThat(appliedAppendStore.waitUntilApplied("recording-counter", preResetAppendId, Duration.ofSeconds(20)))
                    .as("sanity: the projection actually recorded the pre-reset append")
                    .isTrue();
        }

        deleteCheckpoint(databaseName, "recording-counter");

        try (ConfigurableApplicationContext secondIncarnation = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName))) {
            AppliedAppendStore appliedAppendStore = secondIncarnation.getBean(AppliedAppendStore.class);

            // The pre-reset append must not come back, even though the replay redelivers the very same event.
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).untilAsserted(() ->
                    assertThat(appliedAppendStore.hasApplied("recording-counter", preResetAppendId))
                            .as("a replay records nothing, and the reset cleared the pre-reset record; the wait must not lie true")
                            .isFalse());

            // The projection is not permanently broken: a genuinely live write after the reset still gets recorded.
            EventStore eventStore = secondIncarnation.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = secondIncarnation.getBean(CloudEventConverter.class);
            WriteResult postResetWrite = eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("two"))));
            AppendId postResetAppendId = postResetWrite.appendId().orElseThrow();
            assertThat(appliedAppendStore.waitUntilApplied("recording-counter", postResetAppendId, Duration.ofSeconds(20)))
                    .as("recording resumes normally for genuinely live appends after the reset")
                    .isTrue();
        }
    }

    @Test
    void a_filtered_rebuild_whose_replay_delivers_no_matching_event_is_still_cleared_by_the_scheduled_poll() {
        String databaseName = "record-applied-appends-filtered-rebuild";
        // Flushed for the same reason as the ordinary-reset test above, and doubly so here: this test pads the
        // backlog with roughly 180MB per run, so an unflushed reused container would grow that database without
        // bound across repeat runs.
        flushDatabase(databaseName);
        AppendId staleAppendId;
        try (ConfigurableApplicationContext firstIncarnation = SpringApplication.run(FilteredRecordingProjectionApplication.class, bootArgs(databaseName))) {
            AppliedAppendStore appliedAppendStore = firstIncarnation.getBean(AppliedAppendStore.class);
            // Padding: this projection's filter (NeverMatched.class) never matches any of these, but the catch-up
            // layer still has to scan through them to determine it has reached "now", giving the second incarnation's
            // replay real wall-clock duration instead of the empty-database edge case where a zero-document replay
            // can complete inside a single poll tick and leave nothing for the poll to observe (decision 7's stated
            // residual, not the case this test targets).
            EventStore eventStore = firstIncarnation.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = firstIncarnation.getBean(CloudEventConverter.class);
            // A large payload per event, not just a large count: an indexed or server-side-filtered scan can skip
            // past many small non-matching documents almost instantly, but MongoDB still has to touch each
            // document's bytes during a collection scan, so bulking up the payload forces genuine scan latency
            // rather than hoping raw count alone outruns the poll.
            String padding = "x".repeat(300_000);
            for (int i = 0; i < 600; i++) {
                eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("padding-" + i + "-" + padding))));
            }
            // This projection's filter (NeverMatched.class) matches no event this test ever writes, live or
            // replayed, so a real delivery could never have produced a record for it in the first place. Seeding one
            // directly stands in for a record a previous incarnation of this same projection id left behind, which
            // is exactly the state decision 7's reset rule has to clear: membership rows key only by projection id
            // and outlive whatever produced them.
            staleAppendId = AppendId.mint();
            appliedAppendStore.recordApplied("filtered-recording-counter", staleAppendId);
            assertThat(appliedAppendStore.hasApplied("filtered-recording-counter", staleAppendId)).isTrue();
        }

        deleteCheckpoint(databaseName, "filtered-recording-counter");

        try (ConfigurableApplicationContext secondIncarnation = SpringApplication.run(FilteredRecordingProjectionApplication.class, bootArgs(databaseName))) {
            AppliedAppendStore appliedAppendStore = secondIncarnation.getBean(AppliedAppendStore.class);

            // No delivery of any kind can reach this projection's recording wrapper (the filter matches nothing),
            // so only the registrar's scheduled poll observing isCatchingUp can ever clear the stale row. Bounded by
            // a generous multiple of the poll's default max interval (5s) rather than the per-delivery gate, which
            // does not apply here.
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertThat(appliedAppendStore.hasApplied("filtered-recording-counter", staleAppendId))
                            .as("the scheduled poll must clear a stale record left by a predecessor incarnation, even with zero deliveries")
                            .isFalse());
        }
    }


    /**
     * Deletes the durable checkpoint for {@code subscriptionId}, reached through the same scoped connection string
     * the application used ({@code ReplicaSetReadyMongoDBContainer.getReplicaSetUrl(databaseName)} scopes the
     * database name), not the raw {@code databaseName}, which would inspect an unrelated, empty database.
     */
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

    // The poll's initial interval is far below the shipped default (200ms): the filtered-rebuild test's replay
    // genuinely finishes in tens of milliseconds even with a padded backlog (confirmed by instrumenting the poll
    // directly during development, every tick at the shipped default observed replaying=false, none ever caught the
    // window), so this test needs a poll fast enough to observe a transient state real production traffic would
    // rarely make this brief.
    //
    // Max is set equal to initial rather than left at the shipped default (5s). isCatchingUp is an in-memory check,
    // so polling every 5ms for the whole 60-second wait costs nothing. Left to back off, the poll doubles its
    // interval on every live tick and reaches the old 1s cap within about 8 ticks, so a scheduling delay under load
    // that pushes those first few ticks past the (equally load-affected) catch-up window means the remaining 59
    // seconds poll once a second, with no chance left to observe a phase that already ended.
    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName,
                "--occurrent.projection.applied-append.replay-poll.initial=5ms",
                "--occurrent.projection.applied-append.replay-poll.max=5ms"
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
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-reset-test"))
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

    @SpringBootApplication
    @EnableOccurrent
    static class FilteredRecordingProjectionApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-filtered-rebuild-test"))
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
        FilteredRecordingCounterProjection filteredRecordingCounterProjection() {
            return new FilteredRecordingCounterProjection();
        }
    }

    static class RecordingCounterProjection {
        @Projection(id = "recording-counter", recordAppliedAppends = true, storeName = "counterStore", startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(Counted.class, (state, event) -> new Counter(state.count() + 1))
                    .build();
        }
    }

    /** Handles only {@link NeverMatched}, an event type this test never constructs, so its derived subscription
     * filter matches nothing, live or replayed. */
    static class FilteredRecordingCounterProjection {
        // BACKGROUND rather than the default: DEFAULT blocks SpringApplication.run() until catch-up finishes, and a
        // filtered, zero-match replay over Mongo finishes well under the poll's fastest tick (confirmed by
        // instrumenting the poll directly: every tick observed replaying=false, none ever caught it), so the
        // per-instance registrar's own scheduler never gets a chance to run concurrently with the replay under
        // DEFAULT. BACKGROUND moves the replay off the startup path onto its own thread, so it and the poll's
        // scheduler actually overlap in wall-clock time, which is what this test needs to exercise.
        @Projection(id = "filtered-recording-counter", recordAppliedAppends = true, storeName = "counterStore", startAt = StartPosition.BEGINNING, startupMode = StartupMode.BACKGROUND)
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(NeverMatched.class, (state, event) -> new Counter(state.count() + 1))
                    .build();
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

    record NeverMatched(String eventId, Date timestamp, String name) implements TestEvent {
    }
}
