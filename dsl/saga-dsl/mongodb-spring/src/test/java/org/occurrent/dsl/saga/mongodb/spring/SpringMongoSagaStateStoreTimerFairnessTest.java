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
package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. Since 0.35.0 a timer records the instant of its last failed fire, and {@code nextTimerFiresAt} is the
 * earliest of the timers' effective due times, so {@code findWithDueTimers} can order by what it filters on and a
 * failing instance does not hold up the ones behind it (#1003). The timer subdocument stays readable by 0.34.0 and a
 * 0.34.0 document stays readable here, because a rolling deploy runs both versions against one collection.
 */
@Testcontainers
@DisplayName("SpringMongoSagaStateStore timer fairness")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreTimerFairnessTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-timer-fairness";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final long BASE = NOW.toEpochMilli();

    private MongoOperations mongoOperations;
    private SagaStateStore<String> stateStore;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-fair-" + UUID.randomUUID()));
        mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    private static SagaEnvelope<String> instanceWith(String sagaId, List<TimerEntry> timers) {
        return instanceWith(sagaId, timers, 1);
    }

    private static SagaEnvelope<String> instanceWith(String sagaId, List<TimerEntry> timers, long version) {
        return new SagaEnvelope<>(sagaId, "awaiting-payment", SagaStatus.ACTIVE, version, timers, Map.of(), null, NOW, NOW, null, null);
    }

    private Document rawDocument(String sagaId) {
        return requireNonNull(mongoOperations.getCollection(COLLECTION).find(new Document("_id", sagaId)).first());
    }

    private List<Document> rawTimers(String sagaId) {
        return rawDocument(sagaId).getList("timers", Document.class);
    }

    @Test
    void a_failed_attempt_marker_round_trips_on_the_timer_that_carries_it_and_not_on_the_others() {
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(
                new TimerEntry("marked", BASE - 5_000, BASE - 1_000), new TimerEntry("unmarked", BASE - 4_000))), 0);

        SagaEnvelope<String> read = stateStore.find("s1").orElseThrow();

        assertThat(read.timers()).containsExactly(new TimerEntry("marked", BASE - 5_000, BASE - 1_000L), new TimerEntry("unmarked", BASE - 4_000));
    }

    @Test
    void a_marker_written_without_the_state_round_trips_too() {
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(new TimerEntry("t", BASE - 5_000))), 0);
        SagaEnvelope<String> marked = instanceWith("s1", List.of(new TimerEntry("t", BASE - 5_000, BASE - 1_000)), 2);

        boolean saved = stateStore.compareAndSaveWithoutState("s1", marked, 1);

        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(stateStore.find("s1").orElseThrow().timers()).containsExactly(new TimerEntry("t", BASE - 5_000, BASE - 1_000L)),
                () -> assertThat(stateStore.find("s1").orElseThrow().state()).as("the state is left as it was").isEqualTo("awaiting-payment")
        );
    }

    @Test
    void the_stored_timer_subdocument_has_no_marker_field_when_the_marker_is_null() {
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(new TimerEntry("t", BASE - 5_000))), 0);

        assertThat(rawTimers("s1")).singleElement().satisfies(timer -> assertThat(timer.keySet()).containsExactlyInAnyOrder("name", "firesAtEpochMilli"));
    }

    @Test
    void the_stored_timer_subdocument_carries_the_marker_as_a_long_when_there_is_one() {
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(new TimerEntry("t", BASE - 5_000, BASE - 1_000))), 0);

        assertThat(rawTimers("s1")).singleElement().satisfies(timer -> assertAll(
                () -> assertThat(timer.keySet()).containsExactlyInAnyOrder("name", "firesAtEpochMilli", "nextAttemptAtEpochMilli"),
                () -> assertThat(timer.get("nextAttemptAtEpochMilli")).isEqualTo(BASE - 1_000)
        ));
    }

    @Test
    void nextTimerFiresAt_is_the_smallest_effective_due_time_across_timers_with_and_without_markers() {
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(
                new TimerEntry("marked-past-the-others", BASE - 9_000, BASE - 100),
                new TimerEntry("unmarked", BASE - 4_000),
                new TimerEntry("marked-before-its-firing-time", BASE - 3_000, BASE - 8_000))), 0);

        assertThat(rawDocument("s1").get("nextTimerFiresAt")).isEqualTo(BASE - 4_000);
    }

    @Test
    void nextTimerFiresAt_follows_the_marker_when_the_only_timer_has_one() {
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(new TimerEntry("t", BASE - 9_000, BASE - 100))), 0);

        assertThat(rawDocument("s1").get("nextTimerFiresAt")).isEqualTo(BASE - 100);
    }

    @Test
    void findWithDueTimers_returns_instances_in_ascending_order_of_their_effective_due_time_and_respects_the_limit() {
        stateStore.compareAndSave("fired-latest", instanceWith("fired-latest", List.of(new TimerEntry("t", BASE - 500))), 0);
        stateStore.compareAndSave("marked-late", instanceWith("marked-late", List.of(new TimerEntry("t", BASE - 9_000, BASE - 100))), 0);
        stateStore.compareAndSave("fired-earliest", instanceWith("fired-earliest", List.of(new TimerEntry("t", BASE - 7_000))), 0);
        stateStore.compareAndSave("marked-mid", instanceWith("marked-mid", List.of(new TimerEntry("t", BASE - 8_000, BASE - 3_000))), 0);
        stateStore.compareAndSave("not-due", instanceWith("not-due", List.of(new TimerEntry("t", BASE + 5_000))), 0);

        assertAll(
                () -> assertThat(stateStore.findWithDueTimers(NOW, 10)).extracting(SagaEnvelope::sagaId)
                        .containsExactly("fired-earliest", "marked-mid", "fired-latest", "marked-late"),
                () -> assertThat(stateStore.findWithDueTimers(NOW, 2)).extracting(SagaEnvelope::sagaId)
                        .containsExactly("fired-earliest", "marked-mid")
        );
    }

    @Test
    void findWithDueTimers_does_not_return_an_instance_whose_marker_is_still_in_the_future() {
        stateStore.compareAndSave("backed-off", instanceWith("backed-off", List.of(new TimerEntry("t", BASE - 9_000, BASE + 1_000))), 0);

        assertThat(stateStore.findWithDueTimers(NOW, 10)).isEmpty();
    }

    @Test
    void the_sorted_due_query_is_served_in_order_by_the_status_and_nextTimerFiresAt_index_without_an_in_memory_sort() {
        for (int i = 0; i < 5; i++) {
            stateStore.compareAndSave("s" + i, instanceWith("s" + i, List.of(new TimerEntry("t", BASE - 5_000 + i))), 0);
        }
        Document filter = new Document("status", "ACTIVE").append("nextTimerFiresAt", new Document("$lte", BASE));

        Document explain = mongoOperations.getCollection(COLLECTION).find(filter).sort(new Document("nextTimerFiresAt", 1)).limit(3).explain();

        String plan = explain.get("queryPlanner", Document.class).get("winningPlan", Document.class).toJson();
        assertAll(
                () -> assertThat(plan).contains("IXSCAN"),
                () -> assertThat(plan).doesNotContain("\"SORT\"").doesNotContain("SORT_KEY_GENERATOR"),
                () -> assertThat(plan).contains("\"status\": 1").contains("\"nextTimerFiresAt\": 1")
        );
    }

    @Test
    void a_document_written_by_0_34_0_decodes_with_no_marker_and_is_found_by_the_due_query() {
        // 0.34.0 wrote a timer as only name and firesAtEpochMilli, and nextTimerFiresAt as the earliest firesAtEpochMilli
        mongoOperations.getCollection(COLLECTION).insertOne(new Document("_id", "legacy")
                .append("status", "ACTIVE")
                .append("version", 3L)
                .append("state", "awaiting-payment")
                .append("timers", List.of(
                        new Document("name", "second").append("firesAtEpochMilli", BASE - 1_000),
                        new Document("name", "first").append("firesAtEpochMilli", BASE - 6_000)))
                .append("nextTimerFiresAt", BASE - 6_000)
                .append("streamWatermarks", new Document()));

        List<SagaEnvelope<String>> due = stateStore.findWithDueTimers(NOW, 10);

        assertAll(
                () -> assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("legacy"),
                () -> assertThat(due.get(0).timers()).containsExactly(new TimerEntry("second", BASE - 1_000, null), new TimerEntry("first", BASE - 6_000, null)),
                () -> assertThat(due.get(0).timers()).extracting(TimerEntry::nextAttemptAtEpochMilli).containsOnlyNulls(),
                () -> assertThat(stateStore.find("legacy").orElseThrow().timers()).extracting(TimerEntry::nextAttemptAtEpochMilli).containsOnlyNulls()
        );
    }

    @Test
    void a_timer_written_with_a_marker_still_has_the_name_and_firesAtEpochMilli_fields_with_the_types_0_34_0_reads() {
        // The 0.34.0 decoder reads timer.getString("name") and timer.getLong("firesAtEpochMilli") and nothing else of a timer,
        // so an instance written here must keep exactly those two fields, as a string and a long, for a node still on 0.34.0
        stateStore.compareAndSave("s1", instanceWith("s1", List.of(new TimerEntry("t", BASE - 5_000, BASE - 1_000))), 0);

        Document timer = rawTimers("s1").get(0);

        assertAll(
                () -> assertThat(timer.get("name")).isInstanceOf(String.class).isEqualTo("t"),
                () -> assertThat(timer.get("firesAtEpochMilli")).isInstanceOf(Long.class).isEqualTo(BASE - 5_000),
                () -> assertThat(rawDocument("s1").get("nextTimerFiresAt")).isInstanceOf(Long.class)
        );
    }
}
