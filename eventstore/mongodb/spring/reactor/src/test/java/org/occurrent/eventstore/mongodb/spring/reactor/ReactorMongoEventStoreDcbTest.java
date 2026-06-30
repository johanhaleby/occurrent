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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.*;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbreactor"));

    private ReactorMongoEventStore eventStore;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;

    @BeforeEach
    void create_reactive_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbreactor");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        eventStore = storeWith(STREAM, DCB);
    }

    private ReactorMongoEventStore storeWith(EventStoreCapability first, EventStoreCapability... rest) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(first, rest)
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    @Test
    void dcb_write_is_readable_by_tag_and_carries_position() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        DcbEventStream stream = eventStore.read(tags("name:1")).block();
        assertThat(requireNonNull(stream).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        CloudEvent event = stream.events().get(0);
        assertThat(DcbCloudEvents.getTags(event)).containsExactly("name:1");
        assertThat(event.getExtension(DcbCloudEvents.POSITION)).isEqualTo(1L);
        assertThat(stream.lastSequencePosition()).isEqualTo(1);
    }

    @Test
    void reads_events_matching_type_or_all_tags_after_sequence_position() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1", "tenant:1"),
                taggedEvent("OrderPlaced", "order:1"))).block();

        DcbEventStream stream = eventStore.read(
                anyOf(List.of(types(List.of("OrderPlaced")), tags(List.of("name:1", "tenant:1")))),
                DcbReadOptions.afterSequencePosition(1)).block();

        assertThat(requireNonNull(stream).events()).extracting(CloudEvent::getType).containsExactly("NameChanged", "OrderPlaced");
        assertThat(stream.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void reads_tagged_events_except_excluded_types() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("OrderPlaced", "order:1"))).block();

        DcbEventStream stream = eventStore.read(tags(List.of("name:1")).excludingTypes(List.of("NameSnapshot"))).block();

        assertThat(requireNonNull(stream).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
    }

    @Test
    void conditional_append_with_stale_token_is_rejected() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();
        DcbEventStream readModel = eventStore.read(tags("name:1")).block();

        // Another append on the same boundary advances the marker.
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1"))).block();

        StepVerifier.create(eventStore.append(
                        List.of(taggedEvent("NameChanged", "name:1")),
                        failIfEventsMatch(tags("name:1"), requireNonNull(readModel).consistencyToken())))
                .expectError(DcbAppendConditionNotFulfilledException.class)
                .verify();
    }

    @Test
    void conditional_append_with_fresh_token_succeeds() {
        DcbEventStream readModel = eventStore.read(tags("name:1")).block();

        DcbAppendResult result = eventStore.append(
                List.of(taggedEvent("NameDefined", "name:1")),
                failIfEventsMatch(tags("name:1"), requireNonNull(readModel).consistencyToken())).block();

        assertThat(requireNonNull(result).firstSequencePosition()).isEqualTo(1);
    }

    @Test
    void no_token_guard_rejects_when_a_matching_event_exists() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        StepVerifier.create(eventStore.append(
                        List.of(taggedEvent("NameChanged", "name:1")),
                        failIfEventsMatch(tags("name:1"))))
                .expectError(DcbAppendConditionNotFulfilledException.class)
                .verify();
    }

    @Test
    void unconditional_append_makes_a_concurrent_conditional_append_on_the_same_tag_fail() {
        // Read the boundary while empty: token is 0.
        DcbEventStream readModel = eventStore.read(tags("name:1")).block();

        // An unconditional append must still bump the tag marker, so the stale-token conditional append below is rejected.
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        StepVerifier.create(eventStore.append(
                        List.of(taggedEvent("NameChanged", "name:1")),
                        failIfEventsMatch(tags("name:1"), requireNonNull(readModel).consistencyToken())))
                .expectError(DcbAppendConditionNotFulfilledException.class)
                .verify();
    }

    @Test
    void exists_and_count_match_the_query() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("OrderPlaced", "order:1"))).block();

        assertThat(eventStore.exists(tags("name:1")).block()).isTrue();
        assertThat(eventStore.exists(tags("missing:1")).block()).isFalse();
        assertThat(eventStore.count(types(List.of("NameDefined", "OrderPlaced"))).block()).isEqualTo(2L);
    }

    @Test
    void concurrent_appends_to_disjoint_boundaries_both_succeed() {
        Long appended = Flux.range(0, 8)
                .parallel(8)
                .runOn(Schedulers.boundedElastic())
                .flatMap(i -> eventStore.append(List.of(taggedEvent("Defined", "entity:" + i))))
                .sequential()
                .count()
                .block();

        assertThat(appended).isEqualTo(8L);
        assertThat(eventStore.count(all()).block()).isEqualTo(8L);
    }

    @Test
    void dcb_methods_are_rejected_when_only_stream_is_enabled() {
        ReactorMongoEventStore streamOnly = storeWith(STREAM);
        StepVerifier.create(streamOnly.append(List.of(taggedEvent("NameDefined", "name:1"))))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void stream_methods_are_rejected_when_only_dcb_is_enabled() {
        ReactorMongoEventStore dcbOnly = storeWith(DCB);
        StepVerifier.create(dcbOnly.exists("some-stream"))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void empty_capability_set_is_rejected() {
        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(Set.of())
                .build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void same_boundary_serialization_under_contention() throws Exception {
        int threadCount = 8;
        int iterations = 3;

        for (int i = 0; i < iterations; i++) {
            String tag = "contention-" + i;
            DcbConsistencyToken boundaryToken = requireNonNull(eventStore.read(tags(tag)).block()).consistencyToken();
            DcbAppendCondition condition = failIfEventsMatch(tags(tag), boundaryToken);

            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger condFailCount = new AtomicInteger(0);
            AtomicInteger unexpectedFailCount = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    barrier.await();
                    try {
                        eventStore.append(List.of(taggedEvent("SomeEvent", tag)), condition).block();
                        successCount.incrementAndGet();
                    } catch (DcbAppendConditionNotFulfilledException e) {
                        condFailCount.incrementAndGet();
                    } catch (Exception e) {
                        unexpectedFailCount.incrementAndGet();
                    }
                    return null;
                }));
            }

            pool.shutdown();
            for (Future<?> f : futures) {
                f.get();
            }

            assertThat(successCount.get()).as("iteration %d: exactly one append wins (tag=%s)", i, tag).isEqualTo(1);
            assertThat(unexpectedFailCount.get()).as("iteration %d: no unexpected failures, transient errors must be retried internally", i).isZero();
            assertThat(condFailCount.get()).as("iteration %d: the other %d appends fail the condition", i, threadCount - 1).isEqualTo(threadCount - 1);
        }
    }

    @Test
    void appending_an_event_with_a_duplicate_id_and_source_fails_fast_without_retrying() {
        CloudEvent event = taggedEvent("NameDefined", "name:1");
        eventStore.append(List.of(event)).block();

        // The same id and source again is a duplicate CloudEvent, a business error, not a transient conflict or the
        // cold-start marker race. It must surface as DuplicateCloudEventException and must not be fed into the append
        // backoff, so it fails fast instead of running the full 15-attempt retry before giving up.
        long start = System.nanoTime();
        StepVerifier.create(eventStore.append(List.of(event)))
                .expectError(DuplicateCloudEventException.class)
                .verify(Duration.ofSeconds(10));
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();
        assertThat(elapsedMillis).as("a duplicate CloudEvent must not be retried, so it fails well before the multi-second backoff").isLessThan(3000L);
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), Set.of(tags));
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
