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

import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.blocking.SnapshotDcbDecider;
import org.occurrent.dsl.snapshot.blocking.SnapshotDcbDeciderApplicationService;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A snapshot saved by an earlier 0.35 build holds its marker versions as a bare list, without the criteria they were
 * read with. The snapshot store can't tell which criteria that list was read with, so a decider over the store must read
 * in full and overwrite the snapshot in the new shape, instead of resuming from the list.
 */
@DisplayName("A decider over SpringMongoSnapshotStore and a snapshot saved with a list of marker versions")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = EarlierSnapshotShapeDcbDeciderMongoTest.Application.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:earlier-snapshot-shape-test"
        }
)
@Import(EarlierSnapshotShapeDcbDeciderMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class EarlierSnapshotShapeDcbDeciderMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:earlier-snapshot-shape-test");
    private static final String COLLECTION = "occurrent-snapshot-earlier-shape-counter";
    private static final Tag ACCOUNT = Tag.parse("account:earlier-shape");
    private static final int SCHEMA_VERSION = 1;

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<Incremented> converter;

    @Autowired
    private MongoOperations mongoOperations;

    @Test
    void reads_in_full_ignores_the_state_of_the_list_shaped_snapshot_and_overwrites_it_with_the_shape_that_holds_the_criteria() {
        DcbCriteria criteria = DcbCriteria.tagsAnyOf(ACCOUNT);
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        append(new Incremented(1), new Incremented(2));
        DcbMarkerVersions current = dcbEventStore.read(criteria, DcbReadOptions.fromBeginning().withVersions()).markerVersions().orElseThrow();
        long highestPosition = dcbEventStore.read(criteria).lastSequencePosition();
        plantListShapedSnapshot(key, current, highestPosition, new Document("total", 1_000));
        SpringMongoSnapshotStore<Counter> store = new SpringMongoSnapshotStore<>(mongoOperations, Counter.class, COLLECTION);
        SnapshotDcbDecider<Increment, Counter, Incremented> decider = SnapshotDcbDecider.from(
                DcbDecider.from(counter(), command -> criteria, event -> Set.of(ACCOUNT)), store, SnapshotOptions.of(SCHEMA_VERSION, SnapshotPolicy.always()));
        SnapshotDcbDeciderApplicationService<Incremented> service = new SnapshotDcbDeciderApplicationService<>(
                new GenericDcbApplicationService<>(dcbEventStore, converter, (Incremented event) -> Set.of(ACCOUNT), GenericDcbApplicationService.defaultRetryStrategy()));

        Counter[] decided = new Counter[1];
        assertThatCode(() -> decided[0] = service.executeAndReturnState(new Increment(10), decider)).doesNotThrowAnyException();

        assertThat(decided[0]).as("the state after the command, from a full read of 1 + 2 + 10 and not from the 1000 of the planted snapshot").isEqualTo(new Counter(13));
        Document stored = mongoOperations.findById(key, Document.class, COLLECTION);
        assertThat(stored).isNotNull();
        assertThat(stored.get("state", Document.class).getInteger("total")).as("the planted state was overwritten").isEqualTo(13);
        assertThat(stored.get("markerVersions")).as("the list was replaced with the shape that holds the criteria").isInstanceOf(Document.class);
        Document markerVersions = stored.get("markerVersions", Document.class);
        assertThat(markerVersions.getString("c")).isEqualTo(DcbMarkerVersions.of(criteria, current.markers()).criteria());
        assertThat(markerVersions.getList("m", Document.class)).extracting(marker -> marker.getString("k")).containsExactly("tag:" + ACCOUNT.value());
        assertThat(store.findLatest(key)).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.state()).isEqualTo(new Counter(13));
            assertThat(snapshot.markerVersions()).hasValueSatisfying(versions -> assertThat(versions.readWith(criteria)).isTrue());
        });
    }

    private void plantListShapedSnapshot(String key, DcbMarkerVersions current, long highestPosition, Document wrongState) {
        List<Document> list = new ArrayList<>();
        current.markers().forEach((markerKey, marker) -> list.add(new Document("k", markerKey).append("e", marker.epoch()).append("v", marker.version()).append("u", marker.unstamped())));
        mongoOperations.save(new Document("_id", key)
                .append("markerVersions", list)
                .append("highestFoldedPosition", highestPosition)
                .append("schemaVersion", SCHEMA_VERSION)
                .append("state", wrongState), COLLECTION);
    }

    private void append(Incremented... events) {
        List<CloudEvent> cloudEvents = java.util.Arrays.stream(events)
                .map(converter::toCloudEvent)
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(ACCOUNT)))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    private static Decider<Increment, Counter, Incremented> counter() {
        return new Decider<>() {
            @Override
            public Counter initialState() {
                return new Counter(0);
            }

            @NonNull
            @Override
            public List<Incremented> decide(@NonNull Increment command, Counter state) {
                return List.of(new Incremented(command.amount()));
            }

            @Override
            public Counter evolve(Counter state, @NonNull Incremented event) {
                return new Counter(state.total() + event.amount());
            }
        };
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class Application {
        @Bean
        CloudEventTypeMapper<Incremented> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<Incremented> converter(CloudEventTypeMapper<Incremented> typeMapper) {
            return new JacksonCloudEventConverter.Builder<Incremented>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(Incremented::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }
    }

    record Increment(int amount) {
    }

    record Counter(int total) {
    }

    record Incremented(String eventId, Date timestamp, int amount) {
        Incremented(int amount) {
            this(UUID.randomUUID().toString(), new Date(), amount);
        }
    }
}
