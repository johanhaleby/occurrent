/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.*;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbTest {

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
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb"));

    private SpringMongoEventStore eventStore;

    @BeforeEach
    void create_mongo_spring_blocking_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb");
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Test
    void dcb_writes_are_visible_to_normal_event_store_queries() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        assertThat(eventStore.all(SortBy.natural(SortBy.SortDirection.ASCENDING)))
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
        CloudEvent dcbEvent = eventStore.read(tags("name:1")).events().get(0);
        assertThat(DcbCloudEvents.getTags(dcbEvent)).containsExactly("name:1");
        assertThat(dcbEvent.getExtension(DcbCloudEvents.POSITION)).isEqualTo(1L);
    }

    @Test
    void reads_events_matching_type_or_all_tags_after_sequence_position() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1", "tenant:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(
                anyOf(List.of(
                        DcbQuery.types(List.of("OrderPlaced")),
                        DcbQuery.tags(List.of("name:1", "tenant:1")))),
                DcbReadOptions.afterSequencePosition(1));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameChanged", "OrderPlaced");
        assertThat(eventStream.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void reads_tagged_events_except_excluded_types() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(tags(List.of("name:1")).excludingTypes(List.of("NameSnapshot")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
    }

    @Test
    void reads_type_and_tagged_events_except_excluded_types() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream eventStream = eventStore.read(types(List.of("NameDefined", "NameChanged")).tags(List.of("name:1")).excludingTypes(List.of("OrderPlaced")));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "NameChanged");
    }

    @Test
    void applies_excluded_types_per_query_item() {
        eventStore.append(List.of(
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(anyOf(List.of(
                DcbQuery.tags(List.of("name:1")).excludingTypes(List.of("NameSnapshot")),
                DcbQuery.tags(List.of("order:1")))));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined", "OrderPlaced");
    }

    @Test
    void rejects_append_when_matching_event_exists_after_condition_position() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        DcbEventStream readModel = eventStore.read(tags("name:1"));

        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(tags("name:1"), readModel.consistencyToken())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void append_condition_conservatively_conflicts_on_an_excluded_type_sharing_a_tag() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        DcbQuery query = tags(List.of("name:1")).excludingTypes(List.of("NameSnapshot"));
        DcbEventStream readModel = eventStore.read(query);

        // An excluded-type event is appended after the read. It does not match the query (reads exclude it precisely,
        // see reads_tagged_events_except_excluded_types), but it shares the positive tag name:1, whose marker the consistency
        // token is derived from. The token model over-approximates and conservatively conflicts: it is sound (it never
        // misses a real conflict) at the cost of a false conflict here, which self-heals through the application service
        // (it re-reads the still-excluded boundary and retries). See ADR 0021.
        eventStore.append(List.of(taggedEvent("NameSnapshot", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(query, readModel.consistencyToken())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void append_condition_rejects_non_excluded_event_types_after_condition_position() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        DcbQuery query = tags(List.of("name:1")).excludingTypes(List.of("NameSnapshot"));
        DcbEventStream readModel = eventStore.read(query);

        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                List.of(taggedEvent("NameImported", "name:1")),
                failIfEventsMatch(query, readModel.consistencyToken())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void exists_and_count_honour_the_read_options_position_window() {
        eventStore.append(List.of(taggedEvent("E", "t")));   // position 1
        eventStore.append(List.of(taggedEvent("E", "t")));   // position 2
        eventStore.append(List.of(taggedEvent("E", "t")));   // position 3

        assertThat(eventStore.count(tags("t"))).isEqualTo(3);
        assertThat(eventStore.count(tags("t"), DcbReadOptions.afterSequencePosition(1))).isEqualTo(2);
        assertThat(eventStore.count(tags("t"), DcbReadOptions.upToSequencePosition(2))).isEqualTo(2);
        assertThat(eventStore.count(tags("t"), DcbReadOptions.between(1, 2))).isEqualTo(1);

        assertThat(eventStore.exists(tags("t"))).isTrue();
        assertThat(eventStore.exists(tags("t"), DcbReadOptions.between(2, 3))).isTrue();
        assertThat(eventStore.exists(tags("t"), DcbReadOptions.afterSequencePosition(3))).isFalse();
        assertThat(eventStore.exists(tags("missing"))).isFalse();
    }

    @Test
    void match_all_condition_does_not_detect_a_tag_scoped_append_documenting_the_known_limitation() {
        // ADR 0021: a MatchAll condition is keyed on a single "all" marker that only other MatchAll appends touch, so it
        // is NOT skew-safe against a tag-scoped or type-scoped append. This test pins that documented limitation so a
        // future change cannot silently alter it.
        DcbEventStream readModel = eventStore.read(all());

        // A tag-scoped append commits after the MatchAll read. It bumps the name:1 tag and NameDefined type markers, but
        // not the "all" marker that the MatchAll condition is keyed on.
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));

        // The MatchAll condition therefore does not observe it and the append succeeds, even though an event was written
        // after the read. A tag-scoped condition on name:1 would correctly conflict here.
        DcbAppendResult result = eventStore.append(
                List.of(taggedEvent("NameChanged", "name:2")),
                failIfEventsMatch(all(), readModel.consistencyToken()));

        assertThat(result.firstSequencePosition()).isEqualTo(2);
    }

    @Test
    void no_token_append_condition_reflects_current_existence_not_past_appends() {
        DcbQuery query = tags("name:1");
        CloudEvent existing = taggedEvent("NameDefined", "name:1");
        eventStore.append(List.of(existing));

        // While a matching event exists, the no-token guard conflicts.
        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent("NameChanged", "name:1")), failIfEventsMatch(query)))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

        // After the matching event is deleted, the no-token guard succeeds again. It means "currently exists", not
        // "ever appended": the no-token path checks the live events, not the never-decremented marker versions, so it
        // matches the in-memory store and survives deletes.
        eventStore.deleteEvent(existing.getId(), existing.getSource());
        DcbAppendResult result = eventStore.append(List.of(taggedEvent("NameChanged", "name:1")), failIfEventsMatch(query));
        assertThat(result.eventCount()).isEqualTo(1);
    }

    @Test
    void abandons_reserved_position_block_when_duplicate_cloud_event_fails_insert() {
        CloudEvent cloudEvent = taggedEvent("NameDefined", "name:1");
        eventStore.append(List.of(cloudEvent));

        assertThatThrownBy(() -> eventStore.append(List.of(cloudEvent)))
                .isExactlyInstanceOf(DuplicateCloudEventException.class);

        // Positions are reserved outside the transaction (ADR 0021), so the failed append abandons its reserved block
        // and dcbposition has a gap. The next successful append lands after the abandoned block.
        DcbAppendResult appendResult = eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));
        assertThat(appendResult.firstSequencePosition()).isEqualTo(3);
        assertThat(appendResult.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void same_stale_append_condition_rejects_second_append_and_abandons_its_position_block() {
        DcbEventStream readModel = eventStore.read(tags("name:1"));
        DcbAppendCondition appendCondition = failIfEventsMatch(tags("name:1"), readModel.consistencyToken());

        DcbAppendResult firstAppend = eventStore.append(List.of(taggedEvent("NameDefined", "name:1")), appendCondition);
        assertThat(firstAppend).isEqualTo(new DcbAppendResult(1, 1, 1));

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent("NameChanged", "name:1")), appendCondition))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

        // The condition-failed append abandons its reserved position block (ADR 0021), so dcbposition has a gap and the
        // next successful append lands at position 3 rather than 2.
        DcbAppendResult nextAppend = eventStore.append(List.of(taggedEvent("NameChanged", "name:2")));
        assertThat(nextAppend).isEqualTo(new DcbAppendResult(3, 3, 1));
    }

    @Test
    void does_not_inspect_payload_when_matching_tags() {
        CloudEvent cloudEvent = DcbCloudEvents.withTags(CloudEventBuilder.v1(event("NameDefined"))
                .withDataContentType("application/json")
                .withData("{\"tags\":[\"name:1\"]}".getBytes(UTF_8))
                .build(), Set.of("name:2"));

        eventStore.append(List.of(cloudEvent));

        assertThat(eventStore.read(tags("name:1")).events()).isEmpty();
        assertThat(eventStore.read(tags("name:2")).events()).hasSize(1);
    }

    @Test
    void last_sequence_position_is_the_store_head_not_the_max_matched_position() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "name:2")));

        // The query matches only the two "name:1" events (positions 1 and 2), but the store head is 3.
        DcbEventStream matchesSome = eventStore.read(tags("name:1"));
        assertThat(matchesSome.events()).extracting(CloudEvent::getType).containsExactly("NameDefined", "NameChanged");
        assertThat(matchesSome.lastSequencePosition()).isEqualTo(3);

        // A query that matches nothing still observes the store head.
        DcbEventStream matchesNone = eventStore.read(tags("name:absent"));
        assertThat(matchesNone.events()).isEmpty();
        assertThat(matchesNone.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void exists_and_count_report_matching_dcb_events() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "order:1")));

        assertThat(eventStore.exists(tags("name:1"))).isTrue();
        assertThat(eventStore.exists(tags("absent:1"))).isFalse();
        assertThat(eventStore.count(tags("name:1"))).isEqualTo(2);
        assertThat(eventStore.count(all())).isEqualTo(3);
    }

    @Test
    void read_honors_up_to_sequence_position_upper_bound() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1"),
                taggedEvent("OrderPlaced", "name:1")));

        DcbEventStream upToTwo = eventStore.read(tags("name:1"), DcbReadOptions.upToSequencePosition(2));

        assertThat(upToTwo.events()).extracting(CloudEvent::getType).containsExactly("NameDefined", "NameChanged");
        assertThat(upToTwo.lastSequencePosition()).isEqualTo(3);
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
