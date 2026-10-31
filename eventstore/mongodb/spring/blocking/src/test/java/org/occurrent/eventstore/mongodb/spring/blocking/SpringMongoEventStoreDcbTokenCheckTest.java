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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.UpdateOptions;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.untaggedDcbEvent;

/**
 * What the Spring blocking store answers to a consistency token that its own read didn't create. The token holds what the read saw of
 * each marker of its criteria, so a token that doesn't hold exactly those markers can't be compared, and the store
 * refuses it instead of comparing something else (ADR 149). The in-memory store compares events and accepts any token.
 * <p>
 * Also the cases of the check that need a store or a marker the conformance suites can't build, such as markers a 0.34
 * writer incremented, events with the {@code mv} of another marker and a server that fails {@code hello}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbTokenCheckTest {

    private static final String ACCOUNT_A = "account:a1";
    private static final DcbCriteria BOUNDARY_A = DcbCriteria.tags(tag(ACCOUNT_A));
    private static final String TICKET_SOLD = "TicketSold";
    private static final String CONCERT_RENAMED = "ConcertRenamed";
    private static final String CONCERT_K1 = "concert:k1";
    private static final String CONCERT_K2 = "concert:k2";
    // The type with no event sorts before the type with one, so a check that looks only at the first moved type marker looks at the wrong one.
    private static final String TYPE_WITH_NO_EVENT = "AaTypeWithNoEvent";
    private static final String TYPE_WITH_EVENT = "BbTypeWithEvent";
    private static final String OTHER_TYPE = "CcOtherType";
    private static final DcbCriteria EITHER_TYPE = DcbCriteria.types(TYPE_WITH_NO_EVENT, TYPE_WITH_EVENT);
    private static final DcbCriteria TICKETS_OF_K1 = DcbCriteria.type(TICKET_SOLD).tags(tag(CONCERT_K1));

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStoreConformanceFixture fixture;
    private DcbEventStore eventStore;

    @BeforeEach
    void create_the_event_store() {
        fixture = new SpringMongoEventStoreConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
        eventStore = fixture.dcbEventStore();
    }

    @AfterEach
    void close_the_event_store() {
        fixture.close();
    }

    @Test
    void refuses_a_token_that_holds_a_marker_key_the_criteria_lack_and_appends_nothing() {
        DcbConsistencyToken read = eventStore.read(BOUNDARY_A).consistencyToken();
        Map<String, MarkerObservation> markers = new HashMap<>(read.markers());
        markers.put("tag:account:other", MarkerObservation.absent());
        DcbConsistencyToken withExtraKey = DcbConsistencyToken.of(BOUNDARY_A, read.version(), markers);
        CloudEvent event = taggedEvent("Deposited", ACCOUNT_A);

        assertThatThrownBy(() -> eventStore.append(List.of(event), failIfEventsMatch(withExtraKey)))
                .as("The marker keys of the token are not those of the criteria, so the store didn't create it")
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tag:account:other");

        assertThat(eventStore.read(DcbCriteria.all()).events()).as("A refused append writes nothing").isEmpty();
        assertThatCode(() -> eventStore.append(List.of(event), failIfEventsMatch(read)))
                .as("The same append with the token the read returned is accepted, or the refusal above proves nothing")
                .doesNotThrowAnyException();
    }

    @Test
    void refuses_a_token_made_with_a_version_only_for_criteria_with_marker_keys_although_no_marker_exists_and_appends_nothing() {
        DcbConsistencyToken versionOnly = DcbConsistencyToken.of(BOUNDARY_A, 0);
        CloudEvent event = taggedEvent("Deposited", ACCOUNT_A);

        assertThatThrownBy(() -> eventStore.append(List.of(event), failIfEventsMatch(versionOnly)))
                .as("A token with no markers can't be compared with the markers of the criteria, whether or not they exist")
                .isExactlyInstanceOf(IllegalArgumentException.class);

        assertThat(eventStore.read(DcbCriteria.all()).events()).as("A refused append writes nothing").isEmpty();
    }

    @Test
    void refuses_an_append_after_an_event_of_the_second_type_although_the_marker_of_the_first_type_moved_without_an_event() {
        DcbConsistencyToken token = eventStore.read(EITHER_TYPE).consistencyToken();
        DcbConsistencyToken freshToken = eventStore.read(DcbCriteria.type(TYPE_WITH_NO_EVENT)).consistencyToken();
        eventStore.append(List.of(untaggedDcbEvent(OTHER_TYPE)), failIfEventsMatch(freshToken));
        eventStore.append(List.of(untaggedDcbEvent(TYPE_WITH_EVENT)));

        assertThatThrownBy(() -> eventStore.append(List.of(untaggedDcbEvent(OTHER_TYPE)), failIfEventsMatch(token)))
                .as("The conditional append moved the marker of the first type without an event of that type, and an event of the second type matches. "
                        + "A check that asks the events about the first moved marker alone would accept this append")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void accepts_an_append_after_a_marker_of_one_type_moved_without_an_event_of_either_type() {
        DcbConsistencyToken token = eventStore.read(EITHER_TYPE).consistencyToken();
        DcbConsistencyToken freshToken = eventStore.read(DcbCriteria.type(TYPE_WITH_NO_EVENT)).consistencyToken();
        eventStore.append(List.of(untaggedDcbEvent(OTHER_TYPE)), failIfEventsMatch(freshToken));

        assertThatCode(() -> eventStore.append(List.of(untaggedDcbEvent(OTHER_TYPE)), failIfEventsMatch(token)))
                .as("The control of the case above: the marker moved and no event of either type exists")
                .doesNotThrowAnyException();
    }

    @Test
    void fails_to_create_the_store_when_hello_fails_with_another_code_than_command_not_found_although_every_other_command_works() {
        UncategorizedMongoDbException unauthorized = new UncategorizedMongoDbException("hello failed", commandFailed(13));
        ConnectionString connectionString = connectionString();

        try (MongoClient client = MongoClients.create(connectionString)) {
            MongoTemplate helloFails = new MongoTemplate(client, connectionString.getDatabase()) {
                @Override
                public Document executeCommand(Document command) {
                    if (command.containsKey("hello")) {
                        throw unauthorized;
                    }
                    return super.executeCommand(command);
                }
            };

            assertThatThrownBy(() -> new SpringMongoEventStore(helloFails, config(client, connectionString).build()))
                    .as("Only a server that doesn't know hello gets the isMaster fallback, so any other failure must stop the store from being created")
                    .isSameAs(unauthorized);
        }
    }

    @Test
    void accepts_an_append_after_an_event_of_another_type_with_the_tag_although_marker_versions_are_off() {
        try (MongoClient client = MongoClients.create(connectionString())) {
            DcbEventStore store = storeWithMarkerVersionsOff(client);
            DcbConsistencyToken token = store.read(TICKETS_OF_K1).consistencyToken();
            store.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)));

            assertThatCode(() -> store.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                    .as("The query needs only the mv of the events and the epoch of the markers, which every append writes whatever the setting says")
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void refuses_an_append_after_an_event_of_the_type_with_the_tag_although_marker_versions_are_off() {
        try (MongoClient client = MongoClients.create(connectionString())) {
            DcbEventStore store = storeWithMarkerVersionsOff(client);
            DcbConsistencyToken token = store.read(TICKETS_OF_K1).consistencyToken();
            store.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)));

            assertThatThrownBy(() -> store.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                    .as("The control of the case above: a matching event was committed after the read")
                    .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
        }
    }

    @Test
    void refuses_an_append_after_a_marker_was_incremented_the_way_0_34_does_although_the_event_after_the_read_does_not_match() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)));
        try (MongoClient client = MongoClients.create(connectionString())) {
            client.getDatabase(connectionString().getDatabase())
                    .getCollection(DcbMarkerModel.checkpointCollectionName("events"))
                    .updateOne(eq("_id", DcbMarkerModel.markerId(DcbMarkerVersionsSupport.tagKey(tag(CONCERT_K1)))), inc(DcbMarkerModel.CHECKPOINT_VERSION, 1));
        }

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("A 0.34 writer increments version and not s, and its event has no mv, so the events can't be asked for it. "
                        + "Without it the same append succeeds, since the only event after the read is of another type")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void accepts_an_append_when_an_old_matching_event_has_a_high_version_on_another_marker_than_the_one_the_token_holds() {
        eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1, CONCERT_K2)));
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        try (MongoClient client = MongoClients.create(connectionString())) {
            client.getDatabase(connectionString().getDatabase())
                    .getCollection("events")
                    .updateOne(eq("type", TICKET_SOLD), set("mv.$[element].v", 100L),
                            new UpdateOptions().arrayFilters(List.of(eq("element.k", DcbMarkerVersionsSupport.tagKey(tag(CONCERT_K2))))));
        }
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)));

        assertThatCode(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("The old event matches the criteria and has an mv element for the tag of k1 at the version the token holds, "
                        + "and another element at version 100 for k2. The key, the epoch and the version have to be one element "
                        + "of mv, or the 100 of k2 would count as a version of k1 above the token's")
                .doesNotThrowAnyException();
    }

    private static ConnectionString connectionString() {
        return new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
    }

    private static DcbEventStore storeWithMarkerVersionsOff(MongoClient client) {
        ConnectionString connectionString = connectionString();
        return new SpringMongoEventStore(new MongoTemplate(client, connectionString.getDatabase()), config(client, connectionString).resumeFromMarkerVersions(false).build());
    }

    private static EventStoreConfig.Builder config(MongoClient client, ConnectionString connectionString) {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, connectionString.getDatabase())))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB);
    }

    private static MongoCommandException commandFailed(int code) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("command failed"))
                .append("code", new BsonInt32(code));
        return new MongoCommandException(response, new ServerAddress());
    }
}
