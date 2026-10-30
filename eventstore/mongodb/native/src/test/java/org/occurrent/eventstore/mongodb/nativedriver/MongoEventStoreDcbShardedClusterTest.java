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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;

/**
 * What the native store answers to a conditional append when it runs against a mongos. A transaction there can read the
 * markers and the events from different snapshots, so the store doesn't query the events and refuses what the markers
 * can't settle (ADR 149). The markers still settle an item that has a tag marker that didn't move.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDcbShardedClusterTest {

    private static final String TICKET_SOLD = "TicketSold";
    private static final String CONCERT_RENAMED = "ConcertRenamed";
    private static final String ENROLLED = "StudentEnrolledInCourse";
    private static final String CONCERT_K1 = "concert:k1";
    private static final DcbCriteria TICKETS_OF_K1 = DcbCriteria.type(TICKET_SOLD).tags(tag(CONCERT_K1));

    @Container
    private static final MongoDBContainer mongos =
            new MongoDBContainer(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
                    .withSharding()
                    .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");

    private MongoEventStoreConformanceFixture fixture;
    private DcbEventStore eventStore;

    @BeforeEach
    void create_the_event_store() {
        String database = "sharded_" + UUID.randomUUID().toString().replace("-", "");
        assertThat(hello(database).get("msg")).as("The container must be a mongos, or nothing below tests the sharded case").isEqualTo("isdbgrid");
        fixture = new MongoEventStoreConformanceFixture(new ConnectionString(mongos.getConnectionString() + "/" + database));
        eventStore = fixture.dcbEventStore();
    }

    @AfterEach
    void close_the_event_store() {
        fixture.close();
    }

    @Test
    void refuses_an_append_after_an_event_of_another_type_with_the_tag_because_the_events_can_t_be_asked() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)));
        CloudEvent event = taggedEvent(TICKET_SOLD, CONCERT_K1);

        assertThatThrownBy(() -> eventStore.append(List.of(event), failIfEventsMatch(token)))
                .as("The marker of the tag moved and a mongos can't run the query that would show no matching event, so the check refuses. "
                        + "A store that queries here would let this append through")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void refuses_an_append_after_another_conditional_append_on_the_same_criteria_whatever_it_appended() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        DcbConsistencyToken otherToken = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(CONCERT_RENAMED, CONCERT_K1)), failIfEventsMatch(otherToken));
        CloudEvent event = taggedEvent(TICKET_SOLD, CONCERT_K1);

        assertThatThrownBy(() -> eventStore.append(List.of(event), failIfEventsMatch(token)))
                .as("The first append incremented the marker of the criteria without matching it, and a mongos can't tell that from a match")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void accepts_an_append_after_an_event_with_only_some_of_the_tags_of_the_criteria() {
        DcbCriteria criteria = DcbCriteria.type(ENROLLED).tags(tag("course:c1"), tag("student:s1"));
        DcbConsistencyToken token = eventStore.read(criteria).consistencyToken();
        eventStore.append(List.of(taggedEvent(ENROLLED, "course:c1", "student:s2")));

        assertThatCode(() -> eventStore.append(List.of(taggedEvent(ENROLLED, "course:c1", "student:s1")), failIfEventsMatch(token)))
                .as("The marker of student s1 didn't move, so the markers rule out a matching event and nothing needs the events")
                .doesNotThrowAnyException();
    }

    @Test
    void refuses_an_append_after_an_event_that_matches_the_criteria() {
        DcbConsistencyToken token = eventStore.read(TICKETS_OF_K1).consistencyToken();
        eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)));

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent(TICKET_SOLD, CONCERT_K1)), failIfEventsMatch(token)))
                .as("The control of the cases above: a matching event is refused as well, so the store works against a mongos and doesn't refuse for another reason")
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    private static Document hello(String database) {
        try (MongoClient client = MongoClients.create(mongos.getConnectionString())) {
            return client.getDatabase(database).runCommand(new Document("hello", 1));
        }
    }
}
