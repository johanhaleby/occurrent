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
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken.MarkerObservation;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEvent;

/**
 * What the reactor store answers to a consistency token that its own read didn't create. The token holds what the read saw of
 * each marker of its criteria, so a token that doesn't hold exactly those markers can't be compared, and the store
 * refuses it instead of comparing something else (ADR 149). The in-memory store compares events and accepts any token.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbTokenCheckTest {

    private static final String ACCOUNT_A = "account:a1";
    private static final DcbCriteria BOUNDARY_A = DcbCriteria.tags(tag(ACCOUNT_A));

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStoreConformanceFixture fixture;
    private DcbEventStore eventStore;

    @BeforeEach
    void create_the_event_store() {
        fixture = new ReactorMongoEventStoreConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
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
}
