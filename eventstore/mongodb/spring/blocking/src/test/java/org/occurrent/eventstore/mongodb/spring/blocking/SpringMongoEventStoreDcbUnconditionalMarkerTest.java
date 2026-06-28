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
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
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
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tags;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;

/**
 * Regression test for the unconditional-append write-skew gap (ADR 0021).
 * <p>
 * An unconditional {@link org.occurrent.eventstore.api.dcb.DcbEventStore#append(List)} must still bump the conflict
 * markers derived from its events. Otherwise a later conditional append on an overlapping tag observes an unchanged
 * consistency token and wrongly succeeds, even though a matching event was committed after it read the boundary.
 * <p>
 * Because the consistency token is derived from marker versions bumped at commit, this is reproducible deterministically
 * without threads: read the boundary, commit an unconditional matching append, then verify a conditional append carrying
 * the original token is rejected. With the fix the unconditional append advances the tag marker, so the token has moved;
 * without it the marker never moves and the conditional append slips through (write skew).
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbUnconditionalMarkerTest {

    private static final URI SOURCE = URI.create("urn:test:unconditional-marker");
    private static final String COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(
            new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_unconditional_marker"));

    private SpringMongoEventStore eventStore;

    @BeforeEach
    void create_event_store() {
        ConnectionString connectionString = connectionString();
        MongoClient mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        MongoTemplate template = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager txManager = new MongoTransactionManager(
                new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(txManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(template, config);
    }

    @Test
    void unconditional_append_is_detected_by_a_later_conditional_append_on_an_overlapping_tag() {
        String tag = "shared-tag";

        // A command reads the (empty) boundary.
        DcbConsistencyToken token = eventStore.read(tags(tag)).consistencyToken();

        // An unconditional append commits a matching event.
        eventStore.append(List.of(taggedEvent("UnconditionalEvent", tag)));

        // A conditional append carrying the token observed before the unconditional append must be rejected: the
        // unconditional append committed a matching event, which (with the fix) advanced the tag marker so the token has
        // changed. Without event-derived markers on the unconditional path the marker never moves and this wrongly
        // succeeds (write skew).
        DcbAppendCondition condition = failIfEventsMatch(tags(tag), token);
        Throwable thrown = catchThrowable(() ->
                eventStore.append(List.of(taggedEvent("ConditionalEvent", tag)), condition));

        assertThat(thrown)
                .as("The conditional append must fail because an unconditional append committed a matching event after "
                        + "the boundary was read. Without event-derived markers on the unconditional append, the "
                        + "consistency token does not move and the conflict is missed (write skew).")
                .isInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    private ConnectionString connectionString() {
        return new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_unconditional_marker");
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
