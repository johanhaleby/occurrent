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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
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
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tags;

/**
 * Regression test for the unconditional-append write-skew gap (ADR 0021), native driver variant.
 * <p>
 * An unconditional {@link org.occurrent.eventstore.api.dcb.DcbEventStore#append(List)} must still bump the conflict
 * markers derived from its events. Otherwise a later conditional append on an overlapping tag observes an unchanged
 * consistency token and wrongly succeeds, even though a matching event was committed after it read the boundary.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDcbUnconditionalMarkerTest {

    private static final URI SOURCE = URI.create("urn:test:unconditional-marker");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(
            new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_unconditional_marker"));

    private MongoEventStore eventStore;
    private MongoClient mongoClient;

    @BeforeEach
    void create_event_store() {
        ConnectionString connectionString = connectionString();
        mongoClient = MongoClients.create(connectionString);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new MongoEventStore(mongoClient, requireNonNull(connectionString.getDatabase()), "events", config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void unconditional_append_is_detected_by_a_later_conditional_append_on_an_overlapping_tag() {
        String tag = "shared-tag";

        // A command reads the (empty) boundary.
        DcbConsistencyToken token = eventStore.read(tags(tag)).consistencyToken();

        // An unconditional append commits a matching event.
        eventStore.append(List.of(taggedEvent("UnconditionalEvent", tag)));

        // A conditional append carrying the token observed before the unconditional append must be rejected: the
        // unconditional append committed a matching event, which advanced the tag marker so the token has changed.
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
