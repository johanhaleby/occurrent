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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbTagsIndexCheck;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * The startup warning of a store with {@code DCB} and without {@code STREAM} over a collection that holds stream
 * events with a position and no index on {@code dcbTags} alone. The store logs it for that collection and for no
 * other.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDcbTagsIndexWarningTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String databaseName;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger storeLogger;

    @BeforeEach
    void create_mongo_client_and_capture_the_store_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbtagsindex");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());

        logAppender = new ListAppender<>();
        logAppender.start();
        storeLogger = (Logger) LoggerFactory.getLogger(MongoEventStore.class);
        storeLogger.addAppender(logAppender);
    }

    @AfterEach
    void close_mongo_client_and_release_the_log() {
        storeLogger.detachAppender(logAppender);
        logAppender.stop();
        mongoClient.close();
    }

    @Test
    void a_dcb_only_store_warns_when_it_starts_on_a_collection_holding_positioned_stream_events_and_no_dcb_tags_index() {
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a store that cannot tell the stream events apart from DCB events by index must say so, since nothing else will")
                .containsExactly(DcbTagsIndexCheck.missingIndexMessage(EVENT_COLLECTION));
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_collection_holds_only_dcb_events() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), taggedEvent("Renamed", "name:1")));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("DCB events have a position too, and a collection with only them has no stream event for the match-all queries to read")
                .doesNotContain(DcbTagsIndexCheck.missingIndexMessage(EVENT_COLLECTION));
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_collection_has_the_dcb_tags_index() {
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION)
                .createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions().sparse(true));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the operator already did what the warning asks for, so repeating it on every startup would be noise")
                .doesNotContain(DcbTagsIndexCheck.missingIndexMessage(EVENT_COLLECTION));
    }

    private List<String> warnings() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> event.getLoggerName().equals(MongoEventStore.class.getName()))
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private MongoEventStore newStreamStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private MongoEventStore newDcbStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(DCB)
                .build();
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private static CloudEvent taggedEvent(String type, String tag) {
        return DcbCloudEvents.withTags(event(type), List.of(Tag.parse(tag)));
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
