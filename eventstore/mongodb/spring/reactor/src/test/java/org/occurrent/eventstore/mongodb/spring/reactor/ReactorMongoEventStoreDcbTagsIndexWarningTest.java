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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
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
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * The startup warning of a store with {@code DCB} and without {@code STREAM} over a collection that holds stream
 * events with a position and no usable index on {@code dcbTags} alone. The store logs it for that collection and for
 * no other.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbTagsIndexWarningTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger storeLogger;

    @BeforeEach
    void create_template_and_capture_the_store_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbtagsindexreactor");
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        mongoTemplate.dropCollection(EVENT_COLLECTION).block();
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));

        logAppender = new ListAppender<>();
        logAppender.start();
        storeLogger = (Logger) LoggerFactory.getLogger(ReactorMongoEventStore.class);
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
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a store that cannot tell the stream events apart from DCB events by index must say so, since nothing else will")
                .containsExactly(DcbTagsIndexCheck.missingIndexMessage(EVENT_COLLECTION));
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_collection_holds_only_dcb_events() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), taggedEvent("Renamed", "name:1"))).block();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("DCB events have a position too, and a collection with only them has no stream event for the match-all queries to read")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_collection_has_the_dcb_tags_index() {
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        mongoTemplate.getCollection(EVENT_COLLECTION)
                .flatMap(events -> Mono.from(events.createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions().sparse(true))))
                .block();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the operator already did what the warning asks for, so repeating it on every startup would be noise")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_dcb_tags_index_is_sparse_and_descending() {
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        mongoTemplate.getCollection(EVENT_COLLECTION)
                .flatMap(events -> Mono.from(events.createIndex(Indexes.descending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions().sparse(true))))
                .block();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the planner uses a descending index on dcbTags for a match-all read as well as an ascending one")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_dcb_tags_index_is_partial_on_dcb_tags_existing() {
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        mongoTemplate.getCollection(EVENT_COLLECTION)
                .flatMap(events -> Mono.from(events.createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD),
                        new IndexOptions().partialFilterExpression(Filters.exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD)))))
                .block();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a partial index on { dcbTags: { $exists: true } } holds every DCB event and nothing else, like the sparse one")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_warns_that_a_dcb_tags_index_that_is_not_sparse_is_unusable() {
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        Document index = createDcbTagsIndex(new IndexOptions());

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a non sparse index also holds the stream events, so it narrows nothing and the operator must hear that it is the wrong one")
                .containsExactly(DcbTagsIndexCheck.unusableIndexMessage(EVENT_COLLECTION, index));
    }

    @Test
    void a_dcb_only_store_warns_that_a_dcb_tags_index_with_a_partial_filter_expression_is_unusable() {
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        Document index = createDcbTagsIndex(new IndexOptions().partialFilterExpression(Filters.eq("type", "Defined")));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a partial index that doesn't hold every DCB event can't answer a match-all read, so the planner doesn't pick it for one")
                .containsExactly(DcbTagsIndexCheck.unusableIndexMessage(EVENT_COLLECTION, index));
    }

    @Test
    void a_dcb_only_store_warns_that_a_hidden_dcb_tags_index_is_unusable() {
        assumeTrue(serverIsAtLeast(4, 4), "hidden indexes need MongoDB 4.4");
        newStreamStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        Document index = createDcbTagsIndex(new IndexOptions().sparse(true).hidden(true));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the planner ignores a hidden index, so the operator who created it must hear that it does nothing until it is unhidden")
                .containsExactly(DcbTagsIndexCheck.unusableIndexMessage(EVENT_COLLECTION, index));
    }

    @Test
    void a_dcb_only_store_says_nothing_about_an_unusable_dcb_tags_index_when_the_collection_holds_only_dcb_events() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), taggedEvent("Renamed", "name:1"))).block();
        createDcbTagsIndex(new IndexOptions());

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("with no stream event to read, the index being unusable costs nothing")
                .isEmpty();
    }

    private Document createDcbTagsIndex(IndexOptions options) {
        return mongoTemplate.getCollection(EVENT_COLLECTION)
                .flatMap(events -> Mono.from(events.createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), options))
                        .then(Flux.from(events.listIndexes()).filter(index -> index.get("key", Document.class).equals(new Document(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, 1))).next()))
                .block();
    }

    private boolean serverIsAtLeast(int major, int minor) {
        Document buildInfo = Mono.from(mongoClient.getDatabase("admin").runCommand(new Document("buildInfo", 1))).block();
        List<?> version = requireNonNull(buildInfo).getList("versionArray", Object.class);
        int serverMajor = ((Number) version.get(0)).intValue();
        int serverMinor = ((Number) version.get(1)).intValue();
        return serverMajor > major || (serverMajor == major && serverMinor >= minor);
    }

    private List<String> warnings() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> event.getLoggerName().equals(ReactorMongoEventStore.class.getName()))
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private ReactorMongoEventStore newStreamStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    private ReactorMongoEventStore newDcbStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(DCB)
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
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
