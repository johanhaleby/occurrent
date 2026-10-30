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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
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
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
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
class SpringMongoEventStoreDcbTagsIndexWarningTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";
    private static final String DCB_TAGS = DcbDocumentMapper.DCB_TAGS_INDEX_FIELD;
    private static final Pattern PRINTED_COMMANDS = Pattern.compile("(?s) Run(?:, in this order,)? (.*?)(?:\\. Dropping an index removes it.*)?$");
    private static final Pattern RUN_COMMAND = Pattern.compile("^db\\.runCommand\\((.*)\\)$");
    private static final Pattern DROP_INDEX = Pattern.compile("^db\\.(\\w+)\\.dropIndex\\(\"(.*)\"\\)$");
    private static final Pattern CREATE_INDEX = Pattern.compile("^db\\.(\\w+)\\.createIndex\\((\\{.*?\\}), (\\{.*\\})\\)$");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Container
    private static final GenericContainer<?> restrictedMongoContainer = DcbTagsIndexRestrictedMongo.container();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoClient restrictedClient;
    private String databaseName;
    private MongoTemplate mongoTemplate;
    private MongoTransactionManager transactionManager;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger storeLogger;

    @BeforeEach
    void create_template_and_capture_the_store_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbtagsindex");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION).drop();
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        transactionManager = new CommitRetryingMongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));

        logAppender = new ListAppender<>();
        logAppender.start();
        storeLogger = (Logger) LoggerFactory.getLogger(SpringMongoEventStore.class);
        storeLogger.addAppender(logAppender);
    }

    @AfterEach
    void close_mongo_client_and_release_the_log() {
        storeLogger.detachAppender(logAppender);
        logAppender.stop();
        mongoClient.close();
        if (restrictedClient != null) {
            restrictedClient.close();
        }
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
                .isEmpty();
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
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_dcb_tags_index_is_sparse_and_descending() {
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION)
                .createIndex(Indexes.descending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions().sparse(true));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the planner uses a descending index on dcbTags for a match-all read as well as an ascending one")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_dcb_tags_index_is_partial_on_dcb_tags_existing() {
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION)
                .createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD),
                        new IndexOptions().partialFilterExpression(Filters.exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD)));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a partial index on { dcbTags: { $exists: true } } holds every DCB event and nothing else, like the sparse one")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_warns_that_a_dcb_tags_index_that_is_not_sparse_is_unusable() {
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        Document index = createDcbTagsIndex(new IndexOptions());

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a non sparse index also holds the stream events, so it narrows nothing and the operator must hear that it is the wrong one")
                .containsExactly(DcbTagsIndexCheck.unusableIndexMessage(EVENT_COLLECTION, List.of(index)));
    }

    @Test
    void a_dcb_only_store_warns_that_a_dcb_tags_index_with_a_partial_filter_expression_is_unusable() {
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        Document index = createDcbTagsIndex(new IndexOptions().partialFilterExpression(Filters.eq("type", "Defined")));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a partial index that doesn't hold every DCB event can't answer a match-all read, so the planner doesn't pick it for one")
                .containsExactly(DcbTagsIndexCheck.unusableIndexMessage(EVENT_COLLECTION, List.of(index)));
    }

    @Test
    void a_dcb_only_store_warns_that_a_hidden_dcb_tags_index_is_unusable() {
        assumeTrue(serverIsAtLeast(4, 4), "hidden indexes need MongoDB 4.4");
        newStreamStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        Document index = createDcbTagsIndex(new IndexOptions().sparse(true).hidden(true));

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the planner ignores a hidden index, so the operator who created it must hear that it does nothing until it is unhidden")
                .containsExactly(DcbTagsIndexCheck.unusableIndexMessage(EVENT_COLLECTION, List.of(index)));
    }

    @Test
    void a_dcb_only_store_says_nothing_about_an_unusable_dcb_tags_index_when_the_collection_holds_only_dcb_events() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), taggedEvent("Renamed", "name:1")));
        createDcbTagsIndex(new IndexOptions());

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("with no stream event to read, the index being unusable costs nothing")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_warns_once_and_names_both_indexes_when_two_dcb_tags_indexes_are_unusable() {
        insertPositionedStreamEvent(events());
        createTwoUnusableDcbTagsIndexes();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the operator who drops only the index the warning names would still have the other one, so it must name both")
                .singleElement()
                .asString()
                .contains("'legacy_tags'", "'dcbTags_1'");
    }

    @Test
    void the_commands_printed_for_two_unusable_dcb_tags_indexes_run_on_a_real_collection_and_leave_one_index_on_dcb_tags_alone() {
        insertPositionedStreamEvent(events());
        createTwoUnusableDcbTagsIndexes();
        logAppender.list.clear();
        newDcbStore();
        List<String> commands = printedCommands(warnings().get(0));

        Throwable thrown = catchThrowable(() -> commands.forEach(this::runPrintedCommand));

        assertAll(
                () -> assertThat(thrown).as("every printed command must be accepted by MongoDB, in the printed order").isNull(),
                () -> assertThat(indexesOnDcbTagsAlone()).extracting(index -> index.getString("name"))
                        .as("createIndex fails while an index is still named dcbTags_1, so the drops must come first and leave only the new index")
                        .containsExactly("dcbTags_1")
        );
    }

    @Test
    void a_dcb_only_store_says_nothing_after_the_commands_printed_for_two_unusable_dcb_tags_indexes_have_run() {
        insertPositionedStreamEvent(events());
        createTwoUnusableDcbTagsIndexes();
        logAppender.list.clear();
        newDcbStore();
        printedCommands(warnings().get(0)).forEach(this::runPrintedCommand);

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the operator did what the warning said, so the next startup must be quiet")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_prints_the_unhide_and_the_drop_when_one_dcb_tags_index_is_hidden_and_another_is_not_sparse() {
        assumeTrue(serverIsAtLeast(4, 4), "hidden indexes need MongoDB 4.4");
        insertPositionedStreamEvent(events());
        createHiddenAndNonSparseDcbTagsIndexes();

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("a hidden index that holds only DCB events is unhidden, since that builds nothing, and the non sparse one is dropped")
                .singleElement()
                .satisfies(warning -> assertThat(printedCommands(warning)).containsExactly(
                        "db.runCommand({ collMod: \"" + EVENT_COLLECTION + "\", index: { name: \"hidden_tags\", hidden: false } })",
                        "db." + EVENT_COLLECTION + ".dropIndex(\"legacy_tags\")"));
    }

    @Test
    void the_commands_printed_for_a_hidden_and_a_non_sparse_dcb_tags_index_run_on_a_real_collection_and_leave_one_index_on_dcb_tags_alone() {
        assumeTrue(serverIsAtLeast(4, 4), "hidden indexes need MongoDB 4.4");
        insertPositionedStreamEvent(events());
        createHiddenAndNonSparseDcbTagsIndexes();
        logAppender.list.clear();
        newDcbStore();
        List<String> commands = printedCommands(warnings().get(0));

        Throwable thrown = catchThrowable(() -> commands.forEach(this::runPrintedCommand));

        assertAll(
                () -> assertThat(thrown).as("every printed command must be accepted by MongoDB, in the printed order").isNull(),
                () -> assertThat(indexesOnDcbTagsAlone()).extracting(index -> index.getString("name"))
                        .as("the unhidden index is the one left")
                        .containsExactly("hidden_tags"),
                () -> assertThat(indexesOnDcbTagsAlone()).extracting(index -> index.get("hidden"))
                        .as("MongoDB leaves out the hidden option on an index that is not hidden")
                        .doesNotContain(true)
        );
    }

    @Test
    void a_dcb_only_store_says_nothing_after_the_commands_printed_for_a_hidden_and_a_non_sparse_dcb_tags_index_have_run() {
        assumeTrue(serverIsAtLeast(4, 4), "hidden indexes need MongoDB 4.4");
        insertPositionedStreamEvent(events());
        createHiddenAndNonSparseDcbTagsIndexes();
        logAppender.list.clear();
        newDcbStore();
        printedCommands(warnings().get(0)).forEach(this::runPrintedCommand);

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("the operator did what the warning said, so the next startup must be quiet")
                .isEmpty();
    }

    @Test
    void a_dcb_only_store_says_nothing_when_the_dcb_tags_index_is_partial_on_an_and_of_dcb_tags_existing() {
        insertPositionedStreamEvent(events());
        Document andOfExistsDcbTags = Document.parse("{ \"$and\": [ { \"dcbTags\": { \"$exists\": true } } ] }");
        Document stored = createDcbTagsIndex(new IndexOptions().partialFilterExpression(andOfExistsDcbTags));
        System.out.println("DCB_TAGS_INDEX_AND_FORM stored=" + stored.toJson());
        assertThat(stored.get("partialFilterExpression"))
                .as("the form MongoDB stores for the $and is what the check sees, so this fails if MongoDB rewrites it")
                .isEqualTo(andOfExistsDcbTags);

        logAppender.list.clear();
        newDcbStore();

        assertThat(warnings())
                .as("an $and of the match-all predicate holds every DCB event and nothing else, like the bare predicate")
                .isEmpty();
    }

    @Test
    void the_spring_blocking_store_starts_when_the_role_cannot_list_indexes_on_a_collection_holding_a_positioned_stream_event() {
        givenARestrictedClientOnACollectionHoldingAPositionedStreamEvent();

        Throwable thrown = catchThrowable(() -> newDcbStore(restrictedClient, DcbTagsIndexRestrictedMongo.DATABASE));

        assertThat(thrown)
                .as("the check is only a warning, so a role that cannot list indexes must not stop the store from starting")
                .isNull();
    }

    @Test
    void the_spring_blocking_store_logs_that_the_check_failed_and_no_index_warning_when_the_role_cannot_list_indexes_on_a_collection_holding_a_positioned_stream_event() {
        givenARestrictedClientOnACollectionHoldingAPositionedStreamEvent();

        logAppender.list.clear();
        newDcbStore(restrictedClient, DcbTagsIndexRestrictedMongo.DATABASE);

        assertAll(
                () -> assertThat(warnings())
                        .as("the operator must hear that the store could not check, and must not hear a guess about the indexes")
                        .containsExactly(DcbTagsIndexCheck.checkFailedMessage(EVENT_COLLECTION)),
                () -> assertThat(warningEvents()).singleElement()
                        .extracting(event -> event.getThrowableProxy().getMessage())
                        .as("the log keeps the cause, and the cause is the listIndexes command, which is the one the role may not run")
                        .asString()
                        .contains("listIndexes", "Unauthorized")
        );
    }

    @Test
    void the_spring_blocking_store_says_nothing_when_the_role_cannot_list_indexes_and_the_collection_holds_only_dcb_events() {
        givenARestrictedClientOnACollectionHoldingOnlyDcbEvents();

        logAppender.list.clear();
        newDcbStore(restrictedClient, DcbTagsIndexRestrictedMongo.DATABASE);

        assertThat(warnings())
                .as("with no stream event the store never sends listIndexes, so a role without that privilege costs neither a warning nor a failed check")
                .isEmpty();
    }

    private MongoCollection<Document> events() {
        return mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
    }

    private static void insertPositionedStreamEvent(MongoCollection<Document> events) {
        events.insertOne(new Document("id", UUID.randomUUID().toString())
                .append("source", SOURCE.toString())
                .append("type", "Defined")
                .append("streamid", "stream:1")
                .append("streamversion", 1L)
                .append("position", 1L));
    }

    // Appending through a store needs a transaction, which a standalone mongod does not have.
    private static void insertDcbEvent(MongoCollection<Document> events) {
        events.insertOne(new Document("id", UUID.randomUUID().toString())
                .append("source", SOURCE.toString())
                .append("type", "Defined")
                .append("streamid", "dcb:1")
                .append("streamversion", 1L)
                .append(DCB_TAGS, List.of("name:1"))
                .append("position", 1L));
    }

    private void createTwoUnusableDcbTagsIndexes() {
        MongoCollection<Document> events = events();
        events.createIndex(Indexes.ascending(DCB_TAGS), new IndexOptions().name("legacy_tags"));
        events.createIndex(Indexes.ascending(DCB_TAGS), new IndexOptions().name("dcbTags_1").partialFilterExpression(Filters.eq("type", "x")));
    }

    private void createHiddenAndNonSparseDcbTagsIndexes() {
        MongoCollection<Document> events = events();
        events.createIndex(Indexes.ascending(DCB_TAGS), new IndexOptions().name("hidden_tags").sparse(true));
        mongoClient.getDatabase(databaseName).runCommand(Document.parse("{ collMod: \"" + EVENT_COLLECTION + "\", index: { name: \"hidden_tags\", hidden: true } }"));
        events.createIndex(Indexes.ascending(DCB_TAGS), new IndexOptions().name("legacy_tags"));
    }

    private List<Document> indexesOnDcbTagsAlone() {
        return events().listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> index.get("key", Document.class).equals(new Document(DCB_TAGS, 1)))
                .toList();
    }

    private static List<String> printedCommands(String warning) {
        Matcher matcher = PRINTED_COMMANDS.matcher(warning);
        assertThat(matcher.find()).as("the warning ends with the commands to run, but was %s", warning).isTrue();
        return List.of(matcher.group(1).split(" and then "));
    }

    // Runs a command the way the shell would, one driver call per printed form. A form this does not know fails loudly.
    private void runPrintedCommand(String command) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        Matcher runCommand = RUN_COMMAND.matcher(command);
        Matcher dropIndex = DROP_INDEX.matcher(command);
        Matcher createIndex = CREATE_INDEX.matcher(command);
        if (runCommand.matches()) {
            database.runCommand(Document.parse(runCommand.group(1)));
        } else if (dropIndex.matches()) {
            database.getCollection(dropIndex.group(1)).dropIndex(dropIndex.group(2));
        } else if (createIndex.matches()) {
            Document options = Document.parse(createIndex.group(3));
            database.getCollection(createIndex.group(1)).createIndex(Document.parse(createIndex.group(2)), new IndexOptions().sparse(options.getBoolean("sparse")));
        } else {
            throw new AssertionError("The warning printed a command this test cannot run: " + command);
        }
    }

    private void givenARestrictedClientOnACollectionHoldingAPositionedStreamEvent() {
        try (MongoClient root = DcbTagsIndexRestrictedMongo.rootClient(restrictedMongoContainer)) {
            MongoCollection<Document> events = rootCollectionWithTheStoreIndexes(root);
            insertPositionedStreamEvent(events);
            restrictedClient = DcbTagsIndexRestrictedMongo.restrictedClient(restrictedMongoContainer, root, EVENT_COLLECTION);
        }
    }

    private void givenARestrictedClientOnACollectionHoldingOnlyDcbEvents() {
        try (MongoClient root = DcbTagsIndexRestrictedMongo.rootClient(restrictedMongoContainer)) {
            insertDcbEvent(rootCollectionWithTheStoreIndexes(root));
            restrictedClient = DcbTagsIndexRestrictedMongo.restrictedClient(restrictedMongoContainer, root, EVENT_COLLECTION);
        }
    }

    // A store built as root creates the collections and indexes, so the restricted user only has to find and create what already exists.
    private MongoCollection<Document> rootCollectionWithTheStoreIndexes(MongoClient root) {
        MongoCollection<Document> events = root.getDatabase(DcbTagsIndexRestrictedMongo.DATABASE).getCollection(EVENT_COLLECTION);
        events.drop();
        newDcbStore(root, DcbTagsIndexRestrictedMongo.DATABASE);
        return events;
    }

    private Document createDcbTagsIndex(IndexOptions options) {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        events.createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), options);
        return events.listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> index.get("key", Document.class).equals(new Document(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, 1)))
                .findFirst()
                .orElseThrow();
    }

    private boolean serverIsAtLeast(int major, int minor) {
        List<?> version = mongoClient.getDatabase("admin").runCommand(new Document("buildInfo", 1)).getList("versionArray", Object.class);
        int serverMajor = ((Number) version.get(0)).intValue();
        int serverMinor = ((Number) version.get(1)).intValue();
        return serverMajor > major || (serverMajor == major && serverMinor >= minor);
    }

    private List<String> warnings() {
        return warningEvents().stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<ILoggingEvent> warningEvents() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> event.getLoggerName().equals(SpringMongoEventStore.class.getName()))
                .toList();
    }

    private SpringMongoEventStore newStreamStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        return new SpringMongoEventStore(mongoTemplate, config);
    }

    private SpringMongoEventStore newDcbStore() {
        return newDcbStore(mongoClient, databaseName);
    }

    private static SpringMongoEventStore newDcbStore(MongoClient client, String database) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(new CommitRetryingMongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(DCB)
                .build();
        return new SpringMongoEventStore(new MongoTemplate(client, database), config);
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
