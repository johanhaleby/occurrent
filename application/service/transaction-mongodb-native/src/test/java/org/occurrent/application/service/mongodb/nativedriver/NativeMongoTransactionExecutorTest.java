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

package org.occurrent.application.service.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.nativedriver.ClientSessionHolder;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoTransactionExecutorTest {

    private static final URI SOURCE = URI.create("urn:occurrent:native-transaction-executor-test");
    private static final String SIDE_EFFECT_COLLECTION = "handler_side_effects";

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();

    private MongoClient mongoClient;
    private MongoEventStore eventStore;
    private MongoCollection<Document> sideEffectCollection;
    private NativeMongoTransactionExecutor transactionExecutor;
    private final NameConverter converter = new NameConverter();

    @BeforeEach
    void create_event_store_and_executor() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".transaction-executor-test");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = Objects.requireNonNull(connectionString.getDatabase());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new MongoEventStore(mongoClient, databaseName, "events", config);
        // Pre-create the side-effect collection outside a transaction so a handler can insert into it while joined
        // to the executor's transaction without relying on implicit in-transaction collection creation.
        mongoClient.getDatabase(databaseName).createCollection(SIDE_EFFECT_COLLECTION);
        sideEffectCollection = mongoClient.getDatabase(databaseName).getCollection(SIDE_EFFECT_COLLECTION);
        transactionExecutor = new NativeMongoTransactionExecutor(mongoClient);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    // ------------------------------------------------------------------------------------------------------------
    // Stream write path
    // ------------------------------------------------------------------------------------------------------------

    @Test
    void stream_write_commits_the_event_and_runs_the_synchronous_handler_in_the_same_transaction() {
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("side-effect", null, StartAt.now(), recordSideEffectUsingAmbientSession());

        ApplicationService<NameDefined> applicationService = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(subscriptions)
                .build();

        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, __ -> List.of(new NameDefined(UUID.randomUUID().toString(), "stream-ok")));

        assertThat(eventStore.read(streamId).eventList()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(sideEffectCollection.countDocuments(new Document("name", "stream-ok"))).isEqualTo(1);
    }

    @Test
    void throwing_synchronous_handler_rolls_back_the_stream_write() {
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("side-effect", null, StartAt.now(), cloudEvent -> {
            recordSideEffectUsingAmbientSession().accept(cloudEvent);
            throw new IllegalStateException("handler boom");
        });

        ApplicationService<NameDefined> applicationService = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(subscriptions)
                .build();

        String streamId = UUID.randomUUID().toString();
        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new NameDefined(UUID.randomUUID().toString(), "stream-rollback"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("handler boom");

        assertThat(eventStore.read(streamId).isEmpty()).isTrue();
        assertThat(sideEffectCollection.countDocuments(new Document("name", "stream-rollback"))).isZero();
    }

    // ------------------------------------------------------------------------------------------------------------
    // In-handler read visibility (reads issued from a synchronous handler join the executor's transaction)
    // ------------------------------------------------------------------------------------------------------------

    @Test
    void synchronous_handler_sees_the_just_written_event_through_count_and_exists_within_the_transaction() {
        String streamId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        AtomicBoolean existsSeenByHandler = new AtomicBoolean(false);
        AtomicLong idCountSeenByHandler = new AtomicLong(-1);
        AtomicLong countAllSeenByHandler = new AtomicLong(-1);

        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("visibility", null, StartAt.now(), cloudEvent -> {
            // Runs on the writer thread inside the executor's transaction. The store's count/exists reads are routed
            // through the ambient ClientSession, so they must observe the write that has not committed yet.
            existsSeenByHandler.set(eventStore.exists(streamId));
            idCountSeenByHandler.set(eventStore.count(Filter.id(eventId)));
            countAllSeenByHandler.set(eventStore.count(Filter.all()));
        });

        ApplicationService<NameDefined> applicationService = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(subscriptions)
                .build();

        assertThat(eventStore.exists(streamId)).as("stream does not exist before the write").isFalse();
        assertThat(eventStore.count(Filter.id(eventId))).as("event does not exist before the write").isZero();

        applicationService.execute(streamId, __ -> List.of(new NameDefined(eventId, "visibility")));

        assertThat(existsSeenByHandler.get()).as("exists(streamId) inside the handler sees the uncommitted event").isTrue();
        assertThat(idCountSeenByHandler.get()).as("count(Filter.id) inside the handler sees the uncommitted event").isEqualTo(1);
        assertThat(countAllSeenByHandler.get()).as("count(Filter.all()) inside the handler sees at least the uncommitted event").isGreaterThanOrEqualTo(1);
    }

    // ------------------------------------------------------------------------------------------------------------
    // DCB append path
    // ------------------------------------------------------------------------------------------------------------

    @Test
    void dcb_append_commits_the_event_and_runs_the_synchronous_handler_in_the_same_transaction() {
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("side-effect", null, StartAt.now(), recordSideEffectUsingAmbientSession());

        DcbApplicationService<NameDefined> applicationService = dcbApplicationService(subscriptions);

        DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:dcb-ok"));
        applicationService.execute(criteria, DcbExecuteOptions.empty(), __ -> List.of(new NameDefined(UUID.randomUUID().toString(), "dcb-ok")));

        assertThat(eventStore.read(criteria).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(sideEffectCollection.countDocuments(new Document("name", "dcb-ok"))).isEqualTo(1);
    }

    @Test
    void throwing_synchronous_handler_rolls_back_the_dcb_append() {
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("side-effect", null, StartAt.now(), cloudEvent -> {
            recordSideEffectUsingAmbientSession().accept(cloudEvent);
            throw new IllegalStateException("handler boom");
        });

        DcbApplicationService<NameDefined> applicationService = dcbApplicationService(subscriptions);

        DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:dcb-rollback"));
        assertThatThrownBy(() -> applicationService.execute(criteria, DcbExecuteOptions.empty(), __ -> List.of(new NameDefined(UUID.randomUUID().toString(), "dcb-rollback"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("handler boom");

        assertThat(eventStore.read(criteria).events()).isEmpty();
        assertThat(sideEffectCollection.countDocuments(new Document("name", "dcb-rollback"))).isZero();
    }

    private DcbApplicationService<NameDefined> dcbApplicationService(SynchronousSubscriptionModel subscriptions) {
        TagGenerator<NameDefined> tagGenerator = event -> Set.of(Tag.parse("name:" + event.name()));
        return GenericDcbApplicationService.<NameDefined>builder(eventStore, converter)
                .tagGenerator(tagGenerator)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(subscriptions)
                .build();
    }

    /**
     * A handler that writes a marker document to a separate collection using the {@link ClientSession} bound by the
     * executor, so its write is part of the same transaction as the event write. If the surrounding transaction rolls
     * back, this document must vanish along with the event.
     */
    private java.util.function.Consumer<CloudEvent> recordSideEffectUsingAmbientSession() {
        return cloudEvent -> {
            ClientSession ambientSession = ClientSessionHolder.get();
            assertThat(ambientSession).as("handler must observe the executor's ambient ClientSession").isNotNull();
            NameDefined domainEvent = converter.toDomainEvent(cloudEvent);
            sideEffectCollection.insertOne(ambientSession, new Document("name", domainEvent.name()).append("eventId", domainEvent.eventId()));
        };
    }

    // ------------------------------------------------------------------------------------------------------------
    // Minimal domain + converter
    // ------------------------------------------------------------------------------------------------------------

    private record NameDefined(String eventId, String name) {
    }

    private static final class NameConverter implements CloudEventConverter<NameDefined> {
        @Override
        public CloudEvent toCloudEvent(NameDefined domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(domainEvent.eventId())
                    .withSource(SOURCE)
                    .withType("NameDefined")
                    .withTime(OffsetDateTime.now())
                    .withDataContentType("text/plain")
                    .withData(domainEvent.name().getBytes(UTF_8))
                    .build();
        }

        @Override
        public NameDefined toDomainEvent(CloudEvent cloudEvent) {
            String name = new String(Objects.requireNonNull(cloudEvent.getData()).toBytes(), UTF_8);
            return new NameDefined(cloudEvent.getId(), name);
        }

        @Override
        public String getCloudEventType(Class<? extends NameDefined> type) {
            return "NameDefined";
        }
    }
}
