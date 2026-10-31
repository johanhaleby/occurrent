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
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.SynchronousEventDispatcher;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.nativedriver.ClientSessionHolder;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoTransactionExecutorTest {

    private static final URI SOURCE = URI.create("urn:occurrent:native-transaction-executor-test");
    private static final String SIDE_EFFECT_COLLECTION = "handler_side_effects";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

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

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_stream_write_and_the_command_runs_again_when_the_executor_owns_the_transaction() {
        String streamId = UUID.randomUUID().toString();
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("side-effect", null, StartAt.now(), recordSideEffectUsingAmbientSessionAndFailOnce(conflict));
        ApplicationService<NameDefined> applicationService = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(subscriptions)
                .build();
        AtomicInteger decisions = new AtomicInteger();

        applicationService.execute(streamId, __ -> {
            decisions.incrementAndGet();
            return List.of(new NameDefined(UUID.randomUUID().toString(), "owned-stream"));
        });

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffectCollection.countDocuments(new Document("name", "owned-stream"))).as("[side effects]").isEqualTo(1);
        assertThat(eventStore.read(streamId).eventList()).as("[events in the stream]").hasSize(1);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_rolls_back_the_dcb_append_and_the_command_runs_again_when_the_executor_owns_the_transaction() {
        Tag tag = Tag.parse("name:owned-dcb");
        DcbCriteria criteria = DcbCriteria.tags(tag);
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(criteria), 1);
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("side-effect", null, StartAt.now(), recordSideEffectUsingAmbientSessionAndFailOnce(conflict));
        DcbApplicationService<NameDefined> applicationService = dcbApplicationService(subscriptions);
        AtomicInteger decisions = new AtomicInteger();

        applicationService.execute(criteria, DcbExecuteOptions.empty(), __ -> {
            decisions.incrementAndGet();
            return List.of(new NameDefined(UUID.randomUUID().toString(), "owned-dcb"));
        });

        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(2);
        assertThat(sideEffectCollection.countDocuments(new Document("name", "owned-dcb"))).as("[side effects]").isEqualTo(1);
        assertThat(eventStore.read(DcbCriteria.tags(Tag.parse("name:owned-dcb"))).events()).as("[events in the boundary]").hasSize(1);
    }

    // ------------------------------------------------------------------------------------------------------------
    // A failure after the write, where this executor only joins the transaction
    // ------------------------------------------------------------------------------------------------------------

    @Test
    void isTransactional_is_false_outside_inTransaction_and_true_inside_it() {
        boolean before = transactionExecutor.isTransactional();
        boolean inside = transactionExecutor.inTransaction(transactionExecutor::isTransactional);
        boolean after = transactionExecutor.isTransactional();

        assertThat(before).as("[before inTransaction]").isFalse();
        assertThat(inside).as("[inside inTransaction]").isTrue();
        assertThat(after).as("[after inTransaction]").isFalse();
    }

    @Test
    void isTransactional_is_false_for_a_session_bound_to_the_thread_with_no_transaction_started() {
        boolean answer;
        try (ClientSession session = mongoClient.startSession()) {
            ClientSessionHolder.set(session);
            try {
                answer = transactionExecutor.isTransactional();
            } finally {
                ClientSessionHolder.remove();
            }
        }

        assertThat(answer).as("[with a session bound and no transaction started]").isFalse();
    }

    @Test
    void isTransactional_is_true_for_a_session_bound_to_the_thread_once_its_transaction_has_started_and_false_again_after_it_committed() {
        boolean started;
        boolean committed;
        try (ClientSession session = mongoClient.startSession()) {
            ClientSessionHolder.set(session);
            try {
                session.startTransaction();
                started = transactionExecutor.isTransactional();
                session.commitTransaction();
                committed = transactionExecutor.isTransactional();
            } finally {
                ClientSessionHolder.remove();
            }
        }

        assertThat(started).as("[after the transaction started]").isTrue();
        assertThat(committed).as("[after the transaction committed]").isFalse();
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_write_reaches_the_caller_without_running_the_command_again_and_every_handler_sees_the_event_in_a_session_bound_without_a_transaction() {
        String streamId = UUID.randomUUID().toString();
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        AtomicInteger eventsSeenBySecondHandler = new AtomicInteger();
        SynchronousSubscriptionModel subscriptions = new SynchronousSubscriptionModel();
        subscriptions.subscribe("failing", null, StartAt.now(), cloudEvent -> {
            throw conflict;
        });
        subscriptions.subscribe("observing", null, StartAt.now(), cloudEvent -> eventsSeenBySecondHandler.incrementAndGet());
        ApplicationService<NameDefined> applicationService = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(subscriptions)
                .build();
        AtomicInteger decisions = new AtomicInteger();

        Throwable thrown = failureInSessionBoundWithoutTransaction(() -> applicationService.execute(streamId, __ -> {
            decisions.incrementAndGet();
            return List.of(new NameDefined(UUID.randomUUID().toString(), "session-without-transaction"));
        }));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventStore.read(streamId).eventList()).as("[events in the stream]").hasSize(1);
        assertThat(eventsSeenBySecondHandler.get()).as("[events seen by the handler after the failing one]").isEqualTo(1);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_write_reaches_the_caller_without_running_the_command_again_in_a_session_the_caller_owns_on_a_stream() {
        String streamId = UUID.randomUUID().toString();
        WriteConditionNotFulfilledException conflict = new WriteConditionNotFulfilledException(streamId, 1, WriteCondition.anyStreamVersion());
        ApplicationService<NameDefined> applicationService = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .build();
        AtomicInteger decisions = new AtomicInteger();

        Throwable thrown = failureInTransactionOwnedByTheCaller(() -> applicationService.execute(streamId, __ -> {
            decisions.incrementAndGet();
            return List.of(new NameDefined(UUID.randomUUID().toString(), "caller-owned"));
        }));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventStore.read(streamId).eventList()).as("[events in the stream once the caller committed]").hasSize(1);
    }

    @Test
    void a_retried_conflict_thrown_by_a_handler_after_the_append_reaches_the_caller_without_running_the_command_again_in_a_session_the_caller_owns_on_a_dcb_boundary() {
        Tag tag = Tag.parse("name:caller-owned");
        DcbCriteria criteria = DcbCriteria.tags(tag);
        DcbAppendConditionNotFulfilledException conflict = new DcbAppendConditionNotFulfilledException(DcbAppendCondition.failIfEventsMatch(criteria), 1);
        DcbApplicationService<NameDefined> applicationService = GenericDcbApplicationService.<NameDefined>builder(eventStore, converter)
                .tagGenerator(event -> Set.of(tag))
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(new FailingOnceDispatcher(conflict))
                .build();
        AtomicInteger decisions = new AtomicInteger();

        Throwable thrown = failureInTransactionOwnedByTheCaller(() -> applicationService.execute(criteria, __ -> {
            decisions.incrementAndGet();
            return List.of(new NameDefined(UUID.randomUUID().toString(), "caller-owned"));
        }));

        assertThat(thrown).as("[exception reaching the caller]").isSameAs(conflict);
        assertThat(decisions.get()).as("[domain function runs]").isEqualTo(1);
        assertThat(eventStore.read(criteria).events()).as("[events in the boundary once the caller committed]").hasSize(1);
    }

    @Test
    void a_failure_after_the_write_of_a_service_called_from_a_handler_is_retried_by_the_outer_service_not_in_the_joined_transaction() {
        String outerStreamId = "outer-" + UUID.randomUUID();
        String innerStreamId = "inner-" + UUID.randomUUID();
        AtomicInteger innerDecisions = new AtomicInteger();
        AtomicInteger outerDecisions = new AtomicInteger();
        ApplicationService<NameDefined> inner = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(new FailingOnceDispatcher(new WriteConditionNotFulfilledException(innerStreamId, 1, WriteCondition.anyStreamVersion())))
                .build();
        ApplicationService<NameDefined> outer = GenericApplicationService.<NameDefined>builder(eventStore, converter)
                .transactionExecutor(transactionExecutor)
                .synchronousSubscriptions(new RunningDispatcher(() -> inner.execute(innerStreamId, __ -> {
                    innerDecisions.incrementAndGet();
                    return List.of(new NameDefined(UUID.randomUUID().toString(), "inner"));
                })))
                .build();

        outer.execute(outerStreamId, __ -> {
            outerDecisions.incrementAndGet();
            return List.of(new NameDefined(UUID.randomUUID().toString(), "outer"));
        });

        assertThat(outerDecisions.get()).as("[outer domain function runs, one per transaction]").isEqualTo(2);
        assertThat(innerDecisions.get()).as("[inner domain function runs, one per transaction]").isEqualTo(2);
        assertThat(eventStore.read(outerStreamId).eventList()).as("[events in the outer stream]").hasSize(1);
        assertThat(eventStore.read(innerStreamId).eventList()).as("[events in the inner stream]").hasSize(1);
    }

    /**
     * Runs {@code command} in a transaction the test opened and bound with {@link ClientSessionHolder}, as a caller that
     * owns its transaction does, and commits it afterwards whatever the command did, so the events that stay are the ones
     * written.
     */
    private @Nullable Throwable failureInTransactionOwnedByTheCaller(Runnable command) {
        try (ClientSession session = mongoClient.startSession()) {
            ClientSessionHolder.set(session);
            try {
                session.startTransaction();
                Throwable thrown = catchThrowable(command::run);
                session.commitTransaction();
                return thrown;
            } finally {
                ClientSessionHolder.remove();
            }
        }
    }

    /**
     * Runs {@code command} with a session bound with {@link ClientSessionHolder} that has no transaction, as a caller does
     * that holds a session only to give reads a causal order. Writes through it commit as they are made.
     */
    private @Nullable Throwable failureInSessionBoundWithoutTransaction(Runnable command) {
        try (ClientSession session = mongoClient.startSession()) {
            ClientSessionHolder.set(session);
            try {
                return catchThrowable(command::run);
            } finally {
                ClientSessionHolder.remove();
            }
        }
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

    /**
     * As {@link #recordSideEffectUsingAmbientSession()}, then throws {@code failure} the first time only. The first
     * attempt's side effect has to be rolled back with its event for the second attempt to leave exactly one.
     */
    private java.util.function.Consumer<CloudEvent> recordSideEffectUsingAmbientSessionAndFailOnce(RuntimeException failure) {
        AtomicBoolean failed = new AtomicBoolean();
        return cloudEvent -> {
            recordSideEffectUsingAmbientSession().accept(cloudEvent);
            if (failed.compareAndSet(false, true)) {
                throw failure;
            }
        };
    }

    private static final class FailingOnceDispatcher implements SynchronousEventDispatcher {
        private final AtomicBoolean failed = new AtomicBoolean();
        private final RuntimeException failure;

        FailingOnceDispatcher(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            if (failed.compareAndSet(false, true)) {
                throw failure;
            }
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
    }

    private record RunningDispatcher(Runnable onDispatch) implements SynchronousEventDispatcher {
        @Override
        public void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
            onDispatch.run();
        }

        @Override
        public boolean hasSubscriptions() {
            return true;
        }
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
